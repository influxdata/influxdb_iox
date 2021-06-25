//! Basic test runner that runs queries in files and compares the output to the expected results

use arrow::record_batch::RecordBatch;
use query::frontend::sql::SqlQueryPlanner;
use snafu::{OptionExt, ResultExt, Snafu};
use std::{
    fmt::Display,
    io::BufWriter,
    io::Write,
    path::{Path, PathBuf},
    sync::Arc,
};

use crate::scenarios::{get_all_setups, get_db_setup, DbScenario, DbSetup};

const IOX_SETUP_NEEDLE: &str = "-- IOX_SETUP: ";

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("Can not find case file '{:?}': {}", path, source))]
    NoCaseFile {
        path: PathBuf,
        source: std::io::Error,
    },

    #[snafu(display("Can not find expected output file '{:?}': {}", path, source))]
    NoExpectedFile {
        path: PathBuf,
        source: std::io::Error,
    },

    #[snafu(display("case file '{:?}' is not UTF8 {}", path, source))]
    CaseFileNotUtf8 {
        path: PathBuf,
        source: std::string::FromUtf8Error,
    },

    #[snafu(display("expected file '{:?}' is not UTF8 {}", path, source))]
    ExpectedFileNotUtf8 {
        path: PathBuf,
        source: std::string::FromUtf8Error,
    },

    #[snafu(display("can not open output file '{:?}': {}", output_path, source))]
    CreatingOutputFile {
        output_path: PathBuf,
        source: std::io::Error,
    },

    #[snafu(display("can not write to output file '{:?}': {}", output_path, source))]
    WritingToOutputFile {
        output_path: PathBuf,
        source: std::io::Error,
    },

    #[snafu(display(
        "No setup found. Looking for lines that start with '{}'",
        IOX_SETUP_NEEDLE
    ))]
    SetupNotFound {},

    #[snafu(display(
        "Contents of output '{:?}' does not match contents of expected '{:?}'",
        output_path,
        expected_path
    ))]
    OutputMismatch {
        output_path: PathBuf,
        expected_path: PathBuf,
    },

    #[snafu(display("No setup named '{}' found. Perhaps you need to define it in scenarios.rs. Known setups: {:?}",
                    setup_name, known_setups))]
    NamedSetupNotFound {
        setup_name: String,
        known_setups: Vec<String>,
    },

    #[snafu(display(
        "Only one setup is supported. Previously say setup '{}' and now saw '{}'",
        setup_name,
        new_setup_name
    ))]
    SecondSetupFound {
        setup_name: String,
        new_setup_name: String,
    },
}
pub type Result<T, E = Error> = std::result::Result<T, E>;

pub struct Runner {}

// The case runner
impl Runner {
    pub fn new() -> Self {
        Self {}
    }

    /// Run the test case of the specified file
    ///
    /// Expect to find the file at `cases/path`
    ///
    /// Returns Ok on success, or Err() on failure
    pub async fn run(&self, input_filename: &str) -> Result<()> {
        let input_path = Path::new("cases").join(input_filename);
        // create output and expected output
        let output_path = input_path.with_extension("out");
        let expected_path = input_path.with_extension("expected");

        println!("Running case in {:?}", input_path);
        println!("  writing output to {:?}", output_path);
        println!("  expected output in {:?}", expected_path);

        let contents = std::fs::read(&input_path).context(NoCaseFile { path: &input_path })?;
        let contents =
            String::from_utf8(contents).context(CaseFileNotUtf8 { path: &input_path })?;

        println!("Processing contents:\n{}", contents);
        let test_setup = TestSetup::try_from_lines(contents.lines())?;
        let queries = TestQueries::from_lines(contents.lines());
        println!("Using test setup:\n{}", test_setup);

        // Make a place to store output files
        let output_file = std::fs::File::create(&output_path).context(CreatingOutputFile {
            output_path: output_path.clone(),
        })?;

        let mut output = vec![];
        output.push(format!("-- Test Setup: {}", test_setup.setup_name()));

        let db_setup = test_setup.get_setup()?;
        for q in queries.iter() {
            output.push(format!("-- SQL: {}", q));

            output.append(&mut self.run_query(q, db_setup.as_ref()).await);
        }

        let mut output_file = BufWriter::new(output_file);
        for o in &output {
            writeln!(&mut output_file, "{}", o).with_context(|| WritingToOutputFile {
                output_path: output_path.clone(),
            })?;
        }
        output_file.flush().with_context(|| WritingToOutputFile {
            output_path: output_path.clone(),
        })?;

        std::mem::drop(output_file);

        // Now, compare to expected results
        let expected_data = std::fs::read(&expected_path)
            .ok() // no output is fine
            .unwrap_or_else(Vec::new);

        let expected_contents: Vec<_> = String::from_utf8(expected_data)
            .context(ExpectedFileNotUtf8 {
                path: &expected_path,
            })?
            .lines()
            .map(|s| s.to_string())
            .collect();

        if expected_contents != output {
            let expected_path = make_absolute(&expected_path);
            let output_path = make_absolute(&output_path);

            println!("Expected output does not match actual output");
            println!("  expected output in {:?}", expected_path);
            println!("  actual output in {:?}", output_path);
            println!("Possibly helpful commands:");
            println!("  # See diff");
            println!("  diff -du {:?} {:?}", expected_path, output_path);
            println!("  # Update expected");
            println!("  cp -f {:?} {:?}", output_path, expected_path);
            OutputMismatch {
                output_path,
                expected_path,
            }
            .fail()
        } else {
            Ok(())
        }
    }

    /// runs the specified query against each scenario in `db_setup`
    /// and compares them for equality with each other. If they all
    /// produce the same answer, that answer is returned as pretty
    /// printed strings.
    ///
    /// If there is a mismatch the runner panics
    ///
    /// Note this does not (yet) understand how to compare results
    /// while ignoring output order
    async fn run_query(&self, sql: &str, db_setup: &dyn DbSetup) -> Vec<String> {
        let mut previous_results = vec![];

        for scenario in db_setup.make().await {
            let DbScenario {
                scenario_name, db, ..
            } = scenario;
            let db = Arc::new(db);

            println!("Running scenario '{}'", scenario_name);
            println!("SQL: '{:#?}'", sql);
            let planner = SqlQueryPlanner::default();
            let executor = db.executor();

            let physical_plan = planner
                .query(db, &sql, executor.as_ref())
                .expect("built plan successfully");

            let results: Vec<RecordBatch> =
                executor.collect(physical_plan).await.expect("Running plan");

            let current_results = arrow::util::pretty::pretty_format_batches(&results)
                .unwrap()
                .trim()
                .lines()
                .map(|s| s.to_string())
                .collect::<Vec<_>>();

            if !previous_results.is_empty() {
                assert_eq!(
                    previous_results, current_results,
                    "Answers produced by scenario {} differ from previous answer\
                     \n\nprevious:\n\n{:#?}\ncurrent:\n\n{:#?}\n\n",
                    scenario_name, previous_results, current_results
                );
            }
            previous_results = current_results;
        }

        previous_results
    }
}

/// Return the absolute path to `path`, regardless of if it exists or
/// not on the local filesystem
fn make_absolute(path: &Path) -> PathBuf {
    let mut absolute = std::env::current_dir().expect("can not get current working directory");
    absolute.extend(path);
    absolute
}

/// Encapsulates the setup needed for a test
///
/// Currently supports the following commands
///
/// # Run the specified setup:
/// # -- IOX_SETUP: SetupName
#[derive(Debug, PartialEq)]
struct TestSetup {
    setup_name: String,
}

impl Display for TestSetup {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        writeln!(f, "TestSetup")?;
        writeln!(f, "  Setup: {}", self.setup_name)?;

        Ok(())
    }
}

impl TestSetup {
    fn setup_name(&self) -> &str {
        &self.setup_name
    }

    fn get_setup(&self) -> Result<Arc<dyn DbSetup>> {
        let setup_name = self.setup_name();
        get_db_setup(setup_name).with_context(|| {
            let known_setups = get_all_setups()
                .keys()
                .map(|s| s.to_string())
                .collect::<Vec<_>>();
            NamedSetupNotFound {
                setup_name,
                known_setups,
            }
        })
    }

    /// Create a new TestSetup object from the lines
    fn try_from_lines<I, S>(lines: I) -> Result<Self>
    where
        I: IntoIterator<Item = S>,
        S: AsRef<str>,
    {
        let mut parser = lines.into_iter().filter_map(|line| {
            let line = line.as_ref().trim();
            line.strip_prefix(IOX_SETUP_NEEDLE)
                .map(|setup_name| setup_name.trim().to_string())
        });

        let setup_name = parser.next().context(SetupNotFound)?;

        if let Some(new_setup_name) = parser.next() {
            return SecondSetupFound {
                setup_name,
                new_setup_name,
            }
            .fail();
        }

        Ok(Self { setup_name })
    }
}

// Poor man's parser to find all the SQL queries in the input
#[derive(Debug, PartialEq)]
struct TestQueries {
    queries: Vec<String>,
}

impl TestQueries {
    fn new<I, S>(queries: I) -> Self
    where
        I: IntoIterator<Item = S>,
        S: AsRef<str>,
    {
        let queries = queries
            .into_iter()
            .map(|s| s.as_ref().to_string())
            .collect();

        Self { queries }
    }

    /// find all queries (more or less a fancy split on `;`
    fn from_lines<I, S>(lines: I) -> Self
    where
        I: IntoIterator<Item = S>,
        S: AsRef<str>,
    {
        let mut queries = vec![];
        let mut current_line = String::new();

        lines.into_iter().for_each(|line| {
            let line = line.as_ref().trim();
            if line.starts_with("--") {
                return;
            }
            if line.is_empty() {
                return;
            }

            // declare queries when we see a semicolon at the end of the line
            if !current_line.is_empty() {
                current_line.push(' ');
            }
            current_line.push_str(line);

            if line.ends_with(';') {
                // resets current_line to String::new()
                let t = std::mem::take(&mut current_line);
                queries.push(t);
            }
        });

        if !current_line.is_empty() {
            queries.push(current_line);
        }

        Self { queries }
    }

    // Get an iterator over the queries
    fn iter(&self) -> impl Iterator<Item = &str> {
        self.queries.iter().map(|s| s.as_str())
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn test_parse_lines() {
        let lines = vec!["Foo", "bar", "-- IOX_SETUP: MySetup", "goo"];
        let setup = TestSetup::try_from_lines(lines).unwrap();
        assert_eq!(
            setup,
            TestSetup {
                setup_name: "MySetup".into()
            }
        );
    }

    #[test]
    fn test_parse_lines_extra_whitespace() {
        let lines = vec!["Foo", "  -- IOX_SETUP:   MySetup   "];
        let setup = TestSetup::try_from_lines(lines).unwrap();
        assert_eq!(
            setup,
            TestSetup {
                setup_name: "MySetup".into()
            }
        );
    }

    #[test]
    fn test_parse_lines_setup_name_with_whitespace() {
        let lines = vec!["Foo", "  -- IOX_SETUP:   My Awesome  Setup   "];
        let setup = TestSetup::try_from_lines(lines).unwrap();
        assert_eq!(
            setup,
            TestSetup {
                setup_name: "My Awesome  Setup".into()
            }
        );
    }

    #[test]
    fn test_parse_lines_none() {
        let lines = vec!["Foo", " MySetup   "];
        let setup = TestSetup::try_from_lines(lines).unwrap_err().to_string();
        assert_eq!(
            setup,
            "No setup found. Looking for lines that start with '-- IOX_SETUP: '"
        );
    }

    #[test]
    fn test_parse_lines_multi() {
        let lines = vec!["Foo", "-- IOX_SETUP:   MySetup", "-- IOX_SETUP:   MySetup2"];
        let setup = TestSetup::try_from_lines(lines).unwrap_err().to_string();
        assert_eq!(
            setup,
            "Only one setup is supported. Previously say setup 'MySetup' and now saw 'MySetup2'"
        );
    }

    #[test]
    fn test_parse_queries() {
        let input = r#"
-- This is a test
select * from foo;
-- another comment

select * from bar;
-- This query has been commented out and should not be seen
-- select * from baz;
"#;
        let queries = TestQueries::from_lines(input.split('\n'));
        assert_eq!(
            queries,
            TestQueries::new(vec!["select * from foo;", "select * from bar;"])
        )
    }

    #[test]
    fn test_parse_queries_no_ending_semi() {
        let input = r#"
select * from foo;
-- no ending semi colon
select * from bar
"#;
        let queries = TestQueries::from_lines(input.split('\n'));
        assert_eq!(
            queries,
            TestQueries::new(vec!["select * from foo;", "select * from bar"])
        )
    }

    #[test]
    fn test_parse_queries_empty() {
        let input = r#"
-- This is a test
-- another comment
"#;
        let queries = TestQueries::from_lines(input.split('\n'));
        assert_eq!(queries, TestQueries::new(vec![] as Vec<String>))
    }
}
