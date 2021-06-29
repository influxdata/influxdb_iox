//! Basic test runner that runs queries in files and compares the output to the expected results

mod parse;
mod setup;

use arrow::record_batch::RecordBatch;
use query::frontend::sql::SqlQueryPlanner;
use snafu::{ResultExt, Snafu};
use std::{
    io::BufWriter,
    io::Write,
    path::{Path, PathBuf},
    sync::Arc,
};

use self::{parse::TestQueries, setup::TestSetup};
use crate::scenarios::{DbScenario, DbSetup};

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("Can not find case file '{:?}': {}", path, source))]
    NoCaseFile {
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
        "Contents of output '{:?}' does not match contents of expected '{:?}'",
        output_path,
        expected_path
    ))]
    OutputMismatch {
        output_path: PathBuf,
        expected_path: PathBuf,
    },

    #[snafu(display("Test Setup Error: {}", source))]
    SetupError { source: self::setup::Error },
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
        let test_setup = TestSetup::try_from_lines(contents.lines()).context(SetupError)?;
        let queries = TestQueries::from_lines(contents.lines());
        println!("Using test setup:\n{}", test_setup);

        // Make a place to store output files
        let output_file = std::fs::File::create(&output_path).context(CreatingOutputFile {
            output_path: output_path.clone(),
        })?;

        let mut output = vec![];
        output.push(format!("-- Test Setup: {}", test_setup.setup_name()));

        let db_setup = test_setup.get_setup().context(SetupError)?;
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

#[cfg(test)]
mod test {
    use super::*;

    // TODO; implement end to end test of the runner
    #[tokio::test]
    async fn runner_positive() {}
}
