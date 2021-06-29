//! Finds all .sql files in cases/ and creates corresponding entries in src/cases.rs
//! native Rust types.

use std::path::{Path, PathBuf};

type Error = Box<dyn std::error::Error>;
type Result<T, E = Error> = std::result::Result<T, E>;

fn main() -> Result<()> {
    // crate root
    let root = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    let cases = root.join("cases");

    let sql_files = find_sql_files(&cases);

    // Tell cargo to recompile if anything in the cases directory changes
    println!("cargo:rerun-if-changed={}", cases.display());

    // Now create the generated sql file
    let output_content = make_cases_rs(&sql_files).join("\n");
    let output_file = root.join("src").join("cases.rs");
    std::fs::write(&output_file, output_content)
        .map_err(|e| format!("Error writing to {:?}: {}", output_file, e))
        .unwrap();

    Ok(())
}

/// Returns a sorted list of all files named `.sql` in the specified root directory
fn find_sql_files(root: &Path) -> Vec<PathBuf> {
    let mut sqls: Vec<PathBuf> = root
        .read_dir()
        .map_err(|e| format!("can not read root: {:?}: {}", root, e))
        .unwrap()
        .map(|dir_ent| {
            let dir_ent = dir_ent
                .map_err(|e| format!("can not read directory entry: {}", e))
                .unwrap();
            dir_ent.path()
        })
        .filter(|p| p.extension() == Some(std::ffi::OsStr::new("sql")))
        .collect();

    sqls.sort();
    sqls
}

/// writes out what will be the rust test file that lists out .sqls
fn make_cases_rs(sqls: &[PathBuf]) -> Vec<String> {
    let mut output_lines: Vec<String> = vec![r#"
//! This file is auto generated by build.rs
//! Do not edit manually --> will result in sadness
use std::path::Path;
use crate::runner::Runner;"#
        .into()];

    for sql in sqls {
        let file_name = sql.file_name().expect("a name").to_string_lossy();

        let test_name = file_name.replace(".", "_");

        output_lines.push(format!(
            r#"
#[tokio::test]
// Tests from {:?},
async fn test_cases_{}() {{
    let input_path = Path::new("cases").join("{}");
    let mut runner = Runner::new();
    runner
        .run(input_path)
        .await
        .expect("test failed");
    runner
        .flush()
        .expect("flush worked");
}}"#,
            file_name, test_name, file_name
        ));
    }

    output_lines
}
