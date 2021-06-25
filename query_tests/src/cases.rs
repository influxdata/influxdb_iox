//! This file is auto generated by build.rs
//! Do not edit manually --> will result in sadness

use crate::runner::Runner;

#[tokio::test]
// Tests from "duplicates.sql",
async fn test_cases_duplicates_sql() {
    Runner::new()
        .run("duplicates.sql")
        .await
        .expect("test failed")
}

#[tokio::test]
// Tests from "pushdown.sql",
async fn test_cases_pushdown_sql() {
    Runner::new()
        .run("pushdown.sql")
        .await
        .expect("test failed")
}