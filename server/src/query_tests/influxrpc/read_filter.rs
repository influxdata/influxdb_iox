//! Tests for the Influx gRPC queries
#[cfg(test)]
use super::util::run_series_set_plan;

use crate::query_tests::scenarios::*;
use arrow_deps::datafusion::logical_plan::{col, lit};
use async_trait::async_trait;
use query::{
    frontend::influxrpc::InfluxRpcPlanner,
    predicate::{Predicate, PredicateBuilder, EMPTY_PREDICATE},
};

#[derive(Debug)]
pub struct TwoMeasurementsMultiSeries {}
#[async_trait]
impl DbSetup for TwoMeasurementsMultiSeries {
    async fn make(&self) -> Vec<DbScenario> {
        let partition_key = "1970-01-01T00";

        let mut lp_lines = vec![
            "h2o,state=MA,city=Boston temp=70.4 100", // to row 2
            "h2o,state=MA,city=Boston temp=72.4 250", // to row 1
            "h2o,state=CA,city=LA temp=90.0 200",     // to row 0
            "h2o,state=CA,city=LA temp=90.0 350",     // to row 3
            "o2,state=MA,city=Boston temp=50.4,reading=50 100", // to row 5
            "o2,state=MA,city=Boston temp=53.4,reading=51 250", // to row 4
        ];

        // Swap around  data is not inserted in series order
        lp_lines.swap(0, 2);
        lp_lines.swap(4, 5);

        make_one_chunk_scenarios(partition_key, &lp_lines.join("\n")).await
    }
}

/// runs read_filter(predicate) and compares it to the expected
/// output
macro_rules! run_read_filter_test_case {
    ($DB_SETUP:expr, $PREDICATE:expr, $EXPECTED_RESULTS:expr) => {
        test_helpers::maybe_start_logging();
        let predicate = $PREDICATE;
        let expected_results = $EXPECTED_RESULTS;
        for scenario in $DB_SETUP.make().await {
            let DbScenario {
                scenario_name, db, ..
            } = scenario;
            println!("Running scenario '{}'", scenario_name);
            println!("Predicate: '{:#?}'", predicate);
            let planner = InfluxRpcPlanner::new();

            let plan = planner
                .read_filter(&db, predicate.clone())
                .expect("built plan successfully");

            let string_results = run_series_set_plan(db.executor(), plan).await;

            assert_eq!(
                expected_results, string_results,
                "Error in  scenario '{}'\n\nexpected:\n{:#?}\nactual:\n{:#?}",
                scenario_name, expected_results, string_results
            );
        }
    };
}

#[tokio::test]
async fn test_read_filter_no_data_no_pred() {
    let predicate = EMPTY_PREDICATE;
    let expected_results = vec![] as Vec<&str>;

    run_read_filter_test_case!(NoData {}, predicate, expected_results);
}

#[tokio::test]
async fn test_read_filter_data_no_pred() {
    let predicate = EMPTY_PREDICATE;
    let expected_results = vec![
        "SeriesSet",
        "table_name: h2o",
        "tags",
        "  (city, Boston)",
        "  (state, MA)",
        "field_indexes:",
        "  (value_index: 2, timestamp_index: 3)",
        "start_row: 0",
        "num_rows: 2",
        "Batches:",
        "+--------+-------+------+-------------------------------+",
        "| city   | state | temp | time                          |",
        "+--------+-------+------+-------------------------------+",
        "| Boston | MA    | 70.4 | 1970-01-01 00:00:00.000000100 |",
        "| Boston | MA    | 72.4 | 1970-01-01 00:00:00.000000250 |",
        "| LA     | CA    | 90   | 1970-01-01 00:00:00.000000200 |",
        "| LA     | CA    | 90   | 1970-01-01 00:00:00.000000350 |",
        "+--------+-------+------+-------------------------------+",
        "SeriesSet",
        "table_name: h2o",
        "tags",
        "  (city, LA)",
        "  (state, CA)",
        "field_indexes:",
        "  (value_index: 2, timestamp_index: 3)",
        "start_row: 2",
        "num_rows: 2",
        "Batches:",
        "+--------+-------+------+-------------------------------+",
        "| city   | state | temp | time                          |",
        "+--------+-------+------+-------------------------------+",
        "| Boston | MA    | 70.4 | 1970-01-01 00:00:00.000000100 |",
        "| Boston | MA    | 72.4 | 1970-01-01 00:00:00.000000250 |",
        "| LA     | CA    | 90   | 1970-01-01 00:00:00.000000200 |",
        "| LA     | CA    | 90   | 1970-01-01 00:00:00.000000350 |",
        "+--------+-------+------+-------------------------------+",
        "SeriesSet",
        "table_name: o2",
        "tags",
        "  (city, Boston)",
        "  (state, MA)",
        "field_indexes:",
        "  (value_index: 2, timestamp_index: 4)",
        "  (value_index: 3, timestamp_index: 4)",
        "start_row: 0",
        "num_rows: 2",
        "Batches:",
        "+--------+-------+---------+------+-------------------------------+",
        "| city   | state | reading | temp | time                          |",
        "+--------+-------+---------+------+-------------------------------+",
        "| Boston | MA    | 50      | 50.4 | 1970-01-01 00:00:00.000000100 |",
        "| Boston | MA    | 51      | 53.4 | 1970-01-01 00:00:00.000000250 |",
        "+--------+-------+---------+------+-------------------------------+",
    ];

    run_read_filter_test_case!(TwoMeasurementsMultiSeries {}, predicate, expected_results);
}

#[tokio::test]
async fn test_read_filter_data_filter() {
    // filter out one row in h20
    let predicate = PredicateBuilder::default()
        .timestamp_range(200, 300)
        .add_expr(col("state").eq(lit("CA"))) // state=CA
        .build();

    let expected_results = vec![
        "SeriesSet",
        "table_name: h2o",
        "tags",
        "  (city, LA)",
        "  (state, CA)",
        "field_indexes:",
        "  (value_index: 2, timestamp_index: 3)",
        "start_row: 0",
        "num_rows: 1",
        "Batches:",
        "+------+-------+------+-------------------------------+",
        "| city | state | temp | time                          |",
        "+------+-------+------+-------------------------------+",
        "| LA   | CA    | 90   | 1970-01-01 00:00:00.000000200 |",
        "+------+-------+------+-------------------------------+",
    ];

    run_read_filter_test_case!(TwoMeasurementsMultiSeries {}, predicate, expected_results);
}

#[tokio::test]
async fn test_read_filter_data_filter_fields() {
    // filter out one row in h20
    let predicate = PredicateBuilder::default()
        .field_columns(vec!["other_temp"])
        .add_expr(col("state").eq(lit("CA"))) // state=CA
        .build();

    // Only expect other_temp in this location
    let expected_results = vec![
        "SeriesSet",
        "table_name: h2o",
        "tags",
        "  (city, Boston)",
        "  (state, CA)",
        "field_indexes:",
        "  (value_index: 2, timestamp_index: 3)",
        "start_row: 0",
        "num_rows: 1",
        "Batches:",
        "+--------+-------+------------+-------------------------------+",
        "| city   | state | other_temp | time                          |",
        "+--------+-------+------------+-------------------------------+",
        "| Boston | CA    | 72.4       | 1970-01-01 00:00:00.000000350 |",
        "+--------+-------+------------+-------------------------------+",
        "SeriesSet",
        "table_name: o2",
        "tags",
        "  (state, CA)",
        "field_indexes:",
        "start_row: 0",
        "num_rows: 1",
        "Batches:",
        "+------+-------+-------------------------------+",
        "| city | state | time                          |",
        "+------+-------+-------------------------------+",
        "|      | CA    | 1970-01-01 00:00:00.000000300 |",
        "+------+-------+-------------------------------+",
    ];

    run_read_filter_test_case!(TwoMeasurementsManyFields {}, predicate, expected_results);
}

#[tokio::test]
async fn test_read_filter_data_pred_refers_to_non_existent_column() {
    let predicate = PredicateBuilder::default()
        .add_expr(col("tag_not_in_h20").eq(lit("foo")))
        .build();

    let expected_results = vec![] as Vec<&str>;

    run_read_filter_test_case!(TwoMeasurements {}, predicate, expected_results);
}

#[tokio::test]
async fn test_read_filter_data_pred_no_columns() {
    // predicate with no columns,
    let predicate = PredicateBuilder::default()
        .add_expr(lit("foo").eq(lit("foo")))
        .build();

    let expected_results = vec![
        "SeriesSet",
        "table_name: cpu",
        "tags",
        "  (region, west)",
        "field_indexes:",
        "  (value_index: 1, timestamp_index: 2)",
        "start_row: 0",
        "num_rows: 2",
        "Batches:",
        "+--------+------+-------------------------------+",
        "| region | user | time                          |",
        "+--------+------+-------------------------------+",
        "| west   | 23.2 | 1970-01-01 00:00:00.000000100 |",
        "| west   | 21   | 1970-01-01 00:00:00.000000150 |",
        "+--------+------+-------------------------------+",
        "SeriesSet",
        "table_name: disk",
        "tags",
        "  (region, east)",
        "field_indexes:",
        "  (value_index: 1, timestamp_index: 2)",
        "start_row: 0",
        "num_rows: 1",
        "Batches:",
        "+--------+-------+-------------------------------+",
        "| region | bytes | time                          |",
        "+--------+-------+-------------------------------+",
        "| east   | 99    | 1970-01-01 00:00:00.000000200 |",
        "+--------+-------+-------------------------------+",
    ];

    run_read_filter_test_case!(TwoMeasurements {}, predicate, expected_results);
}

#[tokio::test]
async fn test_read_filter_data_pred_refers_to_good_and_non_existent_columns() {
    // predicate with both a column that does and does not appear
    let predicate = PredicateBuilder::default()
        .add_expr(col("state").eq(lit("MA")))
        .add_expr(col("tag_not_in_h20").eq(lit("foo")))
        .build();

    let expected_results = vec![] as Vec<&str>;

    run_read_filter_test_case!(TwoMeasurements {}, predicate, expected_results);
}

#[tokio::test]
async fn test_read_filter_data_pred_unsupported_in_scan() {
    test_helpers::maybe_start_logging();

    // These predicates can't be pushed down into chunks, but they can
    // be evaluated by the general purpose DataFusion plan
    // https://github.com/influxdata/influxdb_iox/issues/883
    // (STATE = 'CA') OR (READING > 0)
    let predicate = PredicateBuilder::default()
        .add_expr(col("state").eq(lit("CA")).or(col("reading").gt(lit(0))))
        .build();

    // Note these results are incorrect (they do not include data from h2o where
    // state = CA)
    let expected_results = vec![
        "SeriesSet",
        "table_name: o2",
        "tags",
        "  (city, Boston)",
        "  (state, MA)",
        "field_indexes:",
        "  (value_index: 2, timestamp_index: 4)",
        "  (value_index: 3, timestamp_index: 4)",
        "start_row: 0",
        "num_rows: 2",
        "Batches:",
        "+--------+-------+---------+------+-------------------------------+",
        "| city   | state | reading | temp | time                          |",
        "+--------+-------+---------+------+-------------------------------+",
        "| Boston | MA    | 50      | 50.4 | 1970-01-01 00:00:00.000000100 |",
        "| Boston | MA    | 51      | 53.4 | 1970-01-01 00:00:00.000000250 |",
        "+--------+-------+---------+------+-------------------------------+",
    ];

    run_read_filter_test_case!(TwoMeasurementsMultiSeries {}, predicate, expected_results);
}

#[derive(Debug)]
pub struct MeasurementsSortableTags {}
#[async_trait]
impl DbSetup for MeasurementsSortableTags {
    async fn make(&self) -> Vec<DbScenario> {
        let partition_key = "1970-01-01T00";

        let lp_lines = vec![
            "h2o,zz_tag=A,state=MA,city=Kingston temp=70.1 800",
            "h2o,state=MA,city=Kingston,zz_tag=B temp=70.2 100",
            "h2o,state=CA,city=Boston temp=70.3 250",
            "h2o,state=MA,city=Boston,zz_tag=A temp=70.4 1000",
            "h2o,state=MA,city=Boston temp=70.5,other=5.0 250",
        ];

        make_one_chunk_scenarios(partition_key, &lp_lines.join("\n")).await
    }
}

#[tokio::test]
async fn test_read_filter_data_plan_order() {
    test_helpers::maybe_start_logging();
    let predicate = Predicate::default();
    let expected_results = vec![
        "SeriesSet",
        "table_name: h2o",
        "tags",
        "  (city, Boston)",
        "  (state, CA)",
        "field_indexes:",
        "  (value_index: 3, timestamp_index: 5)",
        "  (value_index: 4, timestamp_index: 5)",
        "start_row: 0",
        "num_rows: 1",
        "Batches:",
        "+----------+-------+--------+-------+------+-------------------------------+",
        "| city     | state | zz_tag | other | temp | time                          |",
        "+----------+-------+--------+-------+------+-------------------------------+",
        "| Boston   | CA    |        |       | 70.3 | 1970-01-01 00:00:00.000000250 |",
        "| Boston   | MA    |        | 5     | 70.5 | 1970-01-01 00:00:00.000000250 |",
        "| Boston   | MA    | A      |       | 70.4 | 1970-01-01 00:00:00.000001    |",
        "| Kingston | MA    | A      |       | 70.1 | 1970-01-01 00:00:00.000000800 |",
        "| Kingston | MA    | B      |       | 70.2 | 1970-01-01 00:00:00.000000100 |",
        "+----------+-------+--------+-------+------+-------------------------------+",
        "SeriesSet",
        "table_name: h2o",
        "tags",
        "  (city, Boston)",
        "  (state, MA)",
        "field_indexes:",
        "  (value_index: 3, timestamp_index: 5)",
        "  (value_index: 4, timestamp_index: 5)",
        "start_row: 1",
        "num_rows: 1",
        "Batches:",
        "+----------+-------+--------+-------+------+-------------------------------+",
        "| city     | state | zz_tag | other | temp | time                          |",
        "+----------+-------+--------+-------+------+-------------------------------+",
        "| Boston   | CA    |        |       | 70.3 | 1970-01-01 00:00:00.000000250 |",
        "| Boston   | MA    |        | 5     | 70.5 | 1970-01-01 00:00:00.000000250 |",
        "| Boston   | MA    | A      |       | 70.4 | 1970-01-01 00:00:00.000001    |",
        "| Kingston | MA    | A      |       | 70.1 | 1970-01-01 00:00:00.000000800 |",
        "| Kingston | MA    | B      |       | 70.2 | 1970-01-01 00:00:00.000000100 |",
        "+----------+-------+--------+-------+------+-------------------------------+",
        "SeriesSet",
        "table_name: h2o",
        "tags",
        "  (city, Boston)",
        "  (state, MA)",
        "  (zz_tag, A)",
        "field_indexes:",
        "  (value_index: 3, timestamp_index: 5)",
        "  (value_index: 4, timestamp_index: 5)",
        "start_row: 2",
        "num_rows: 1",
        "Batches:",
        "+----------+-------+--------+-------+------+-------------------------------+",
        "| city     | state | zz_tag | other | temp | time                          |",
        "+----------+-------+--------+-------+------+-------------------------------+",
        "| Boston   | CA    |        |       | 70.3 | 1970-01-01 00:00:00.000000250 |",
        "| Boston   | MA    |        | 5     | 70.5 | 1970-01-01 00:00:00.000000250 |",
        "| Boston   | MA    | A      |       | 70.4 | 1970-01-01 00:00:00.000001    |",
        "| Kingston | MA    | A      |       | 70.1 | 1970-01-01 00:00:00.000000800 |",
        "| Kingston | MA    | B      |       | 70.2 | 1970-01-01 00:00:00.000000100 |",
        "+----------+-------+--------+-------+------+-------------------------------+",
        "SeriesSet",
        "table_name: h2o",
        "tags",
        "  (city, Kingston)",
        "  (state, MA)",
        "  (zz_tag, A)",
        "field_indexes:",
        "  (value_index: 3, timestamp_index: 5)",
        "  (value_index: 4, timestamp_index: 5)",
        "start_row: 3",
        "num_rows: 1",
        "Batches:",
        "+----------+-------+--------+-------+------+-------------------------------+",
        "| city     | state | zz_tag | other | temp | time                          |",
        "+----------+-------+--------+-------+------+-------------------------------+",
        "| Boston   | CA    |        |       | 70.3 | 1970-01-01 00:00:00.000000250 |",
        "| Boston   | MA    |        | 5     | 70.5 | 1970-01-01 00:00:00.000000250 |",
        "| Boston   | MA    | A      |       | 70.4 | 1970-01-01 00:00:00.000001    |",
        "| Kingston | MA    | A      |       | 70.1 | 1970-01-01 00:00:00.000000800 |",
        "| Kingston | MA    | B      |       | 70.2 | 1970-01-01 00:00:00.000000100 |",
        "+----------+-------+--------+-------+------+-------------------------------+",
        "SeriesSet",
        "table_name: h2o",
        "tags",
        "  (city, Kingston)",
        "  (state, MA)",
        "  (zz_tag, B)",
        "field_indexes:",
        "  (value_index: 3, timestamp_index: 5)",
        "  (value_index: 4, timestamp_index: 5)",
        "start_row: 4",
        "num_rows: 1",
        "Batches:",
        "+----------+-------+--------+-------+------+-------------------------------+",
        "| city     | state | zz_tag | other | temp | time                          |",
        "+----------+-------+--------+-------+------+-------------------------------+",
        "| Boston   | CA    |        |       | 70.3 | 1970-01-01 00:00:00.000000250 |",
        "| Boston   | MA    |        | 5     | 70.5 | 1970-01-01 00:00:00.000000250 |",
        "| Boston   | MA    | A      |       | 70.4 | 1970-01-01 00:00:00.000001    |",
        "| Kingston | MA    | A      |       | 70.1 | 1970-01-01 00:00:00.000000800 |",
        "| Kingston | MA    | B      |       | 70.2 | 1970-01-01 00:00:00.000000100 |",
        "+----------+-------+--------+-------+------+-------------------------------+",
    ];

    run_read_filter_test_case!(MeasurementsSortableTags {}, predicate, expected_results);
}
