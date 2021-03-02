//! Tests for the Influx gRPC queries
use crate::query_tests::scenarios::*;

use arrow_deps::{arrow::util::pretty::pretty_format_batches, datafusion::prelude::*};
use async_trait::async_trait;
use query::{
    exec::Executor,
    frontend::influxrpc::InfluxRPCPlanner,
    group_by::Aggregate,
    predicate::{Predicate, PredicateBuilder},
};

/// runs read_group(predicate) and compares it to the expected
/// output
macro_rules! run_read_group_test_case {
    ($DB_SETUP:expr, $PREDICATE:expr, $AGG:expr, $GROUP_COLUMNS:expr, $EXPECTED_RESULTS:expr) => {
        test_helpers::maybe_start_logging();
        let predicate = $PREDICATE;
        let agg = $AGG;
        let group_columns = $GROUP_COLUMNS;
        let expected_results = $EXPECTED_RESULTS;
        for scenario in $DB_SETUP.make().await {
            let DBScenario {
                scenario_name, db, ..
            } = scenario;
            println!("Running scenario '{}'", scenario_name);
            println!("Predicate: '{:#?}'", predicate);
            let planner = InfluxRPCPlanner::new();
            let executor = Executor::new();

            let plans = planner
                .read_group(&db, predicate.clone(), agg, &group_columns)
                .await
                .expect("built plan successfully");

            let plans = plans.into_inner();

            for (i, plan) in plans.iter().enumerate() {
                assert_eq!(
                    plan.num_prefix_tag_group_columns,
                    Some(group_columns.len()),
                    "Mismatch in plan index {}",
                    i
                );
            }

            let mut string_results = vec![];
            for plan in plans.into_iter() {
                let batches = executor
                    .run_logical_plan(plan.plan)
                    .await
                    .expect("ok running plan");

                string_results.extend(
                    pretty_format_batches(&batches)
                        .expect("formatting results")
                        .trim()
                        .split('\n')
                        .map(|s| s.to_string()),
                );
            }

            assert_eq!(
                expected_results, string_results,
                "Error in  scenario '{}'\n\nexpected:\n{:#?}\nactual:\n{:#?}",
                scenario_name, expected_results, string_results
            );
        }
    };
}

#[tokio::test]
async fn test_read_group_no_data_no_pred() {
    let predicate = Predicate::default();
    let agg = Aggregate::Mean;
    let group_columns = vec![] as Vec<String>;
    let expected_results = vec![] as Vec<&str>;

    run_read_group_test_case!(NoData {}, predicate, agg, group_columns, expected_results);
}

struct OneMeasurementForAggs {}
#[async_trait]
impl DBSetup for OneMeasurementForAggs {
    async fn make(&self) -> Vec<DBScenario> {
        let partition_key = "1970-01-01T00";

        let lp_lines1 = vec![
            "h2o,state=MA,city=Boston temp=70.4 100",
            "h2o,state=MA,city=Boston temp=72.4 250",
        ];
        let lp_lines2 = vec![
            "h2o,state=CA,city=LA temp=90.0 200",
            "h2o,state=CA,city=LA temp=90.0 350",
        ];

        make_two_chunk_scenarios(partition_key, &lp_lines1.join("\n"), &lp_lines2.join("\n")).await
    }
}

#[tokio::test]
async fn test_read_group_data_pred() {
    let predicate = PredicateBuilder::default()
        .add_expr(col("city").eq(lit("LA")))
        .timestamp_range(190, 210)
        .build();
    let agg = Aggregate::Sum;
    let group_columns = vec!["state"];
    let expected_results = vec![
        "+-------+------+------+------+",
        "| state | city | temp | time |",
        "+-------+------+------+------+",
        "| CA    | LA   | 90   | 200  |",
        "+-------+------+------+------+",
    ];

    run_read_group_test_case!(
        OneMeasurementForAggs {},
        predicate,
        agg,
        group_columns,
        expected_results
    );
}

#[tokio::test]
async fn test_read_group_data_field_restriction() {
    // restrict to only the temp column
    let predicate = PredicateBuilder::default()
        .field_columns(vec!["temp"])
        .build();
    let agg = Aggregate::Sum;
    let group_columns = vec!["state"];
    let expected_results = vec![
        "+-------+--------+-------+------+",
        "| state | city   | temp  | time |",
        "+-------+--------+-------+------+",
        "| CA    | LA     | 180   | 550  |",
        "| MA    | Boston | 142.8 | 350  |",
        "+-------+--------+-------+------+",
    ];

    run_read_group_test_case!(
        OneMeasurementForAggs {},
        predicate,
        agg,
        group_columns,
        expected_results
    );
}

struct AnotherMeasurementForAggs {}
#[async_trait]
impl DBSetup for AnotherMeasurementForAggs {
    async fn make(&self) -> Vec<DBScenario> {
        let partition_key = "1970-01-01T00";

        let lp_lines1 = vec![
            "h2o,state=MA,city=Cambridge temp=80 50",
            "h2o,state=MA,city=Cambridge temp=81 100",
            "h2o,state=MA,city=Cambridge temp=82 200",
            "h2o,state=MA,city=Boston temp=70 300",
        ];
        let lp_lines2 = vec![
            "h2o,state=MA,city=Boston temp=71 400",
            "h2o,state=CA,city=LA temp=90,humidity=10 500",
            "h2o,state=CA,city=LA temp=91,humidity=11 600",
        ];

        make_two_chunk_scenarios(partition_key, &lp_lines1.join("\n"), &lp_lines2.join("\n")).await
    }
}

#[tokio::test]
async fn test_grouped_series_set_plan_sum() {
    let predicate = PredicateBuilder::default()
        // city=Boston OR city=Cambridge (filters out LA rows)
        .add_expr(
            col("city")
                .eq(lit("Boston"))
                .or(col("city").eq(lit("Cambridge"))),
        )
        // fiter out first Cambridge row
        .timestamp_range(100, 1000)
        .build();

    let agg = Aggregate::Sum;
    let group_columns = vec!["state"];

    // The null field (after predicates) are not sent as series
    // Note order of city key (boston --> cambridge)
    let expected_results = vec![
        "+-------+-----------+----------+------+------+",
        "| state | city      | humidity | temp | time |",
        "+-------+-----------+----------+------+------+",
        "| MA    | Boston    |          | 141  | 700  |",
        "| MA    | Cambridge |          | 163  | 300  |",
        "+-------+-----------+----------+------+------+",
    ];

    run_read_group_test_case!(
        AnotherMeasurementForAggs {},
        predicate,
        agg,
        group_columns,
        expected_results
    );
}

#[tokio::test]
async fn test_grouped_series_set_plan_count() {
    let predicate = PredicateBuilder::default()
        // city=Boston OR city=Cambridge (filters out LA rows)
        .add_expr(
            col("city")
                .eq(lit("Boston"))
                .or(col("city").eq(lit("Cambridge"))),
        )
        // fiter out first Cambridge row
        .timestamp_range(100, 1000)
        .build();

    let agg = Aggregate::Count;
    let group_columns = vec!["state"];

    let expected_results = vec![
        "+-------+-----------+----------+------+------+",
        "| state | city      | humidity | temp | time |",
        "+-------+-----------+----------+------+------+",
        "| MA    | Boston    | 0        | 2    | 2    |",
        "| MA    | Cambridge | 0        | 2    | 2    |",
        "+-------+-----------+----------+------+------+",
    ];

    run_read_group_test_case!(
        AnotherMeasurementForAggs {},
        predicate,
        agg,
        group_columns,
        expected_results
    );
}

#[tokio::test]
async fn test_grouped_series_set_plan_mean() {
    let predicate = PredicateBuilder::default()
        // city=Boston OR city=Cambridge (filters out LA rows)
        .add_expr(
            col("city")
                .eq(lit("Boston"))
                .or(col("city").eq(lit("Cambridge"))),
        )
        // fiter out first Cambridge row
        .timestamp_range(100, 1000)
        .build();

    let agg = Aggregate::Mean;
    let group_columns = vec!["state"];

    let expected_results = vec![
        "+-------+-----------+----------+------+------+",
        "| state | city      | humidity | temp | time |",
        "+-------+-----------+----------+------+------+",
        "| MA    | Boston    |          | 70.5 | 350  |",
        "| MA    | Cambridge |          | 81.5 | 150  |",
        "+-------+-----------+----------+------+------+",
    ];

    run_read_group_test_case!(
        AnotherMeasurementForAggs {},
        predicate,
        agg,
        group_columns,
        expected_results
    );
}

struct MeasurementForSelectors {}
#[async_trait]
impl DBSetup for MeasurementForSelectors {
    async fn make(&self) -> Vec<DBScenario> {
        let partition_key = "1970-01-01T00";

        let lp_lines1 = vec!["h2o,state=MA,city=Cambridge f=8.0,i=8i,b=true,s=\"d\" 1000"];
        let lp_lines2 = vec![
            "h2o,state=MA,city=Cambridge f=7.0,i=7i,b=true,s=\"c\" 2000",
            "h2o,state=MA,city=Cambridge f=6.0,i=6i,b=false,s=\"b\" 3000",
            "h2o,state=MA,city=Cambridge f=5.0,i=5i,b=false,s=\"a\" 4000",
        ];

        make_two_chunk_scenarios(partition_key, &lp_lines1.join("\n"), &lp_lines2.join("\n")).await
    }
}

#[tokio::test]
async fn test_grouped_series_set_plan_first() {
    let predicate = PredicateBuilder::default()
        // fiter out first row (ts 1000)
        .timestamp_range(1001, 4001)
        .build();

    let agg = Aggregate::First;
    let group_columns = vec!["state"];

    let expected_results = vec![
        "+-------+-----------+------+--------+---+--------+---+--------+---+--------+",
        "| state | city      | b    | time_b | f | time_f | i | time_i | s | time_s |",
        "+-------+-----------+------+--------+---+--------+---+--------+---+--------+",
        "| MA    | Cambridge | true | 2000   | 7 | 2000   | 7 | 2000   | c | 2000   |",
        "+-------+-----------+------+--------+---+--------+---+--------+---+--------+",
    ];

    run_read_group_test_case!(
        MeasurementForSelectors {},
        predicate,
        agg,
        group_columns,
        expected_results
    );
}

#[tokio::test]
async fn test_grouped_series_set_plan_last() {
    let predicate = PredicateBuilder::default()
        // fiter out last row (ts 4000)
        .timestamp_range(100, 3999)
        .build();

    let agg = Aggregate::Last;
    let group_columns = vec!["state"];

    let expected_results = vec![
        "+-------+-----------+-------+--------+---+--------+---+--------+---+--------+",
        "| state | city      | b     | time_b | f | time_f | i | time_i | s | time_s |",
        "+-------+-----------+-------+--------+---+--------+---+--------+---+--------+",
        "| MA    | Cambridge | false | 3000   | 6 | 3000   | 6 | 3000   | b | 3000   |",
        "+-------+-----------+-------+--------+---+--------+---+--------+---+--------+",
    ];

    run_read_group_test_case!(
        MeasurementForSelectors {},
        predicate,
        agg,
        group_columns,
        expected_results
    );
}

struct MeasurementForMin {}
#[async_trait]
impl DBSetup for MeasurementForMin {
    async fn make(&self) -> Vec<DBScenario> {
        let partition_key = "1970-01-01T00";

        let lp_lines1 = vec![
            "h2o,state=MA,city=Cambridge f=8.0,i=8i,b=false,s=\"c\" 1000",
            "h2o,state=MA,city=Cambridge f=7.0,i=7i,b=false,s=\"a\" 2000",
        ];
        let lp_lines2 = vec![
            "h2o,state=MA,city=Cambridge f=6.0,i=6i,b=true,s=\"z\" 3000",
            "h2o,state=MA,city=Cambridge f=5.0,i=5i,b=true,s=\"c\" 4000",
        ];

        make_two_chunk_scenarios(partition_key, &lp_lines1.join("\n"), &lp_lines2.join("\n")).await
    }
}

#[tokio::test]
async fn test_grouped_series_set_plan_min() {
    let predicate = PredicateBuilder::default()
        // fiter out last row (ts 4000)
        .timestamp_range(100, 3999)
        .build();

    let agg = Aggregate::Min;
    let group_columns = vec!["state"];

    let expected_results = vec![
        "+-------+-----------+-------+--------+---+--------+---+--------+---+--------+",
        "| state | city      | b     | time_b | f | time_f | i | time_i | s | time_s |",
        "+-------+-----------+-------+--------+---+--------+---+--------+---+--------+",
        "| MA    | Cambridge | false | 1000   | 6 | 3000   | 6 | 3000   | a | 2000   |",
        "+-------+-----------+-------+--------+---+--------+---+--------+---+--------+",
    ];

    run_read_group_test_case!(
        MeasurementForMin {},
        predicate,
        agg,
        group_columns,
        expected_results
    );
}

struct MeasurementForMax {}
#[async_trait]
impl DBSetup for MeasurementForMax {
    async fn make(&self) -> Vec<DBScenario> {
        let partition_key = "1970-01-01T00";

        let lp_lines1 = vec![
            "h2o,state=MA,city=Cambridge f=8.0,i=8i,b=true,s=\"c\" 1000",
            "h2o,state=MA,city=Cambridge f=7.0,i=7i,b=false,s=\"d\" 2000",
            "h2o,state=MA,city=Cambridge f=6.0,i=6i,b=true,s=\"a\" 3000",
        ];
        let lp_lines2 = vec!["h2o,state=MA,city=Cambridge f=5.0,i=5i,b=true,s=\"z\" 4000"];

        make_two_chunk_scenarios(partition_key, &lp_lines1.join("\n"), &lp_lines2.join("\n")).await
    }
}

#[tokio::test]
async fn test_grouped_series_set_plan_max() {
    let predicate = PredicateBuilder::default()
        // fiter out first row (ts 1000)
        .timestamp_range(1001, 4001)
        .build();

    let agg = Aggregate::Max;
    let group_columns = vec!["state"];

    let expected_results = vec![
        "+-------+-----------+------+--------+---+--------+---+--------+---+--------+",
        "| state | city      | b    | time_b | f | time_f | i | time_i | s | time_s |",
        "+-------+-----------+------+--------+---+--------+---+--------+---+--------+",
        "| MA    | Cambridge | true | 3000   | 7 | 2000   | 7 | 2000   | z | 4000   |",
        "+-------+-----------+------+--------+---+--------+---+--------+---+--------+",
    ];

    run_read_group_test_case!(
        MeasurementForMax {},
        predicate,
        agg,
        group_columns,
        expected_results
    );
}

struct MeasurementForGroupKeys {}
#[async_trait]
impl DBSetup for MeasurementForGroupKeys {
    async fn make(&self) -> Vec<DBScenario> {
        let partition_key = "1970-01-01T00";

        let lp_lines1 = vec![
            "h2o,state=MA,city=Cambridge temp=80 50",
            "h2o,state=MA,city=Cambridge temp=81 100",
            "h2o,state=MA,city=Cambridge temp=82 200",
        ];
        let lp_lines2 = vec![
            "h2o,state=MA,city=Boston temp=70 300",
            "h2o,state=MA,city=Boston temp=71 400",
            "h2o,state=CA,city=LA temp=90,humidity=10 500",
            "h2o,state=CA,city=LA temp=91,humidity=11 600",
        ];

        make_two_chunk_scenarios(partition_key, &lp_lines1.join("\n"), &lp_lines2.join("\n")).await
    }
}

#[tokio::test]
async fn test_grouped_series_set_plan_group_by_state_city() {
    // no predicate
    let predicate = PredicateBuilder::default().build();

    let agg = Aggregate::Sum;
    let group_columns = vec!["state", "city"];

    let expected_results = vec![
        "+-------+-----------+----------+------+------+",
        "| state | city      | humidity | temp | time |",
        "+-------+-----------+----------+------+------+",
        "| CA    | LA        | 21       | 181  | 1100 |",
        "| MA    | Boston    |          | 141  | 700  |",
        "| MA    | Cambridge |          | 243  | 350  |",
        "+-------+-----------+----------+------+------+",
    ];

    run_read_group_test_case!(
        MeasurementForGroupKeys {},
        predicate,
        agg,
        group_columns,
        expected_results
    );
}

#[tokio::test]
async fn test_grouped_series_set_plan_group_by_city_state() {
    // no predicate
    let predicate = PredicateBuilder::default().build();

    let agg = Aggregate::Sum;
    let group_columns = vec!["city", "state"];

    // Test with alternate group key order (note the order of columns is different)
    let expected_results = vec![
        "+-----------+-------+----------+------+------+",
        "| city      | state | humidity | temp | time |",
        "+-----------+-------+----------+------+------+",
        "| Boston    | MA    |          | 141  | 700  |",
        "| Cambridge | MA    |          | 243  | 350  |",
        "| LA        | CA    | 21       | 181  | 1100 |",
        "+-----------+-------+----------+------+------+",
    ];

    run_read_group_test_case!(
        MeasurementForGroupKeys {},
        predicate,
        agg,
        group_columns,
        expected_results
    );
}

#[tokio::test]
async fn test_grouped_series_set_plan_group_aggregate_none() {
    // no predicate
    let predicate = PredicateBuilder::default().build();

    let agg = Aggregate::None;
    let group_columns = vec!["city", "state"];

    // Expect order of the columns to begin with city/state
    let expected_results = vec![
        "+-----------+-------+----------+------+------+",
        "| city      | state | humidity | temp | time |",
        "+-----------+-------+----------+------+------+",
        "| Boston    | MA    |          | 70   | 300  |",
        "| Boston    | MA    |          | 71   | 400  |",
        "| Cambridge | MA    |          | 80   | 50   |",
        "| Cambridge | MA    |          | 81   | 100  |",
        "| Cambridge | MA    |          | 82   | 200  |",
        "| LA        | CA    | 10       | 90   | 500  |",
        "| LA        | CA    | 11       | 91   | 600  |",
        "+-----------+-------+----------+------+------+",
    ];

    run_read_group_test_case!(
        MeasurementForGroupKeys {},
        predicate,
        agg,
        group_columns,
        expected_results
    );
}
