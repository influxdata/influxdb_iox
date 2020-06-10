use arrow::util::pretty;
use delorean_query_benchmarks::{generate_test_data, query, Column};

const NUM_BATCHES: usize = 1;
const NUM_RECORDS: usize = 1000;

fn main() {
    println!("generating test data");
    let columns = vec![
        Column::new_i64("i64".to_string(), None, true),
        Column::new_f64("f64".to_string(), None, true),
        Column::new_i64("ts".to_string(), None, true),
        Column::new_utf8("tag".to_string(), None, true),
    ];
    let (schema, data) = generate_test_data(NUM_BATCHES, NUM_RECORDS, columns);

    let query_str = "select i64, ts, count(ts), min(i64) from foo where ts > 0 group by i64, ts";
    println!("running query: {}", query_str);

    let results = query(query_str, "foo", schema, data).unwrap();

    pretty::print_batches(&results).unwrap();
}
