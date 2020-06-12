use arrow::util::pretty;
use delorean_query_benchmarks::{generate_wide_high_cardinality_data, query};

fn main() {
    println!("generating test data");
    let batch_count = 1;
    let record_count = 1000000;
    let total_records = batch_count * record_count;
    let (schema, data) = generate_wide_high_cardinality_data(batch_count, record_count);

    let query_str = "select count(i64_2) from foo where ts > 99 and i64_49 = 12345";
    println!("running query on {} records: {}", total_records, query_str);

    let now = std::time::Instant::now();
    let results = query(query_str, "foo", schema, data).unwrap();

    let elapsed = now.elapsed();
    println!("ran query in {}.{:03} seconds", elapsed.as_secs(), elapsed.subsec_millis());

    pretty::print_batches(&results).unwrap();
}
