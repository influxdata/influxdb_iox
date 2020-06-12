use arrow::util::pretty;
use delorean_query_benchmarks::{generate_test_data, query, Column};

fn main() {
    println!("generating test data");
    let batch_count = 1;
    let record_count = 1000000;
    let total_records = batch_count * record_count;
    let mut columns = vec![];

    for i in 1..50 as usize {
        let cardinality = i.pow(5);

        columns.push(Column::new_i64(
            format!("i64_{}", i).to_string(),
            Some(cardinality),
            false,
        ));
    }

    for i in 1..51 {
        columns.push(Column::new_f64(
            format!("f64_{}", i).to_string(),
            None,
            false,
        ))
    }

    columns.push(Column::new_i64("ts".to_string(), None, false));

    let (schema, data) = generate_test_data(batch_count, record_count, columns);

    let query_str = "select sum(i64_2) from foo where ts > 99 and i64_49 = 12345";
    println!("running query on {} records: {}", total_records, query_str);

    let now = std::time::Instant::now();
    let results = query(query_str, "foo", schema, data).unwrap();
    println!("ran query in {}.{} seconds", now.elapsed().as_secs(), now.elapsed().subsec_millis());

    pretty::print_batches(&results).unwrap();
}
