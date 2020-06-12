use arrow::array::{ArrayRef, Float64Builder, Int64Builder, StringBuilder};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use datafusion::datasource::MemTable;
use datafusion::error::ExecutionError;
use datafusion::execution::context::ExecutionContext;
use std::sync::Arc;

pub struct Column {
    name: String,
    data_type: arrow::datatypes::DataType,
    cardinality: Option<usize>,
    nullable: bool,
}

impl Column {
    pub fn new_i64(name: String, cardinality: Option<usize>, nullable: bool) -> Column {
        Column {
            name,
            data_type: DataType::Int64,
            cardinality,
            nullable,
        }
    }

    pub fn new_f64(name: String, cardinality: Option<usize>, nullable: bool) -> Column {
        Column {
            name,
            data_type: DataType::Float64,
            cardinality,
            nullable,
        }
    }

    pub fn new_utf8(name: String, cardinality: Option<usize>, nullable: bool) -> Column {
        Column {
            name,
            data_type: DataType::Utf8,
            cardinality,
            nullable,
        }
    }

    // Generates a value for a column based on the row number. The presence of cardinality means
    // that this value represents some sort of identifier (like for a dictionary of tag value -> id)
    fn generate_value(&self, batch: usize, record: usize) -> i64 {
        let row_number = batch * record;

        let value = match self.cardinality {
            Some(c) => row_number % c,
            None => row_number,
        };

        value as i64
    }

    // returns true if the next generated value should be null
    fn next_value_should_be_null(&self, record: usize) -> bool {
       self.nullable && record & 2 == 0
    }
}

pub fn generate_wide_high_cardinality_data(batch_count: usize, record_count: usize) -> (Arc<Schema>, Vec<RecordBatch>) {
    let mut columns = vec![];

    for i in 1..=50_usize {
        let cardinality = i.pow(5);

        columns.push(Column::new_i64(
            format!("i64_{}", i),
            Some(cardinality),
            false,
        ));
    }

    for i in 1..=50 {
        columns.push(Column::new_f64(
            format!("f64_{}", i),
            None,
            false,
        ))
    }

    columns.push(Column::new_i64("ts".to_string(), None, false));

    generate_test_data(batch_count, record_count, columns)
}

pub fn generate_test_data(
    num_batches: usize,
    num_records: usize,
    columns: Vec<Column>,
) -> (Arc<Schema>, Vec<RecordBatch>) {
    let fields: Vec<_> = columns.iter().map(|c| {
        Field::new(&c.name, c.data_type.clone(), c.nullable)
    }).collect();

    let schema = Arc::new(Schema::new(fields));

    let data = (1..=num_batches).map(|batch| {
        let arrow_columns: Vec<_> = columns.iter().map(|c| {
            let arrow_col: ArrayRef = match &c.data_type {
                DataType::Float64 => {
                    let mut builder = Float64Builder::new(num_records);
                    for record in 1..=num_records {
                        if c.next_value_should_be_null(record) {
                            builder.append_null().unwrap();
                        } else {
                            builder.append_value(record as f64).unwrap();
                        }
                    }
                    Arc::new(builder.finish())
                }
                DataType::Int64 => {
                    let mut builder = Int64Builder::new(num_records);
                    for record in 1..=num_records {
                        if c.next_value_should_be_null(record) {
                            builder.append_null().unwrap();
                        } else {
                            let value = c.generate_value(batch, record);
                            builder.append_value(value).unwrap()
                        }
                    }
                    Arc::new(builder.finish())
                }
                DataType::Utf8 => {
                    let mut builder = StringBuilder::new(num_records);
                    for record in 1..=num_records {
                        if c.next_value_should_be_null(record) {
                            builder.append_null().unwrap();
                        } else {
                            let value = c.generate_value(batch, record);
                            builder.append_value(&format!("value for {}-{}", c.name, value)).unwrap()
                        }
                    }
                    Arc::new(builder.finish())
                }
                dt => panic!(format!("data type {:?} not implemented", dt)),
            };

            arrow_col
        }).collect();

        RecordBatch::try_new(schema.clone(), arrow_columns).unwrap()
    }).collect();

    (schema, data)
}

pub fn query(
    query: &str,
    table_name: &str,
    schema: Arc<Schema>,
    data: Vec<RecordBatch>,
) -> Result<Vec<RecordBatch>, ExecutionError> {
    let mut ctx = ExecutionContext::new();
    let provider = MemTable::new(schema, vec![data])?;
    ctx.register_table(table_name, Box::new(provider));

    let plan = ctx.create_logical_plan(&query)?;
    let plan = ctx.optimize(&plan)?;
    let plan = ctx.create_physical_plan(&plan, 1024 * 1024)?;

    ctx.collect(plan.as_ref())
}
