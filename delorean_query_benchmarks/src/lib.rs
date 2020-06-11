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
}

pub fn generate_test_data(
    num_batches: usize,
    num_records: usize,
    columns: Vec<Column>,
) -> (Arc<Schema>, Vec<RecordBatch>) {
    let mut fields: Vec<Field> = Vec::with_capacity(columns.len());

    for c in &columns {
        fields.push(Field::new(&c.name, c.data_type.clone(), c.nullable));
    }

    let schema = Arc::new(Schema::new(fields));

    let mut data: Vec<RecordBatch> = Vec::with_capacity(num_batches);

    for batch in 0..num_batches {
        let mut arrow_columns: Vec<ArrayRef> = Vec::with_capacity(columns.len());

        for c in &columns {
            let arrow_col: ArrayRef = match &c.data_type {
                DataType::Float64 => {
                    let mut builder = Float64Builder::new(num_records);
                    for rec in 0..num_records {
                        if c.nullable && rec % 2 == 0 {
                            builder.append_null().unwrap();
                        } else {
                            builder.append_value(rec as f64).unwrap();
                        }
                    }
                    Arc::new(builder.finish())
                }
                DataType::Int64 => {
                    let mut builder = Int64Builder::new(num_records);
                    for rec in 0..num_records {
                        if c.nullable && rec & 2 == 0 {
                            builder.append_null().unwrap();
                        } else {
                            match c.cardinality {
                                Some(num) => {
                                    let row_number = (batch + 1) * (rec + 1);
                                    let value = row_number % num;
                                    builder.append_value(value as i64).unwrap()
                                }
                                None => builder.append_value(rec as i64).unwrap(),
                            }
                        }
                    }
                    Arc::new(builder.finish())
                }
                DataType::Utf8 => {
                    let mut builder = StringBuilder::new(num_records);
                    for rec in 0..num_records {
                        if c.nullable && rec & 2 == 0 {
                            builder.append_null().unwrap();
                        } else {
                            match c.cardinality {
                                Some(num) => {
                                    let row_number = (batch + 1) * (rec + 1);
                                    let value = row_number % num;
                                    builder
                                        .append_value(&format!("value for {}-{}", c.name, value))
                                        .unwrap()
                                }
                                None => builder
                                    .append_value(&format!("value for {}-{}", c.name, rec))
                                    .unwrap(),
                            }
                        }
                    }
                    Arc::new(builder.finish())
                }
                dt => panic!(format!("data type {:?} not implemented", dt)),
            };

            arrow_columns.push(arrow_col)
        }

        let batch = RecordBatch::try_new(schema.clone(), arrow_columns).unwrap();

        data.push(batch);
    }

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
