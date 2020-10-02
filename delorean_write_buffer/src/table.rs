use delorean_generated_types::wal as wb;
use delorean_storage::TimestampRange;

use std::{collections::HashMap, sync::Arc};

use crate::{
    column::Column,
    dictionary::{Dictionary, Error as DictionaryError},
    partition::Partition,
    wal::type_description,
};
use snafu::{OptionExt, ResultExt, Snafu};

use delorean_arrow::{
    arrow,
    arrow::{
        array::{ArrayRef, BooleanBuilder, Float64Builder, Int64Builder, StringBuilder},
        datatypes::{DataType as ArrowDataType, Field as ArrowField, Schema as ArrowSchema},
        record_batch::RecordBatch,
    },
};

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("Table {} not found", table))]
    TableNotFound { table: String },

    #[snafu(display("Column {} not found", column))]
    ColumnNotFound { column: String },

    #[snafu(display(
        "Column {} said it was type {} but extracting a value of that type failed",
        column,
        expected
    ))]
    WalValueTypeMismatch { column: String, expected: String },

    #[snafu(display(
        "Tag value ID {} not found in dictionary of partition {}",
        value,
        partition
    ))]
    TagValueIdNotFoundInDictionary {
        value: u32,
        partition: String,
        source: DictionaryError,
    },

    #[snafu(display(
        "Column type mismatch for column {}: can't insert {} into column with type {}",
        column,
        inserted_value_type,
        existing_column_type
    ))]
    ColumnTypeMismatch {
        column: String,
        existing_column_type: String,
        inserted_value_type: String,
    },

    #[snafu(display(
        "Column ID {} not found in dictionary of partition {}",
        column_id,
        partition
    ))]
    ColumnIdNotFoundInDictionary {
        column_id: u32,
        partition: String,
        source: DictionaryError,
    },

    #[snafu(display(
        "Column name '{}' not found in dictionary of partition {}",
        column_name,
        partition
    ))]
    ColumnNameNotFoundInDictionary {
        column_name: String,
        partition: String,
        source: DictionaryError,
    },

    #[snafu(display(
        "Schema mismatch: for column {}: can't insert {} into column with type {}",
        column,
        inserted_value_type,
        existing_column_type
    ))]
    SchemaMismatch {
        column: u32,
        existing_column_type: String,
        inserted_value_type: String,
    },

    #[snafu(display("arrow conversion error: {}", source))]
    ArrowError { source: arrow::error::ArrowError },

    #[snafu(display("Schema mismatch: for column {}: {}", column, source))]
    InternalSchemaMismatch {
        column: u32,
        source: crate::column::Error,
    },

    #[snafu(display(
        "No index entry found for column {} with id {}",
        column_name,
        column_id
    ))]
    InternalNoColumnInIndex { column_name: String, column_id: u32 },

    #[snafu(display("Error creating column from wal for column {}: {}", column, source))]
    CreatingFromWal {
        column: u32,
        source: crate::column::Error,
    },

    #[snafu(display("Error evaluating column predicate for column {}: {}", column, source))]
    ColumnPredicateEvaluation {
        column: u32,
        source: crate::column::Error,
    },

    #[snafu(display("Row insert to table {} missing column name", table))]
    ColumnNameNotInRow { table: u32 },
}
pub type Result<T, E = Error> = std::result::Result<T, E>;

#[derive(Debug, Clone, Copy)]
pub struct TimestampPredicate {
    pub time_column_id: u32,
    pub range: TimestampRange,
}

#[derive(Debug)]
pub struct Table {
    pub id: u32,
    /// Maps column name (as a u32 in the partition dictionary) to an index in self.columns
    pub column_id_to_index: HashMap<u32, usize>,
    pub columns: Vec<Column>,
}

impl Table {
    pub fn new(id: u32) -> Self {
        Self {
            id,
            column_id_to_index: HashMap::new(),
            columns: Vec::new(),
        }
    }

    fn append_row(
        &mut self,
        dictionary: &mut Dictionary,
        values: &flatbuffers::Vector<'_, flatbuffers::ForwardsUOffset<wb::Value<'_>>>,
    ) -> Result<()> {
        let row_count = self.row_count();

        // insert new columns and validate existing ones
        for value in values {
            let column_name = value
                .column()
                .context(ColumnNameNotInRow { table: self.id })?;
            let column_id = dictionary.lookup_value_or_insert(column_name);

            let mut column = match self.column_id_to_index.get(&column_id) {
                Some(idx) => &mut self.columns[*idx],
                None => {
                    // Add the column and make all values for existing rows None
                    let idx = self.columns.len();
                    self.column_id_to_index.insert(column_id, idx);
                    self.columns.push(
                        Column::new_from_wal(row_count, value.value_type())
                            .context(CreatingFromWal { column: column_id })?,
                    );

                    &mut self.columns[idx]
                }
            };

            if let (Column::Bool(vals), Some(v)) = (&mut column, value.value_as_bool_value()) {
                vals.push(Some(v.value()));
            } else if let (Column::I64(vals), Some(v)) = (&mut column, value.value_as_i64value()) {
                vals.push(Some(v.value()));
            } else if let (Column::F64(vals), Some(v)) = (&mut column, value.value_as_f64value()) {
                vals.push(Some(v.value()));
            } else if let (Column::String(vals), Some(v)) =
                (&mut column, value.value_as_string_value())
            {
                vals.push(Some(v.value().unwrap().to_string()));
            } else if let (Column::Tag(vals), Some(v)) = (&mut column, value.value_as_tag_value()) {
                let v_id = dictionary.lookup_value_or_insert(v.value().unwrap());

                vals.push(Some(v_id));
            } else {
                return ColumnTypeMismatch {
                    column: column_name,
                    existing_column_type: column.type_description(),
                    inserted_value_type: type_description(value.value_type()),
                }
                .fail();
            }
        }

        // make sure all the columns are of the same length
        for col in &mut self.columns {
            col.push_none_if_len_equal(row_count);
        }

        Ok(())
    }

    pub fn row_count(&self) -> usize {
        self.columns.first().map_or(0, |v| v.len())
    }

    /// Returns a reference to the specified column
    fn column(&self, column_id: u32) -> Result<&Column> {
        Ok(self
            .column_id_to_index
            .get(&column_id)
            .map(|column_index| &self.columns[*column_index])
            .expect("invalid column id"))
    }

    pub fn append_rows(
        &mut self,
        dictionary: &mut Dictionary,
        rows: &flatbuffers::Vector<'_, flatbuffers::ForwardsUOffset<wb::Row<'_>>>,
    ) -> Result<()> {
        for row in rows {
            if let Some(values) = row.values() {
                self.append_row(dictionary, &values)?;
            }
        }

        Ok(())
    }

    /// Converts this table to an arrow record batch.
    pub fn to_arrow(
        &self,
        partition: &Partition,
        requested_columns: &[&str],
    ) -> Result<RecordBatch> {
        // only retrieve the requested columns
        let requested_columns_with_index =
            requested_columns
                .iter()
                .map(|column_name| {
                    let column_name = *column_name;
                    let column_id = partition.dictionary.lookup_value(column_name).context(
                        ColumnNameNotFoundInDictionary {
                            column_name,
                            partition: &partition.key,
                        },
                    )?;

                    let column_index = *self.column_id_to_index.get(&column_id).context(
                        InternalNoColumnInIndex {
                            column_name,
                            column_id,
                        },
                    )?;

                    Ok((column_name, column_index))
                })
                .collect::<Result<Vec<_>>>()?;

        let mut fields = Vec::with_capacity(requested_columns_with_index.len());
        let mut columns: Vec<ArrayRef> = Vec::with_capacity(requested_columns_with_index.len());

        for (column_name, column_index) in requested_columns_with_index.into_iter() {
            let arrow_col: ArrayRef = match &self.columns[column_index] {
                Column::String(vals) => {
                    fields.push(ArrowField::new(column_name, ArrowDataType::Utf8, true));
                    let mut builder = StringBuilder::with_capacity(vals.len(), vals.len() * 10);

                    for v in vals {
                        match v {
                            None => builder.append_null(),
                            Some(s) => builder.append_value(s),
                        }
                        .context(ArrowError {})?;
                    }

                    Arc::new(builder.finish())
                }
                Column::Tag(vals) => {
                    fields.push(ArrowField::new(column_name, ArrowDataType::Utf8, true));
                    let mut builder = StringBuilder::with_capacity(vals.len(), vals.len() * 10);

                    for v in vals {
                        match v {
                            None => builder.append_null(),
                            Some(value_id) => {
                                let tag_value = partition.dictionary.lookup_id(*value_id).context(
                                    TagValueIdNotFoundInDictionary {
                                        value: *value_id,
                                        partition: &partition.key,
                                    },
                                )?;
                                builder.append_value(tag_value)
                            }
                        }
                        .context(ArrowError {})?;
                    }

                    Arc::new(builder.finish())
                }
                Column::F64(vals) => {
                    fields.push(ArrowField::new(column_name, ArrowDataType::Float64, true));
                    let mut builder = Float64Builder::new(vals.len());

                    for v in vals {
                        builder.append_option(*v).context(ArrowError {})?;
                    }

                    Arc::new(builder.finish())
                }
                Column::I64(vals) => {
                    fields.push(ArrowField::new(column_name, ArrowDataType::Int64, true));
                    let mut builder = Int64Builder::new(vals.len());

                    for v in vals {
                        builder.append_option(*v).context(ArrowError {})?;
                    }

                    Arc::new(builder.finish())
                }
                Column::Bool(vals) => {
                    fields.push(ArrowField::new(column_name, ArrowDataType::Boolean, true));
                    let mut builder = BooleanBuilder::new(vals.len());

                    for v in vals {
                        builder.append_option(*v).context(ArrowError {})?;
                    }

                    Arc::new(builder.finish())
                }
            };

            columns.push(arrow_col);
        }

        let schema = ArrowSchema::new(fields);

        RecordBatch::try_new(Arc::new(schema), columns).context(ArrowError {})
    }

    /// returns true if this table should be included in a query that
    /// has an optional table_symbol_predicate. Returns true f the
    /// table_symbol_predicate is not preset, or the table's id
    pub fn matches_id_predicate(&self, table_symbol_predicate: &Option<u32>) -> bool {
        match table_symbol_predicate {
            None => true,
            Some(table_symbol) => self.id == *table_symbol,
        }
    }

    /// returns true if there are any timestamps in this table that
    /// fall within the timestamp range
    pub fn matches_timestamp_predicate(&self, pred: &Option<TimestampPredicate>) -> Result<bool> {
        match pred {
            None => Ok(true),
            Some(pred) => {
                let time_column = self.column(pred.time_column_id)?;
                time_column
                    .has_i64_range(pred.range.start, pred.range.end)
                    .context(ColumnPredicateEvaluation {
                        column: pred.time_column_id,
                    })
            }
        }
    }

    /// returns true if there are any rows in column that are non-null
    /// and within the timestamp range specified by pred
    pub fn column_matches_timestamp_predicate<T>(
        &self,
        column: &[Option<T>],
        pred: &Option<TimestampPredicate>,
    ) -> Result<bool> {
        match pred {
            None => Ok(true),
            Some(pred) => {
                let time_column = self.column(pred.time_column_id)?;
                time_column
                    .has_non_null_i64_range(column, pred.range.start, pred.range.end)
                    .context(ColumnPredicateEvaluation {
                        column: pred.time_column_id,
                    })
            }
        }
    }
}
