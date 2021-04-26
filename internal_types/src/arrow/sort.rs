use std::collections::HashSet;
use std::convert::TryInto;
use std::sync::Arc;

use snafu::{ResultExt, Snafu};

use arrow_deps::arrow::{
    array::DictionaryArray,
    compute::{lexsort_to_indices, take, SortColumn},
    datatypes::Int32Type,
    error::ArrowError,
    record_batch::RecordBatch,
};

use crate::schema::{InfluxColumnType, Schema};

/// Database schema creation / validation errors.
#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("Error validating schema: {}", source,))]
    InvalidSchema { source: crate::schema::Error },

    #[snafu(display("Tag column '{}' was not a dictionary array", column))]
    InvalidTagColumn { column: usize },

    #[snafu(context(false))]
    ArrowError { source: ArrowError },
}

pub type Result<T, E = Error> = std::result::Result<T, E>;

/// Estimates the cardinality of a given dictionary
///
/// This is an estimate because it doesn't handle the case where
/// the dictionary contains duplicate values
///
pub fn estimate_cardinality(array: &DictionaryArray<Int32Type>) -> usize {
    let keys = array.keys();
    let group: HashSet<_> = keys.iter().filter_map(|x| x).collect();
    group.len()
}

/// Sorts rows lexicographically with respect to the tag columns in increasing
/// order of cardinality and finally with respect to time
pub fn sort_record_batch(batch: RecordBatch) -> Result<RecordBatch> {
    let schema: Schema = batch.schema().try_into().context(InvalidSchema)?;

    let mut tag_cardinalities = schema
        .iter()
        .enumerate()
        .filter_map(|(idx, (column_type, _))| match column_type {
            Some(InfluxColumnType::Tag) => Some(idx),
            _ => None,
        })
        .map(|idx| {
            let column = batch.column(idx);
            let dictionary = column
                .as_any()
                .downcast_ref()
                .ok_or(Error::InvalidTagColumn { column: idx })?;
            Ok((Arc::clone(column), estimate_cardinality(dictionary)))
        })
        .collect::<Result<Vec<_>>>()?;

    tag_cardinalities.sort_by_key(|x| x.1);

    let mut sort_columns: Vec<_> = tag_cardinalities
        .into_iter()
        .map(|(column, _)| SortColumn {
            values: column,
            options: None,
        })
        .collect();

    sort_columns.extend(
        schema
            .iter()
            .enumerate()
            .filter_map(|(idx, (column_type, _))| match column_type {
                Some(InfluxColumnType::Timestamp) => Some(SortColumn {
                    values: Arc::clone(batch.column(idx)),
                    options: None,
                }),
                _ => None,
            }),
    );

    let indices = lexsort_to_indices(&sort_columns, None)?;

    let columns = batch
        .columns()
        .iter()
        .map(|column| Ok(take(column.as_ref(), &indices, None)?))
        .collect::<Result<Vec<_>>>()?;

    // TODO: Record sort order in schema

    Ok(RecordBatch::try_new(schema.as_arrow(), columns).expect("failed to recreated sorted batch"))
}

#[cfg(test)]
mod tests {
    use arrow_deps::arrow::array::{Int32Array, StringArray, TimestampNanosecondArray};
    use arrow_deps::arrow::compute::cast;
    use arrow_deps::arrow::datatypes::DataType;

    use crate::schema::builder::SchemaBuilder;

    use super::*;

    #[test]
    fn test_estimate_cardinality() {
        let dict: DictionaryArray<Int32Type> =
            vec!["cupcakes", "foo", "foo", "cupcakes", "bongo", "bananas"]
                .into_iter()
                .collect();
        assert_eq!(estimate_cardinality(&dict), 4);
    }

    #[test]
    fn test_sort_record_batch() {
        let tag1: DictionaryArray<Int32Type> =
            vec!["cupcakes", "foo", "foo", "cupcakes", "bongo", "bananas"]
                .into_iter()
                .collect();

        let tag2: DictionaryArray<Int32Type> =
            vec!["stage", "prod", "prod", "prod", "stage", "prod"]
                .into_iter()
                .collect();

        let time = TimestampNanosecondArray::from(vec![0, 40, 20, 12, 54, 2]);
        let data = Int32Array::from(vec![32, 543, 2133, 2232, 33, 22]);

        let schema = SchemaBuilder::new()
            .tag("tag1")
            .tag("tag2")
            .timestamp()
            .field("data", DataType::Int32)
            .build()
            .unwrap();

        let batch = RecordBatch::try_new(
            schema.as_arrow(),
            vec![
                Arc::new(tag1),
                Arc::new(tag2),
                Arc::new(time),
                Arc::new(data),
            ],
        )
        .unwrap();

        let sorted = sort_record_batch(batch).unwrap();

        let column0 = cast(sorted.column(0), &DataType::Utf8).unwrap();
        let column0: Vec<_> = column0
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap()
            .iter()
            .filter_map(|x| x)
            .collect();

        let column1 = cast(sorted.column(1), &DataType::Utf8).unwrap();
        let column1: Vec<_> = column1
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap()
            .iter()
            .filter_map(|x| x)
            .collect();

        let column2 = sorted.column(2);
        let column2 = column2
            .as_any()
            .downcast_ref::<TimestampNanosecondArray>()
            .unwrap();

        let column3 = sorted.column(3);
        let column3 = column3.as_any().downcast_ref::<Int32Array>().unwrap();

        assert_eq!(
            column0,
            vec!["bananas", "cupcakes", "foo", "foo", "bongo", "cupcakes"]
        );
        assert_eq!(
            column1,
            vec!["prod", "prod", "prod", "prod", "stage", "stage"]
        );
        assert_eq!(column2.values(), &[2, 12, 20, 40, 54, 0]);
        assert_eq!(column3.values(), &[22, 2232, 2133, 543, 33, 32])
    }
}
