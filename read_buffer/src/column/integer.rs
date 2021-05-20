use std::iter::FromIterator;

use arrow::{
    self,
    array::{Array, PrimitiveArray},
    datatypes::Int16Type as ArrowInt16Type,
    datatypes::Int32Type as ArrowInt32Type,
    datatypes::Int64Type as ArrowInt64Type,
    datatypes::Int8Type as ArrowInt8Type,
    datatypes::UInt16Type as ArrowUInt16Type,
    datatypes::UInt32Type as ArrowUInt32Type,
    datatypes::UInt64Type as ArrowUInt64Type,
    datatypes::UInt8Type as ArrowUInt8Type,
};
use either::Either;

use super::encoding::scalar::ScalarEncoding;
use super::{cmp, Statistics};
use crate::column::{EncodedValues, RowIDs, Scalar, Value, Values};

pub enum IntegerEncoding {
    // non-null encodings. These are backed by `Vec<T>`
    I64I64(ScalarEncoding<i64, ArrowInt64Type>),
    I64I32(ScalarEncoding<i32, ArrowInt32Type>),
    I64U32(ScalarEncoding<u32, ArrowUInt32Type>),
    I64I16(ScalarEncoding<i16, ArrowInt16Type>),
    I64U16(ScalarEncoding<u16, ArrowUInt16Type>),
    I64I8(ScalarEncoding<i8, ArrowInt8Type>),
    I64U8(ScalarEncoding<u8, ArrowUInt8Type>),
    U64U64(ScalarEncoding<u64, ArrowUInt64Type>),
    U64U32(ScalarEncoding<u32, ArrowUInt32Type>),
    U64U16(ScalarEncoding<u16, ArrowUInt16Type>),
    U64U8(ScalarEncoding<u8, ArrowUInt8Type>),
}

impl IntegerEncoding {
    /// The total size in bytes of the store columnar data.
    pub fn size(&self) -> usize {
        match self {
            Self::I64I64(enc) => enc.size(),
            Self::I64I32(enc) => enc.size(),
            Self::I64U32(enc) => enc.size(),
            Self::I64I16(enc) => enc.size(),
            Self::I64U16(enc) => enc.size(),
            Self::I64I8(enc) => enc.size(),
            Self::I64U8(enc) => enc.size(),
            Self::U64U64(enc) => enc.size(),
            Self::U64U32(enc) => enc.size(),
            Self::U64U16(enc) => enc.size(),
            Self::U64U8(enc) => enc.size(),
        }
    }

    /// The estimated total size in bytes of the underlying integer values in
    /// the column if they were stored contiguously and uncompressed (natively
    /// as i64/u64). `include_nulls` will effectively size each NULL value as 8b if
    /// `true`.
    pub fn size_raw(&self, include_nulls: bool) -> usize {
        match self {
            Self::I64I64(enc) => enc.size_raw(include_nulls),
            Self::I64I32(enc) => enc.size_raw(include_nulls),
            Self::I64U32(enc) => enc.size_raw(include_nulls),
            Self::I64I16(enc) => enc.size_raw(include_nulls),
            Self::I64U16(enc) => enc.size_raw(include_nulls),
            Self::I64I8(enc) => enc.size_raw(include_nulls),
            Self::I64U8(enc) => enc.size_raw(include_nulls),
            Self::U64U64(enc) => enc.size_raw(include_nulls),
            Self::U64U32(enc) => enc.size_raw(include_nulls),
            Self::U64U16(enc) => enc.size_raw(include_nulls),
            Self::U64U8(enc) => enc.size_raw(include_nulls),
        }
    }

    /// The total number of rows in the column.
    pub fn num_rows(&self) -> u32 {
        match self {
            Self::I64I64(enc) => enc.num_rows(),
            Self::I64I32(enc) => enc.num_rows(),
            Self::I64U32(enc) => enc.num_rows(),
            Self::I64I16(enc) => enc.num_rows(),
            Self::I64U16(enc) => enc.num_rows(),
            Self::I64I8(enc) => enc.num_rows(),
            Self::I64U8(enc) => enc.num_rows(),
            Self::U64U64(enc) => enc.num_rows(),
            Self::U64U32(enc) => enc.num_rows(),
            Self::U64U16(enc) => enc.num_rows(),
            Self::U64U8(enc) => enc.num_rows(),
        }
    }

    // Returns statistics about the physical layout of columns
    pub(crate) fn storage_stats(&self) -> Statistics {
        Statistics {
            enc_type: self.name(),
            log_data_type: self.logical_datatype(),
            values: self.num_rows(),
            nulls: self.null_count(),
            bytes: self.size(),
            raw_bytes: self.size_raw(true),
            raw_bytes_no_null: self.size_raw(false),
        }
    }

    /// Determines if the column contains a NULL value.
    pub fn contains_null(&self) -> bool {
        match self {
            Self::I64I64(enc) => enc.contains_null(),
            Self::I64I32(enc) => enc.contains_null(),
            Self::I64U32(enc) => enc.contains_null(),
            Self::I64I16(enc) => enc.contains_null(),
            Self::I64U16(enc) => enc.contains_null(),
            Self::I64I8(enc) => enc.contains_null(),
            Self::I64U8(enc) => enc.contains_null(),
            Self::U64U64(enc) => enc.contains_null(),
            Self::U64U32(enc) => enc.contains_null(),
            Self::U64U16(enc) => enc.contains_null(),
            Self::U64U8(enc) => enc.contains_null(),
        }
    }

    /// The total number of rows in the column.
    pub fn null_count(&self) -> u32 {
        match self {
            Self::I64I64(enc) => enc.null_count(),
            Self::I64I32(enc) => enc.null_count(),
            Self::I64U32(enc) => enc.null_count(),
            Self::I64I16(enc) => enc.null_count(),
            Self::I64U16(enc) => enc.null_count(),
            Self::I64I8(enc) => enc.null_count(),
            Self::I64U8(enc) => enc.null_count(),
            Self::U64U64(enc) => enc.null_count(),
            Self::U64U32(enc) => enc.null_count(),
            Self::U64U16(enc) => enc.null_count(),
            Self::U64U8(enc) => enc.null_count(),
        }
    }

    /// Determines if the column contains a non-null value.
    pub fn has_any_non_null_value(&self) -> bool {
        match self {
            Self::I64I64(enc) => enc.has_any_non_null_value(),
            Self::I64I32(enc) => enc.has_any_non_null_value(),
            Self::I64U32(enc) => enc.has_any_non_null_value(),
            Self::I64I16(enc) => enc.has_any_non_null_value(),
            Self::I64U16(enc) => enc.has_any_non_null_value(),
            Self::I64I8(enc) => enc.has_any_non_null_value(),
            Self::I64U8(enc) => enc.has_any_non_null_value(),
            Self::U64U64(enc) => enc.has_any_non_null_value(),
            Self::U64U32(enc) => enc.has_any_non_null_value(),
            Self::U64U16(enc) => enc.has_any_non_null_value(),
            Self::U64U8(enc) => enc.has_any_non_null_value(),
        }
    }

    /// Determines if the column contains a non-null value at one of the
    /// provided rows.
    pub fn has_non_null_value(&self, row_ids: &[u32]) -> bool {
        match self {
            Self::I64I64(enc) => enc.has_non_null_value(row_ids),
            Self::I64I32(enc) => enc.has_non_null_value(row_ids),
            Self::I64U32(enc) => enc.has_non_null_value(row_ids),
            Self::I64I16(enc) => enc.has_non_null_value(row_ids),
            Self::I64U16(enc) => enc.has_non_null_value(row_ids),
            Self::I64I8(enc) => enc.has_non_null_value(row_ids),
            Self::I64U8(enc) => enc.has_non_null_value(row_ids),
            Self::U64U64(enc) => enc.has_non_null_value(row_ids),
            Self::U64U32(enc) => enc.has_non_null_value(row_ids),
            Self::U64U16(enc) => enc.has_non_null_value(row_ids),
            Self::U64U8(enc) => enc.has_non_null_value(row_ids),
        }
    }

    /// Returns the logical value found at the provided row id.
    pub fn value(&self, row_id: u32) -> Value<'_> {
        match &self {
            // N.B., The `Scalar` variant determines the physical type `U` that
            // `c.value` should return as the logical type.
            Self::I64I64(enc) => match enc.value(row_id) {
                Some(v) => Value::Scalar(Scalar::I64(v)),
                None => Value::Null,
            },
            Self::I64I32(enc) => match enc.value(row_id) {
                Some(v) => Value::Scalar(Scalar::I64(v)),
                None => Value::Null,
            },
            Self::I64U32(enc) => match enc.value(row_id) {
                Some(v) => Value::Scalar(Scalar::I64(v)),
                None => Value::Null,
            },
            Self::I64I16(enc) => match enc.value(row_id) {
                Some(v) => Value::Scalar(Scalar::I64(v)),
                None => Value::Null,
            },
            Self::I64U16(enc) => match enc.value(row_id) {
                Some(v) => Value::Scalar(Scalar::I64(v)),
                None => Value::Null,
            },
            Self::I64I8(enc) => match enc.value(row_id) {
                Some(v) => Value::Scalar(Scalar::I64(v)),
                None => Value::Null,
            },
            Self::I64U8(enc) => match enc.value(row_id) {
                Some(v) => Value::Scalar(Scalar::I64(v)),
                None => Value::Null,
            },
            Self::U64U64(enc) => match enc.value(row_id) {
                Some(v) => Value::Scalar(Scalar::U64(v)),
                None => Value::Null,
            },
            Self::U64U32(enc) => match enc.value(row_id) {
                Some(v) => Value::Scalar(Scalar::U64(v)),
                None => Value::Null,
            },
            Self::U64U16(enc) => match enc.value(row_id) {
                Some(v) => Value::Scalar(Scalar::U64(v)),
                None => Value::Null,
            },
            Self::U64U8(enc) => match enc.value(row_id) {
                Some(v) => Value::Scalar(Scalar::U64(v)),
                None => Value::Null,
            },
        }
    }

    /// Returns the logical values found at the provided row ids.
    pub fn values(&self, row_ids: &[u32]) -> Values<'_> {
        match &self {
            Self::I64I64(enc) => match enc.values(row_ids) {
                Either::Left(values) => Values::I64(values),
                Either::Right(values) => Values::I64N(values),
            },
            Self::I64I32(enc) => match enc.values(row_ids) {
                Either::Left(values) => Values::I64(values),
                Either::Right(values) => Values::I64N(values),
            },
            Self::I64U32(enc) => match enc.values(row_ids) {
                Either::Left(values) => Values::I64(values),
                Either::Right(values) => Values::I64N(values),
            },
            Self::I64I16(enc) => match enc.values(row_ids) {
                Either::Left(values) => Values::I64(values),
                Either::Right(values) => Values::I64N(values),
            },
            Self::I64U16(enc) => match enc.values(row_ids) {
                Either::Left(values) => Values::I64(values),
                Either::Right(values) => Values::I64N(values),
            },
            Self::I64I8(enc) => match enc.values(row_ids) {
                Either::Left(values) => Values::I64(values),
                Either::Right(values) => Values::I64N(values),
            },
            Self::I64U8(enc) => match enc.values(row_ids) {
                Either::Left(values) => Values::I64(values),
                Either::Right(values) => Values::I64N(values),
            },
            Self::U64U64(enc) => match enc.values(row_ids) {
                Either::Left(values) => Values::U64(values),
                Either::Right(values) => Values::U64N(values),
            },
            Self::U64U32(enc) => match enc.values(row_ids) {
                Either::Left(values) => Values::U64(values),
                Either::Right(values) => Values::U64N(values),
            },
            Self::U64U16(enc) => match enc.values(row_ids) {
                Either::Left(values) => Values::U64(values),
                Either::Right(values) => Values::U64N(values),
            },
            Self::U64U8(enc) => match enc.values(row_ids) {
                Either::Left(values) => Values::U64(values),
                Either::Right(values) => Values::U64N(values),
            },
        }
    }

    /// Returns all logical values in the column.
    pub fn all_values(&self) -> Values<'_> {
        match &self {
            Self::I64I64(enc) => match enc.all_values() {
                Either::Left(values) => Values::I64(values),
                Either::Right(values) => Values::I64N(values),
            },
            Self::I64I32(enc) => match enc.all_values() {
                Either::Left(values) => Values::I64(values),
                Either::Right(values) => Values::I64N(values),
            },
            Self::I64U32(enc) => match enc.all_values() {
                Either::Left(values) => Values::I64(values),
                Either::Right(values) => Values::I64N(values),
            },
            Self::I64I16(enc) => match enc.all_values() {
                Either::Left(values) => Values::I64(values),
                Either::Right(values) => Values::I64N(values),
            },
            Self::I64U16(enc) => match enc.all_values() {
                Either::Left(values) => Values::I64(values),
                Either::Right(values) => Values::I64N(values),
            },
            Self::I64I8(enc) => match enc.all_values() {
                Either::Left(values) => Values::I64(values),
                Either::Right(values) => Values::I64N(values),
            },
            Self::I64U8(enc) => match enc.all_values() {
                Either::Left(values) => Values::I64(values),
                Either::Right(values) => Values::I64N(values),
            },
            Self::U64U64(enc) => match enc.all_values() {
                Either::Left(values) => Values::U64(values),
                Either::Right(values) => Values::U64N(values),
            },
            Self::U64U32(enc) => match enc.all_values() {
                Either::Left(values) => Values::U64(values),
                Either::Right(values) => Values::U64N(values),
            },
            Self::U64U16(enc) => match enc.all_values() {
                Either::Left(values) => Values::U64(values),
                Either::Right(values) => Values::U64N(values),
            },
            Self::U64U8(enc) => match enc.all_values() {
                Either::Left(values) => Values::U64(values),
                Either::Right(values) => Values::U64N(values),
            },
        }
    }

    /// Returns the encoded values found at the provided row ids. For an
    /// `IntegerEncoding` the encoded values are typically just the raw values.
    pub fn encoded_values(&self, row_ids: &[u32], dst: EncodedValues) -> EncodedValues {
        // Right now the use-case for encoded values on non-string columns is
        // that it's used for grouping with timestamp columns, which should be
        // non-null signed 64-bit integers.
        match dst {
            EncodedValues::I64(_) => match self {
                Self::I64I64(enc) => EncodedValues::I64(enc.encoded_values(row_ids)),
                Self::I64I32(enc) => EncodedValues::I64(enc.encoded_values(row_ids)),
                Self::I64U32(enc) => EncodedValues::I64(enc.encoded_values(row_ids)),
                Self::I64I16(enc) => EncodedValues::I64(enc.encoded_values(row_ids)),
                Self::I64U16(enc) => EncodedValues::I64(enc.encoded_values(row_ids)),
                Self::I64I8(enc) => EncodedValues::I64(enc.encoded_values(row_ids)),
                Self::I64U8(enc) => EncodedValues::I64(enc.encoded_values(row_ids)),
                _ => unreachable!("encoded values on encoding type not currently supported"),
            },
            _ => unreachable!("currently only support encoded values as i64"),
        }
    }

    /// All encoded values for the column. For `IntegerEncoding` this is
    /// typically equivalent to `all_values`.
    pub fn all_encoded_values(&self, dst: EncodedValues) -> EncodedValues {
        // Right now the use-case for encoded values on non-string columns is
        // that it's used for grouping with timestamp columns, which should be
        // non-null signed 64-bit integers.
        match dst {
            EncodedValues::I64(_) => match &self {
                Self::I64I64(enc) => EncodedValues::I64(enc.all_encoded_values()),
                Self::I64I32(enc) => EncodedValues::I64(enc.all_encoded_values()),
                Self::I64U32(enc) => EncodedValues::I64(enc.all_encoded_values()),
                Self::I64I16(enc) => EncodedValues::I64(enc.all_encoded_values()),
                Self::I64U16(enc) => EncodedValues::I64(enc.all_encoded_values()),
                Self::I64I8(enc) => EncodedValues::I64(enc.all_encoded_values()),
                Self::I64U8(enc) => EncodedValues::I64(enc.all_encoded_values()),
                _ => unreachable!("encoded values on encoding type not supported"),
            },
            _ => unreachable!("currently only support encoded values as i64"),
        }
    }

    /// Returns the row ids that satisfy the provided predicate.
    ///
    /// Note: it is the caller's responsibility to ensure that the provided
    /// `Scalar` value will fit within the physical type of the encoded column.
    /// `row_ids_filter` will panic if this invariant is broken.
    pub fn row_ids_filter(&self, op: &cmp::Operator, value: &Scalar, dst: RowIDs) -> RowIDs {
        match &self {
            Self::I64I64(enc) => enc.row_ids_filter(value.as_i64(), op, dst),
            Self::I64I32(enc) => enc.row_ids_filter(value.as_i32(), op, dst),
            Self::I64U32(enc) => enc.row_ids_filter(value.as_u32(), op, dst),
            Self::I64I16(enc) => enc.row_ids_filter(value.as_i16(), op, dst),
            Self::I64U16(enc) => enc.row_ids_filter(value.as_u16(), op, dst),
            Self::I64I8(enc) => enc.row_ids_filter(value.as_i8(), op, dst),
            Self::I64U8(enc) => enc.row_ids_filter(value.as_u8(), op, dst),
            Self::U64U64(enc) => enc.row_ids_filter(value.as_u64(), op, dst),
            Self::U64U32(enc) => enc.row_ids_filter(value.as_u32(), op, dst),
            Self::U64U16(enc) => enc.row_ids_filter(value.as_u16(), op, dst),
            Self::U64U8(enc) => enc.row_ids_filter(value.as_u8(), op, dst),
        }
    }

    /// Returns the row ids that satisfy both the provided predicates.
    ///
    /// Note: it is the caller's responsibility to ensure that the provided
    /// `Scalar` value will fit within the physical type of the encoded column.
    /// `row_ids_filter` will panic if this invariant is broken.
    pub fn row_ids_filter_range(
        &self,
        low: (&cmp::Operator, &Scalar),
        high: (&cmp::Operator, &Scalar),
        dst: RowIDs,
    ) -> RowIDs {
        match &self {
            Self::I64I64(enc) => {
                enc.row_ids_filter_range((low.1.as_i64(), low.0), (high.1.as_i64(), high.0), dst)
            }
            Self::I64I32(enc) => {
                enc.row_ids_filter_range((low.1.as_i32(), low.0), (high.1.as_i32(), high.0), dst)
            }
            Self::I64U32(enc) => {
                enc.row_ids_filter_range((low.1.as_u32(), low.0), (high.1.as_u32(), high.0), dst)
            }
            Self::I64I16(enc) => {
                enc.row_ids_filter_range((low.1.as_i16(), low.0), (high.1.as_i16(), high.0), dst)
            }
            Self::I64U16(enc) => {
                enc.row_ids_filter_range((low.1.as_u16(), low.0), (high.1.as_u16(), high.0), dst)
            }
            Self::I64I8(enc) => {
                enc.row_ids_filter_range((low.1.as_i8(), low.0), (high.1.as_i8(), high.0), dst)
            }
            Self::I64U8(enc) => {
                enc.row_ids_filter_range((low.1.as_u8(), low.0), (high.1.as_u8(), high.0), dst)
            }
            Self::U64U64(enc) => {
                enc.row_ids_filter_range((low.1.as_u64(), low.0), (high.1.as_u64(), high.0), dst)
            }
            Self::U64U32(enc) => {
                enc.row_ids_filter_range((low.1.as_u32(), low.0), (high.1.as_u32(), high.0), dst)
            }
            Self::U64U16(enc) => {
                enc.row_ids_filter_range((low.1.as_u16(), low.0), (high.1.as_u16(), high.0), dst)
            }
            Self::U64U8(enc) => {
                enc.row_ids_filter_range((low.1.as_u8(), low.0), (high.1.as_u8(), high.0), dst)
            }
        }
    }

    pub fn min(&self, row_ids: &[u32]) -> Value<'_> {
        match &self {
            Self::I64I64(enc) => match enc.min(row_ids) {
                Some(v) => Value::Scalar(Scalar::I64(v)),
                None => Value::Null,
            },
            Self::I64I32(enc) => match enc.min(row_ids) {
                Some(v) => Value::Scalar(Scalar::I64(v)),
                None => Value::Null,
            },
            Self::I64U32(enc) => match enc.min(row_ids) {
                Some(v) => Value::Scalar(Scalar::I64(v)),
                None => Value::Null,
            },
            Self::I64I16(enc) => match enc.min(row_ids) {
                Some(v) => Value::Scalar(Scalar::I64(v)),
                None => Value::Null,
            },
            Self::I64U16(enc) => match enc.min(row_ids) {
                Some(v) => Value::Scalar(Scalar::I64(v)),
                None => Value::Null,
            },
            Self::I64I8(enc) => match enc.min(row_ids) {
                Some(v) => Value::Scalar(Scalar::I64(v)),
                None => Value::Null,
            },
            Self::I64U8(enc) => match enc.min(row_ids) {
                Some(v) => Value::Scalar(Scalar::I64(v)),
                None => Value::Null,
            },
            Self::U64U64(enc) => match enc.min(row_ids) {
                Some(v) => Value::Scalar(Scalar::U64(v)),
                None => Value::Null,
            },
            Self::U64U32(enc) => match enc.min(row_ids) {
                Some(v) => Value::Scalar(Scalar::U64(v)),
                None => Value::Null,
            },
            Self::U64U16(enc) => match enc.min(row_ids) {
                Some(v) => Value::Scalar(Scalar::U64(v)),
                None => Value::Null,
            },
            Self::U64U8(enc) => match enc.min(row_ids) {
                Some(v) => Value::Scalar(Scalar::U64(v)),
                None => Value::Null,
            },
        }
    }

    pub fn max(&self, row_ids: &[u32]) -> Value<'_> {
        match &self {
            Self::I64I64(enc) => match enc.max(row_ids) {
                Some(v) => Value::Scalar(Scalar::I64(v)),
                None => Value::Null,
            },
            Self::I64I32(enc) => match enc.max(row_ids) {
                Some(v) => Value::Scalar(Scalar::I64(v)),
                None => Value::Null,
            },
            Self::I64U32(enc) => match enc.max(row_ids) {
                Some(v) => Value::Scalar(Scalar::I64(v)),
                None => Value::Null,
            },
            Self::I64I16(enc) => match enc.max(row_ids) {
                Some(v) => Value::Scalar(Scalar::I64(v)),
                None => Value::Null,
            },
            Self::I64U16(enc) => match enc.max(row_ids) {
                Some(v) => Value::Scalar(Scalar::I64(v)),
                None => Value::Null,
            },
            Self::I64I8(enc) => match enc.max(row_ids) {
                Some(v) => Value::Scalar(Scalar::I64(v)),
                None => Value::Null,
            },
            Self::I64U8(enc) => match enc.max(row_ids) {
                Some(v) => Value::Scalar(Scalar::I64(v)),
                None => Value::Null,
            },

            Self::U64U64(enc) => match enc.max(row_ids) {
                Some(v) => Value::Scalar(Scalar::U64(v)),
                None => Value::Null,
            },
            Self::U64U32(enc) => match enc.max(row_ids) {
                Some(v) => Value::Scalar(Scalar::U64(v)),
                None => Value::Null,
            },
            Self::U64U16(enc) => match enc.max(row_ids) {
                Some(v) => Value::Scalar(Scalar::U64(v)),
                None => Value::Null,
            },
            Self::U64U8(enc) => match enc.max(row_ids) {
                Some(v) => Value::Scalar(Scalar::U64(v)),
                None => Value::Null,
            },
        }
    }

    pub fn sum(&self, row_ids: &[u32]) -> Scalar {
        match &self {
            Self::I64I64(enc) => match enc.sum(row_ids) {
                Some(v) => Scalar::I64(v),
                None => Scalar::Null,
            },
            Self::I64I32(enc) => match enc.sum(row_ids) {
                Some(v) => Scalar::I64(v),
                None => Scalar::Null,
            },
            Self::I64U32(enc) => match enc.sum(row_ids) {
                Some(v) => Scalar::I64(v),
                None => Scalar::Null,
            },
            Self::I64I16(enc) => match enc.sum(row_ids) {
                Some(v) => Scalar::I64(v),
                None => Scalar::Null,
            },
            Self::I64U16(enc) => match enc.sum(row_ids) {
                Some(v) => Scalar::I64(v),
                None => Scalar::Null,
            },
            Self::I64I8(enc) => match enc.sum(row_ids) {
                Some(v) => Scalar::I64(v),
                None => Scalar::Null,
            },
            Self::I64U8(enc) => match enc.sum(row_ids) {
                Some(v) => Scalar::I64(v),
                None => Scalar::Null,
            },

            Self::U64U64(enc) => match enc.sum(row_ids) {
                Some(v) => Scalar::U64(v),
                None => Scalar::Null,
            },
            Self::U64U32(enc) => match enc.sum(row_ids) {
                Some(v) => Scalar::U64(v),
                None => Scalar::Null,
            },
            Self::U64U16(enc) => match enc.sum(row_ids) {
                Some(v) => Scalar::U64(v),
                None => Scalar::Null,
            },
            Self::U64U8(enc) => match enc.sum(row_ids) {
                Some(v) => Scalar::U64(v),
                None => Scalar::Null,
            },
        }
    }

    pub fn count(&self, row_ids: &[u32]) -> u32 {
        match &self {
            Self::I64I64(enc) => enc.count(row_ids),
            Self::I64I32(enc) => enc.count(row_ids),
            Self::I64U32(enc) => enc.count(row_ids),
            Self::I64I16(enc) => enc.count(row_ids),
            Self::I64U16(enc) => enc.count(row_ids),
            Self::I64I8(enc) => enc.count(row_ids),
            Self::I64U8(enc) => enc.count(row_ids),
            Self::U64U64(enc) => enc.count(row_ids),
            Self::U64U32(enc) => enc.count(row_ids),
            Self::U64U16(enc) => enc.count(row_ids),
            Self::U64U8(enc) => enc.count(row_ids),
        }
    }

    /// The name of this encoding.
    //
    // TODO(edd): This is insane. Need to figure out why Open Telemetry needs
    // static strings for metric labels....
    //
    pub fn name(&self) -> &'static str {
        match &self {
            Self::I64I64(enc) => match enc {
                ScalarEncoding::Fixed(_) => "NONE_VEC",
                ScalarEncoding::FixedNullable(_) => "NONE_VECN",
                ScalarEncoding::RLE(_) => "NONE_RLE",
            },
            Self::I64I32(enc) => match enc {
                ScalarEncoding::Fixed(_) => "BT_I32_VEC",
                ScalarEncoding::FixedNullable(_) => "BT_I32_VECN",
                ScalarEncoding::RLE(_) => "BT_I32_RLE",
            },
            Self::I64U32(enc) => match enc {
                ScalarEncoding::Fixed(_) => "BT_U32_VEC",
                ScalarEncoding::FixedNullable(_) => "BT_U32_VECN",
                ScalarEncoding::RLE(_) => "BT_U32_RLE",
            },
            Self::I64I16(enc) => match enc {
                ScalarEncoding::Fixed(_) => "BT_I16_VEC",
                ScalarEncoding::FixedNullable(_) => "BT_I16_VECN",
                ScalarEncoding::RLE(_) => "BT_I16_RLE",
            },
            Self::I64U16(enc) => match enc {
                ScalarEncoding::Fixed(_) => "BT_U16_VEC",
                ScalarEncoding::FixedNullable(_) => "BT_U16_VECN",
                ScalarEncoding::RLE(_) => "BT_U16_RLE",
            },
            Self::I64I8(enc) => match enc {
                ScalarEncoding::Fixed(_) => "BT_I8_VEC",
                ScalarEncoding::FixedNullable(_) => "BT_I8_VECN",
                ScalarEncoding::RLE(_) => "BT_I8_RLE",
            },
            Self::I64U8(enc) => match enc {
                ScalarEncoding::Fixed(_) => "BT_U8_VEC",
                ScalarEncoding::FixedNullable(_) => "BT_U8_VECN",
                ScalarEncoding::RLE(_) => "BT_U8_RLE",
            },
            Self::U64U64(enc) => match enc {
                ScalarEncoding::Fixed(_) => "NONE_VEC",
                ScalarEncoding::FixedNullable(_) => "NONE_VECN",
                ScalarEncoding::RLE(_) => "NONE_RLE",
            },
            Self::U64U32(enc) => match enc {
                ScalarEncoding::Fixed(_) => "BT_U32_VEC",
                ScalarEncoding::FixedNullable(_) => "BT_U32_VECN",
                ScalarEncoding::RLE(_) => "BT_U32_RLE",
            },
            Self::U64U16(enc) => match enc {
                ScalarEncoding::Fixed(_) => "BT_U16_VEC",
                ScalarEncoding::FixedNullable(_) => "BT_U16_VECN",
                ScalarEncoding::RLE(_) => "BT_U16_RLE",
            },
            Self::U64U8(enc) => match enc {
                ScalarEncoding::Fixed(_) => "BT_I8_VEC",
                ScalarEncoding::FixedNullable(_) => "BT_I8_VECN",
                ScalarEncoding::RLE(_) => "BT_I8_RLE",
            },
        }
    }

    /// The logical datatype of this encoding.
    pub fn logical_datatype(&self) -> &'static str {
        match &self {
            Self::I64I64(_) => "i64",
            Self::I64I32(_) => "i64",
            Self::I64U32(_) => "i64",
            Self::I64I16(_) => "i64",
            Self::I64U16(_) => "i64",
            Self::I64I8(_) => "i64",
            Self::I64U8(_) => "i64",
            Self::U64U64(_) => "u64",
            Self::U64U32(_) => "u64",
            Self::U64U16(_) => "u64",
            Self::U64U8(_) => "u64",
        }
    }
}

impl std::fmt::Display for IntegerEncoding {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let name = self.name();
        match self {
            Self::I64I64(enc) => write!(f, "[{}]: {}", name, enc),
            Self::I64I32(enc) => write!(f, "[{}]: {}", name, enc),
            Self::I64U32(enc) => write!(f, "[{}]: {}", name, enc),
            Self::I64I16(enc) => write!(f, "[{}]: {}", name, enc),
            Self::I64U16(enc) => write!(f, "[{}]: {}", name, enc),
            Self::I64I8(enc) => write!(f, "[{}]: {}", name, enc),
            Self::I64U8(enc) => write!(f, "[{}]: {}", name, enc),
            Self::U64U64(enc) => write!(f, "[{}]: {}", name, enc),
            Self::U64U32(enc) => write!(f, "[{}]: {}", name, enc),
            Self::U64U16(enc) => write!(f, "[{}]: {}", name, enc),
            Self::U64U8(enc) => write!(f, "[{}]: {}", name, enc),
        }
    }
}

impl std::fmt::Debug for IntegerEncoding {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let name = self.name();
        match self {
            Self::I64I64(enc) => write!(f, "[{}]: {:?}", name, enc),
            Self::I64I32(enc) => write!(f, "[{}]: {:?}", name, enc),
            Self::I64U32(enc) => write!(f, "[{}]: {:?}", name, enc),
            Self::I64I16(enc) => write!(f, "[{}]: {:?}", name, enc),
            Self::I64U16(enc) => write!(f, "[{}]: {:?}", name, enc),
            Self::I64I8(enc) => write!(f, "[{}]: {:?}", name, enc),
            Self::I64U8(enc) => write!(f, "[{}]: {:?}", name, enc),
            Self::U64U64(enc) => write!(f, "[{}]: {:?}", name, enc),
            Self::U64U32(enc) => write!(f, "[{}]: {:?}", name, enc),
            Self::U64U16(enc) => write!(f, "[{}]: {:?}", name, enc),
            Self::U64U8(enc) => write!(f, "[{}]: {:?}", name, enc),
        }
    }
}

/// Converts a slice of i64 values into an IntegerEncoding.
///
/// The most compact physical type needed to store the columnar values is
/// determined, and a `Fixed` encoding is used for storage.
///
/// Panics if the provided slice is empty.
impl From<&[i64]> for IntegerEncoding {
    fn from(arr: &[i64]) -> Self {
        // determine min and max values.
        let mut min = arr[0];
        let mut max = arr[0];
        for &v in arr.iter().skip(1) {
            min = min.min(v);
            max = max.max(v);
        }

        // This match is carefully ordered. It prioritises smaller physical
        // datatypes that can safely represent the provided logical data
        match (min, max) {
            // encode as u8 values
            (min, max) if min >= 0 && max <= u8::MAX as i64 => {
                Self::I64U8(ScalarEncoding::from(i64_to_u8(arr)))
            }
            // encode as i8 values
            (min, max) if min >= i8::MIN as i64 && max <= i8::MAX as i64 => {
                Self::I64I8(ScalarEncoding::from(i64_to_i8(arr)))
            }
            // encode as u16 values
            (min, max) if min >= 0 && max <= u16::MAX as i64 => {
                Self::I64U16(ScalarEncoding::from(i64_to_u16(arr)))
            }
            // encode as i16 values
            (min, max) if min >= i16::MIN as i64 && max <= i16::MAX as i64 => {
                Self::I64I16(ScalarEncoding::from(i64_to_i16(arr)))
            }
            // encode as u32 values
            (min, max) if min >= 0 && max <= u32::MAX as i64 => {
                Self::I64U32(ScalarEncoding::from(i64_to_u32(arr)))
            }
            // encode as i32 values
            (min, max) if min >= i32::MIN as i64 && max <= i32::MAX as i64 => {
                Self::I64I32(ScalarEncoding::from(i64_to_i32(arr)))
            }
            // otherwise, encode with the same physical type (i64)
            (_, _) => Self::I64I64(ScalarEncoding::from(arr)),
        }
    }
}

macro_rules! byte_trim_types {
    ($(($name:ident, $from:ty, $to:ty),)*) => {
        $(
            fn $name(arr: &[$from]) -> Vec<$to> {
                arr.iter().map(|v| *v as $to).collect::<Vec<_>>()
            }
        )*
    };
}

byte_trim_types! {
    (i64_to_u32, i64, u32),
    (i64_to_i32, i64, i32),
    (i64_to_u16, i64, u16),
    (i64_to_i16, i64, i16),
    (i64_to_u8, i64, u8),
    (i64_to_i8, i64, i8),
    (u64_to_u32, u64, u32),
    (u64_to_u16, u64, u16),
    (u64_to_u8, u64, u8),
}

/// Converts an Arrow array into an IntegerEncoding.
///
/// The most compact physical Arrow array type is used to store the column
/// within a `FixedNull` encoding.
impl From<arrow::array::Int64Array> for IntegerEncoding {
    fn from(arr: arrow::array::Int64Array) -> Self {
        if arr.null_count() == 0 {
            return Self::from(arr.values());
        }

        // determine min and max values.
        let min = arrow::compute::kernels::aggregate::min(&arr);
        let max = arrow::compute::kernels::aggregate::max(&arr);

        // This match is carefully ordered. It prioritises smaller physical
        // datatypes that can safely represent the provided logical data
        match (min, max) {
            // encode as u8 values
            (min, max) if min >= Some(0) && max <= Some(u8::MAX as i64) => {
                let arr = PrimitiveArray::from_iter(arr.iter().map(|v| v.map(|v| v as u8)));
                Self::I64U8(ScalarEncoding::from(arr))
            }
            // encode as i8 values
            (min, max) if min >= Some(i8::MIN as i64) && max <= Some(i8::MAX as i64) => {
                let arr = PrimitiveArray::from_iter(arr.iter().map(|v| v.map(|v| v as i8)));
                Self::I64I8(ScalarEncoding::from(arr))
            }
            // encode as u16 values
            (min, max) if min >= Some(0) && max <= Some(u16::MAX as i64) => {
                let arr = PrimitiveArray::from_iter(arr.iter().map(|v| v.map(|v| v as u16)));
                Self::I64U16(ScalarEncoding::from(arr))
            }
            // encode as i16 values
            (min, max) if min >= Some(i16::MIN as i64) && max <= Some(i16::MAX as i64) => {
                let arr = PrimitiveArray::from_iter(arr.iter().map(|v| v.map(|v| v as i16)));
                Self::I64I16(ScalarEncoding::from(arr))
            }
            // encode as u32 values
            (min, max) if min >= Some(0) && max <= Some(u32::MAX as i64) => {
                let arr = PrimitiveArray::from_iter(arr.iter().map(|v| v.map(|v| v as u32)));
                Self::I64U32(ScalarEncoding::from(arr))
            }
            // encode as i32 values
            (min, max) if min >= Some(i32::MIN as i64) && max <= Some(i32::MAX as i64) => {
                let arr = PrimitiveArray::from_iter(arr.iter().map(|v| v.map(|v| v as i32)));
                Self::I64I32(ScalarEncoding::from(arr))
            }
            // otherwise, encode with the same physical type (i64)
            (_, _) => Self::I64I64(ScalarEncoding::from(arr)),
        }
    }
}

/// Converts a slice of u64 values into an IntegerEncoding.
///
/// The most compact physical type needed to store the columnar values is
/// determined, and a `Fixed` encoding is used for storage.
///
/// Panics if the provided slice is empty.
impl From<&[u64]> for IntegerEncoding {
    fn from(arr: &[u64]) -> Self {
        // determine min and max values.
        let mut min = arr[0];
        let mut max = arr[0];
        for &v in arr.iter().skip(1) {
            min = min.min(v);
            max = max.max(v);
        }

        // This match is carefully ordered. It prioritises smaller physical
        // datatypes that can safely represent the provided logical data
        match (min, max) {
            // encode as u8 values
            (_, max) if max <= u8::MAX as u64 => {
                let arr = arr.iter().map(|&v| v as u8).collect::<Vec<_>>();
                Self::U64U8(ScalarEncoding::from(arr))
            }
            // encode as u16 values
            (_, max) if max <= u16::MAX as u64 => {
                let arr = arr.iter().map(|&v| v as u16).collect::<Vec<_>>();
                Self::U64U16(ScalarEncoding::from(arr))
            }
            // encode as u32 values
            (_, max) if max <= u32::MAX as u64 => {
                let arr = arr.iter().map(|&v| v as u32).collect::<Vec<_>>();
                Self::U64U32(ScalarEncoding::from(arr))
            }
            // otherwise, encode with the same physical type (u64)
            (_, _) => Self::U64U64(ScalarEncoding::from(arr)),
        }
    }
}

/// Converts an Arrow array into an IntegerEncoding.
///
/// The most compact physical Arrow array type is used to store the column
/// within a `FixedNull` encoding.
impl From<arrow::array::UInt64Array> for IntegerEncoding {
    fn from(arr: arrow::array::UInt64Array) -> Self {
        if arr.null_count() == 0 {
            return Self::from(arr.values());
        }

        // determine max value.
        let max = arrow::compute::kernels::aggregate::max(&arr);

        // This match is carefully ordered. It prioritises smaller physical
        // datatypes that can safely represent the provided logical data
        match max {
            // encode as u8 values
            max if max <= Some(u8::MAX as u64) => {
                let arr = PrimitiveArray::from_iter(arr.iter().map(|v| v.map(|v| v as u8)));
                Self::U64U8(ScalarEncoding::from(arr))
            }
            // encode as u16 values
            max if max <= Some(u16::MAX as u64) => {
                let arr = PrimitiveArray::from_iter(arr.iter().map(|v| v.map(|v| v as u16)));
                Self::U64U16(ScalarEncoding::from(arr))
            }
            // encode as u32 values
            max if max <= Some(u32::MAX as u64) => {
                let arr = PrimitiveArray::from_iter(arr.iter().map(|v| v.map(|v| v as u32)));
                Self::U64U32(ScalarEncoding::from(arr))
            }
            // otherwise, encode with the same physical type (u64)
            _ => Self::U64U64(ScalarEncoding::from(arr)),
        }
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::column::encoding::scalar::Fixed;
    use arrow::array::*;

    #[test]
    fn from_slice_i64() {
        assert!(matches!(
            IntegerEncoding::from(vec![0_i64].as_slice()),
            IntegerEncoding::I64U8(ScalarEncoding::Fixed(Fixed::<u8> { .. }))
        ));
        assert!(matches!(
            IntegerEncoding::from(vec![0_i64, -120].as_slice()),
            IntegerEncoding::I64I8(ScalarEncoding::Fixed(Fixed::<i8> { .. }))
        ));
        assert!(matches!(
            IntegerEncoding::from(vec![399, 0_i64].as_slice()),
            IntegerEncoding::I64U16(ScalarEncoding::Fixed(Fixed::<u16> { .. }))
        ));
        assert!(matches!(
            IntegerEncoding::from(vec![0_i64, -400].as_slice()),
            IntegerEncoding::I64I16(ScalarEncoding::Fixed(Fixed::<i16> { .. }))
        ));
        assert!(matches!(
            IntegerEncoding::from(vec![0_i64, u32::MAX as i64].as_slice()),
            IntegerEncoding::I64U32(ScalarEncoding::Fixed(Fixed::<u32> { .. }))
        ));
        assert!(matches!(
            IntegerEncoding::from(vec![0_i64, i32::MIN as i64].as_slice()),
            IntegerEncoding::I64I32(ScalarEncoding::Fixed(Fixed::<i32> { .. }))
        ));
        assert!(matches!(
            IntegerEncoding::from(vec![0_i64, u32::MAX as i64 + 1].as_slice()),
            IntegerEncoding::I64I64(ScalarEncoding::Fixed(Fixed::<i64> { .. }))
        ));
    }

    #[test]
    fn from_slice_u64() {
        assert!(matches!(
            IntegerEncoding::from(vec![0_u64].as_slice()),
            IntegerEncoding::U64U8(ScalarEncoding::Fixed(Fixed::<u8> { .. }))
        ));
        assert!(matches!(
            IntegerEncoding::from(vec![399, 0_u64].as_slice()),
            IntegerEncoding::U64U16(ScalarEncoding::Fixed(Fixed::<u16> { .. }))
        ));
        assert!(matches!(
            IntegerEncoding::from(vec![0_u64, u32::MAX as u64].as_slice()),
            IntegerEncoding::U64U32(ScalarEncoding::Fixed(Fixed::<u32> { .. }))
        ));
        assert!(matches!(
            IntegerEncoding::from(vec![0_u64, u32::MAX as u64 + 1].as_slice()),
            IntegerEncoding::U64U64(ScalarEncoding::Fixed(Fixed::<u64> { .. }))
        ));
    }

    #[test]
    fn from_arrow_i64_array() {
        assert!(matches!(
            IntegerEncoding::from(Int64Array::from(vec![0])),
            IntegerEncoding::I64U8(ScalarEncoding::Fixed(Fixed::<u8> { .. }))
        ));
        assert!(matches!(
            IntegerEncoding::from(Int64Array::from(vec![Some(0)])),
            IntegerEncoding::I64U8(ScalarEncoding::Fixed(Fixed::<u8> { .. }))
        ));

        assert!(matches!(
            IntegerEncoding::from(Int64Array::from(vec![0, -120])),
            IntegerEncoding::I64I8(ScalarEncoding::Fixed(Fixed::<i8> { .. }))
        ));
        assert!(matches!(
            IntegerEncoding::from(Int64Array::from(vec![399])),
            IntegerEncoding::I64U16(ScalarEncoding::Fixed(Fixed::<u16> { .. }))
        ));
        assert!(matches!(
            IntegerEncoding::from(Int64Array::from(vec![-399, 2])),
            IntegerEncoding::I64I16(ScalarEncoding::Fixed(Fixed::<i16> { .. }))
        ));
        assert!(matches!(
            IntegerEncoding::from(Int64Array::from(vec![u32::MAX as i64, 2])),
            IntegerEncoding::I64U32(ScalarEncoding::Fixed(Fixed::<u32> { .. }))
        ));
        assert!(matches!(
            IntegerEncoding::from(Int64Array::from(vec![i32::MIN as i64, 2])),
            IntegerEncoding::I64I32(ScalarEncoding::Fixed(Fixed::<i32> { .. }))
        ));
        assert!(matches!(
            IntegerEncoding::from(Int64Array::from(vec![0, u32::MAX as i64 + 1])),
            IntegerEncoding::I64I64(ScalarEncoding::Fixed(Fixed::<i64> { .. }))
        ));
    }

    #[test]
    fn from_arrow_u64_array() {
        assert!(matches!(
            IntegerEncoding::from(UInt64Array::from(vec![0])),
            IntegerEncoding::U64U8(ScalarEncoding::Fixed(Fixed::<u8> { .. }))
        ));
        assert!(matches!(
            IntegerEncoding::from(UInt64Array::from(vec![Some(0)])),
            IntegerEncoding::U64U8(ScalarEncoding::Fixed(Fixed::<u8> { .. }))
        ));

        assert!(matches!(
            IntegerEncoding::from(UInt64Array::from(vec![0, 2452])),
            IntegerEncoding::U64U16(ScalarEncoding::Fixed(Fixed::<u16> { .. }))
        ));
        assert!(matches!(
            IntegerEncoding::from(UInt64Array::from(vec![0, u32::MAX as u64])),
            IntegerEncoding::U64U32(ScalarEncoding::Fixed(Fixed::<u32> { .. }))
        ));
        assert!(matches!(
            IntegerEncoding::from(UInt64Array::from(vec![0, u32::MAX as u64 + 1])),
            IntegerEncoding::U64U64(ScalarEncoding::Fixed(Fixed::<u64> { .. }))
        ));
    }
}
