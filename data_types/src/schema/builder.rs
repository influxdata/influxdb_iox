use snafu::{OptionExt, ResultExt, Snafu};
use std::{
    collections::{HashMap, HashSet},
    convert::TryInto,
};
use tracing::warn;

use arrow_deps::arrow::datatypes::{DataType as ArrowDataType, Field as ArrowField};

use super::{LPColumnType, LPFieldType, Schema, TIME_COLUMN_NAME};

/// Database schema creation / validation errors.
#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("No measurement provided",))]
    NoMeasurement {},

    #[snafu(display(
        "Multiple measurement names not supported. Old measurement '{}', new measurement '{}'",
        old_measurement,
        new_measurement
    ))]
    MultipleMeasurementNames {
        old_measurement: String,
        new_measurement: String,
    },

    #[snafu(display("Error validating schema: {}", source))]
    ValidatingSchema {
        source: Box<dyn std::error::Error + 'static + Send + Sync>,
    },
}

pub type Result<T, E = Error> = std::result::Result<T, E>;

/// Builder for a Schema
#[derive(Debug, Default)]
pub struct SchemaBuilder {
    /// Optional measurement name
    measurement: Option<String>,

    /// The fields, in order
    fields: Vec<ArrowField>,

    /// which columns represent tags
    tag_cols: HashSet<String>,

    /// which columns represent fields
    field_cols: HashMap<String, LPColumnType>,

    /// which field was the time column, if any
    time_col: Option<String>,
}

impl SchemaBuilder {
    pub fn new() -> Self {
        Self::default()
    }

    /// Add a new tag column to this schema. By default tags are
    /// potentially nullable as they are not guaranteed to be present
    /// for all rows
    pub fn tag(self, column_name: &str) -> Self {
        self.add_column(
            column_name,
            true,
            Some(LPColumnType::Tag),
            ArrowDataType::Utf8,
        )
    }

    /// Add a new tag column to this schema that is known (somehow) to
    /// have no nulls for all rows
    pub fn non_null_tag(self, column_name: &str) -> Self {
        self.add_column(
            column_name,
            false,
            Some(LPColumnType::Tag),
            ArrowDataType::Utf8,
        )
    }

    /// Add a new field column with the specified line protocol type
    pub fn lp_field(self, column_name: &str, lp_field_type: LPFieldType) -> Self {
        let arrow_type: ArrowDataType = lp_field_type.into();
        self.add_column(
            column_name,
            false,
            Some(LPColumnType::Field(lp_field_type)),
            arrow_type,
        )
    }

    /// Add a new nullable field column with the specified Arrow datatype.
    pub fn field(self, column_name: &str, arrow_type: ArrowDataType) -> Self {
        let lp_column_type = arrow_type.clone().try_into().map(LPColumnType::Field).ok();

        self.add_column(column_name, true, lp_column_type, arrow_type)
    }

    /// Add a new field column with the specified Arrow datatype that can not be
    /// null
    pub fn non_null_field(self, column_name: &str, arrow_type: ArrowDataType) -> Self {
        let lp_column_type = arrow_type.clone().try_into().map(LPColumnType::Field).ok();

        self.add_column(column_name, false, lp_column_type, arrow_type)
    }

    /// Add the timestamp column
    pub fn timestamp(self) -> Self {
        let lp_column_type = LPColumnType::Timestamp;
        let arrow_type = (&lp_column_type).into();
        self.add_column(TIME_COLUMN_NAME, false, Some(lp_column_type), arrow_type)
    }

    /// Set optional measurement name
    pub fn measurement(mut self, measurement_name: impl Into<String>) -> Self {
        self.measurement = Some(measurement_name.into());
        self
    }

    /// Creates an Arrow schema with embedded metadata, consuming self. All
    /// schema validation happens at this time.

    /// ```
    /// use data_types::schema::{builder::SchemaBuilder, LPColumnType, LPFieldType};
    ///
    /// let schema = SchemaBuilder::new()
    ///   .tag("region")
    ///   .lp_field("counter", LPFieldType::Float)
    ///   .timestamp()
    ///   .build()
    ///   .unwrap();
    ///
    /// let (lp_column_type, arrow_field) = schema.field(0);
    /// assert_eq!(arrow_field.name(), "region");
    /// assert_eq!(lp_column_type, Some(LPColumnType::Tag));
    ///
    /// let (lp_column_type, arrow_field) = schema.field(1);
    /// assert_eq!(arrow_field.name(), "counter");
    /// assert_eq!(lp_column_type, Some(LPColumnType::Field(LPFieldType::Float)));
    ///
    /// let (lp_column_type, arrow_field) = schema.field(2);
    /// assert_eq!(arrow_field.name(), "time");
    /// assert_eq!(lp_column_type, Some(LPColumnType::Timestamp));
    /// ```
    pub fn build(self) -> Result<Schema> {
        let Self {
            measurement,
            fields,
            tag_cols,
            field_cols,
            time_col,
        } = self;

        Schema::new_from_parts(measurement, fields, tag_cols, field_cols, time_col)
            .map_err(|e| Box::new(e) as _)
            .context(ValidatingSchema)
    }

    /// Internal helper method to add a column definition
    fn add_column(
        mut self,
        column_name: &str,
        nullable: bool,
        lp_column_type: Option<LPColumnType>,
        arrow_type: ArrowDataType,
    ) -> Self {
        self.fields
            .push(ArrowField::new(column_name, arrow_type, nullable));

        match &lp_column_type {
            Some(LPColumnType::Tag) => {
                self.tag_cols.insert(column_name.to_string());
            }
            Some(LPColumnType::Field(_)) => {
                self.field_cols
                    .insert(column_name.to_string(), lp_column_type.unwrap());
            }
            Some(LPColumnType::Timestamp) => {
                self.time_col = Some(column_name.to_string());
            }
            None => {}
        }
        self
    }
}

/// Specialized Schema Builder for use building up a Schema while
/// streaming line protocol through it.
///
/// Duplicated tag and field definitions are ignored, and the
/// resulting schema always has puts tags as the initial columns (in the
/// order of appearance) followed by fields (in order of appearance)
/// and then timestamp
#[derive(Debug, Default)]
pub struct LPSchemaBuilder {
    /// What tag names we have seen so far
    tag_set: HashSet<String>,
    /// What field names we have seen so far
    field_set: HashMap<String, LPFieldType>,

    /// Keep track of the tag_columns in order they were added
    tag_list: Vec<String>,
    /// Track the fields in order they were added
    field_list: Vec<String>,

    /// Keep The measurement name, if seen
    measurement: Option<String>,
}

impl LPSchemaBuilder {
    pub fn new() -> Self {
        Self::default()
    }

    /// Set optional measurement name, erroring if previously specified
    pub fn saw_measurement(mut self, measurement: impl Into<String>) -> Result<Self> {
        let new_measurement = measurement.into();

        if let Some(old_measurement) = &self.measurement {
            if old_measurement != &new_measurement {
                return MultipleMeasurementNames {
                    new_measurement,
                    old_measurement,
                }
                .fail();
            }
        } else {
            self.measurement = Some(new_measurement)
        }
        Ok(self)
    }

    /// Add a new tag column to this schema, ignoring if the tag has already
    /// been seen
    pub fn saw_tag(mut self, column_name: &str) -> Self {
        if !self.tag_set.contains(column_name) {
            self.tag_set.insert(column_name.to_string());
            self.tag_list.push(column_name.to_string());
        };

        self
    }

    /// Add a new field column with the specified line protocol type, ignoring
    /// if that field has been seen TODO error if the field is a different
    /// type (old implementation produces warn! in this condition)
    pub fn saw_lp_field(mut self, column_name: &str, lp_field_type: LPFieldType) -> Self {
        if let Some(existing_lp_field_type) = self.field_set.get(column_name) {
            if &lp_field_type != existing_lp_field_type {
                warn!("Ignoring new type for field '{}': Previously it had type {:?}, attempted to set type {:?}.",
                      column_name, existing_lp_field_type, lp_field_type);
            }
        } else {
            self.field_set
                .insert(column_name.to_string(), lp_field_type);
            self.field_list.push(column_name.to_string())
        }
        self
    }

    /// Build a schema object from the collected schema
    pub fn build(self) -> Result<Schema> {
        let builder =
            SchemaBuilder::new().measurement(self.measurement.as_ref().context(NoMeasurement)?);

        // tags always first
        let builder = self
            .tag_list
            .iter()
            .fold(builder, |builder, tag_name| builder.tag(tag_name));

        // then fields (in order they were added)
        let builder = self.field_list.iter().fold(builder, |builder, field_name| {
            let lp_field_type = self.field_set.get(field_name).unwrap();
            builder.lp_field(field_name, *lp_field_type)
        });

        // and now timestamp
        builder.timestamp().build()
    }
}

#[cfg(test)]
mod test {
    use crate::assert_column_eq;

    use super::*;

    #[test]
    fn test_builder_basic() {
        let s = SchemaBuilder::new()
            .lp_field("str_field", LPFieldType::String)
            .tag("the_tag")
            .lp_field("int_field", LPFieldType::Integer)
            .lp_field("bool_field", LPFieldType::Boolean)
            .lp_field("float_field", LPFieldType::Float)
            .tag("the_second_tag")
            .timestamp()
            .measurement("the_measurement")
            .build()
            .unwrap();

        assert_column_eq!(s, 0, LPColumnType::Field(LPFieldType::String), "str_field");
        assert_column_eq!(s, 1, LPColumnType::Tag, "the_tag");
        assert_column_eq!(s, 2, LPColumnType::Field(LPFieldType::Integer), "int_field");
        assert_column_eq!(
            s,
            3,
            LPColumnType::Field(LPFieldType::Boolean),
            "bool_field"
        );
        assert_column_eq!(s, 4, LPColumnType::Field(LPFieldType::Float), "float_field");
        assert_column_eq!(s, 5, LPColumnType::Tag, "the_second_tag");
        assert_column_eq!(s, 6, LPColumnType::Timestamp, "time");

        assert_eq!(s.measurement().unwrap(), "the_measurement");
        assert_eq!(s.len(), 7);
    }

    #[test]
    fn test_builder_tag() {
        let s = SchemaBuilder::new()
            .tag("the_tag")
            .non_null_tag("the_non_null_tag")
            .build()
            .unwrap();

        let (lp_column_type, field) = s.field(0);
        assert_eq!(field.name(), "the_tag");
        assert_eq!(field.data_type(), &ArrowDataType::Utf8);
        assert_eq!(field.is_nullable(), true);
        assert_eq!(lp_column_type, Some(LPColumnType::Tag));

        let (lp_column_type, field) = s.field(1);
        assert_eq!(field.name(), "the_non_null_tag");
        assert_eq!(field.data_type(), &ArrowDataType::Utf8);
        assert_eq!(field.is_nullable(), false);
        assert_eq!(lp_column_type, Some(LPColumnType::Tag));

        assert_eq!(s.len(), 2);
    }

    #[test]
    fn test_builder_field() {
        let s = SchemaBuilder::new()
            .field("the_lp_field", ArrowDataType::Float64)
            // can't represent with lp
            .field("the_no_lp_field", ArrowDataType::Decimal(10, 0))
            .build()
            .unwrap();

        let (lp_column_type, field) = s.field(0);
        assert_eq!(field.name(), "the_lp_field");
        assert_eq!(field.data_type(), &ArrowDataType::Float64);
        assert_eq!(field.is_nullable(), true);
        assert_eq!(
            lp_column_type,
            Some(LPColumnType::Field(LPFieldType::Float))
        );

        let (lp_column_type, field) = s.field(1);
        assert_eq!(field.name(), "the_no_lp_field");
        assert_eq!(field.data_type(), &ArrowDataType::Decimal(10, 0));
        assert_eq!(field.is_nullable(), true);
        assert_eq!(lp_column_type, None);

        assert_eq!(s.len(), 2);
    }

    #[test]
    fn test_builder_non_field() {
        let s = SchemaBuilder::new()
            .non_null_field("the_lp_field", ArrowDataType::Float64)
            // can't represent with lp
            .non_null_field("the_no_lp_field", ArrowDataType::Decimal(10, 0))
            .build()
            .unwrap();

        let (lp_column_type, field) = s.field(0);
        assert_eq!(field.name(), "the_lp_field");
        assert_eq!(field.data_type(), &ArrowDataType::Float64);
        assert_eq!(field.is_nullable(), false);
        assert_eq!(
            lp_column_type,
            Some(LPColumnType::Field(LPFieldType::Float))
        );

        let (lp_column_type, field) = s.field(1);
        assert_eq!(field.name(), "the_no_lp_field");
        assert_eq!(field.data_type(), &ArrowDataType::Decimal(10, 0));
        assert_eq!(field.is_nullable(), false);
        assert_eq!(lp_column_type, None);

        assert_eq!(s.len(), 2);
    }

    #[test]
    fn test_builder_no_measurement() {
        let s = SchemaBuilder::new().tag("the tag").build().unwrap();

        assert_eq!(s.measurement(), None);
    }

    #[test]
    fn test_builder_dupe_tag() {
        let res = SchemaBuilder::new().tag("the tag").tag("the tag").build();

        assert_eq!(
            res.unwrap_err().to_string(),
            "Error validating schema: Error: Duplicate column name found in schema: 'the tag'"
        );
    }

    #[test]
    fn test_builder_dupe_field_and_tag() {
        let res = SchemaBuilder::new()
            .tag("the name")
            .lp_field("the name", LPFieldType::Integer)
            .build();

        assert_eq!(res.unwrap_err().to_string(), "Error validating schema: Error validating schema: 'the name' is both a field and a tag");
    }

    #[test]
    fn test_builder_dupe_field_and_timestamp() {
        let res = SchemaBuilder::new().tag("time").timestamp().build();

        assert_eq!(res.unwrap_err().to_string(), "Error validating schema: Duplicate column name: 'time' was specified to be Tag as well as timestamp");
    }

    #[test]
    fn test_lp_builder_basic() {
        let s = LPSchemaBuilder::new()
            .saw_lp_field("the_field", LPFieldType::Float)
            .saw_tag("the_tag")
            .saw_tag("the_tag")
            .saw_lp_field("the_field", LPFieldType::Float)
            .saw_measurement("the_measurement")
            .unwrap()
            .saw_tag("the_tag")
            .saw_tag("the_second_tag")
            .saw_measurement("the_measurement")
            .unwrap()
            .build()
            .unwrap();

        assert_column_eq!(s, 0, LPColumnType::Tag, "the_tag");
        assert_column_eq!(s, 1, LPColumnType::Tag, "the_second_tag");
        assert_column_eq!(s, 2, LPColumnType::Field(LPFieldType::Float), "the_field");
        assert_column_eq!(s, 3, LPColumnType::Timestamp, "time");

        assert_eq!(s.measurement().unwrap(), "the_measurement");
        assert_eq!(s.len(), 4);
    }

    #[test]
    fn test_lp_builder_no_measurement() {
        let res = LPSchemaBuilder::new()
            .saw_tag("the_tag")
            .saw_lp_field("the_field", LPFieldType::Float)
            .build();

        assert_eq!(res.unwrap_err().to_string(), "No measurement provided");
    }

    #[test]
    fn test_lp_builder_different_measurement() {
        let res = LPSchemaBuilder::new()
            .saw_measurement("m1")
            .unwrap()
            .saw_measurement("m2");

        assert_eq!(res.unwrap_err().to_string(), "Multiple measurement names not supported. Old measurement \'m1\', new measurement \'m2\'");
    }

    #[test]
    fn test_lp_changed_field_type() {
        let s = LPSchemaBuilder::new()
            .saw_measurement("the_measurement")
            .unwrap()
            .saw_lp_field("the_field", LPFieldType::Float)
            // same field name seen again as a different type
            .saw_lp_field("the_field", LPFieldType::Integer)
            .build()
            .unwrap();

        assert_column_eq!(s, 0, LPColumnType::Field(LPFieldType::Float), "the_field");
        assert_column_eq!(s, 1, LPColumnType::Timestamp, "time");

        assert_eq!(s.len(), 2);
    }
}
