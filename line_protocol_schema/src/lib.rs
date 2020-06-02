/// This module is used to represent the abstract "schema" of a set of line
/// protocol data records, as defined in the
/// [documentation](https://docs.influxdata.com/influxdb/v1.8/write_protocols/line_protocol_tutorial)
///
/// The line protocol format has an inherently "flexible" schema
/// (e.g. the tags and fields for a measurement can and do change over
/// time), the schema only makes sense for a given set of rows (not
/// all possible rows in that measurement).
///
/// The line protocol schema consists of a series of columns, each with a
/// specific type, indexed by 0.
///
///```
/// use line_protocol_schema::{SchemaBuilder, DataType, ColumnDefinition};
/// let schema = SchemaBuilder::new(String::from("my_measurement"))
///     .tag("tag1")
///     .field("field1", DataType::Float)
///     .field("field2", DataType::Boolean)
///     .tag("tag2")
///     .build();
///
/// let cols = schema.get_col_defs();
/// assert_eq!(cols.len(), 5);
/// assert_eq!(cols[0], ColumnDefinition::new("tag1", 0, DataType::String));
/// assert_eq!(cols[1], ColumnDefinition::new("tag2", 1, DataType::String));
/// assert_eq!(cols[2], ColumnDefinition::new("field1", 2, DataType::Float));
/// assert_eq!(cols[3], ColumnDefinition::new("field2", 3, DataType::Boolean));
/// assert_eq!(cols[4], ColumnDefinition::new("timestamp", 4, DataType::Timestamp));
/// ```
use std::collections::HashMap;

/// Represents a specific Line Protocol Tag name
#[derive(Debug, PartialEq)]
pub struct Tag {
    pub tag_name: String,
    column_index: u32,
}

impl Tag {
    pub fn new(name: impl Into<String>, idx: u32) -> Tag {
        Tag {
            tag_name: name.into(),
            column_index: idx,
        }
    }
}

#[derive(Debug, PartialEq, Clone, Copy)]
/// Line Protocol Data Types from
/// https://docs.influxdata.com/influxdb/v1.8/write_protocols/line_protocol_tutorial/#data-types
pub enum DataType {
    /// 64-bit floating point number (TDB if NULLs / Nans are allowed)
    Float,
    /// 64-bit signed integer
    Integer,
    /// UTF-8 encoded string
    String,
    /// true or false
    Boolean,
    /// 64 bit timestamp "UNIX timestamps" representing nanosecods
    /// since the UNIX epoch (00:00:00 UTC on 1 January 1970).
    Timestamp,
}

/// Represents a specific Line Protocol Field name
#[derive(Debug, PartialEq)]
pub struct Field {
    pub field_name: String,
    pub field_type: DataType,
    column_index: u32,
}

impl Field {
    pub fn new(field_name: impl Into<String>, field_type: DataType, column_index: u32) -> Field {
        Field {
            field_name: field_name.into(),
            field_type,
            column_index,
        }
    }
}

#[derive(Debug, PartialEq)]
// Represents the timestamp value of a field of Line Protocol data
pub struct Timestamp {
    pub timestamp_name: String,
    column_index: u32,
}

impl Timestamp {
    fn new(name: impl Into<String>, idx: u32) -> Timestamp {
        Timestamp {
            timestamp_name: name.into(),
            column_index: idx,
        }
    }
}

/// Represents a column of the line protocol data (specifically how to
/// find tag values and field values in a set of columns)
#[derive(Debug, PartialEq)]
pub struct ColumnDefinition {
    pub column_name: String,
    pub column_index: u32,
    pub column_type: DataType,
}

impl ColumnDefinition {
    pub fn new(name: &str, idx: u32, t: DataType) -> ColumnDefinition {
        ColumnDefinition {
            column_name: name.to_string(),
            column_index: idx,
            column_type: t,
        }
    }
}

/// Represents the overall "schema" of a set of line protocol data
/// records as an ordered list of columns. It records possible tag names and
/// field names as well as in what order these columns appear
/// set of columns)
#[derive(Debug)]
pub struct Schema {
    measurement: String,
    tags: HashMap<String, Tag>,
    fields: HashMap<String, Field>,
    timestamp: Timestamp,
}

impl Schema {
    // Return an Vec of ColumnDefinition's such that
    // v[column_index].column_index == column_index for all columns
    // (aka that the vec is in the same order as the columns of the schema
    // TODO : consider pre-computing this on schema directly.
    pub fn get_col_defs(&self) -> Vec<ColumnDefinition> {
        let mut cols = Vec::new();
        cols.reserve(self.tags.len() + self.fields.len() + 1);
        for (tag_name, tag) in self.tags.iter() {
            cols.push(ColumnDefinition {
                column_name: tag_name.clone(),
                column_index: tag.column_index,
                column_type: DataType::String,
            });
        }
        for (field_name, field) in self.fields.iter() {
            cols.push(ColumnDefinition {
                column_name: field_name.clone(),
                column_index: field.column_index,
                column_type: field.field_type,
            });
        }
        cols.push(ColumnDefinition {
            column_name: self.timestamp.timestamp_name.clone(),
            column_index: self.timestamp.column_index,
            column_type: DataType::Timestamp,
        });

        cols.sort_by(|a, b| a.column_index.cmp(&b.column_index));
        cols
    }
}

/// Used to create new Schema objects
pub struct SchemaBuilder {
    measurement_name: String,
    tag_names: Vec<String>,
    field_defs: Vec<(String, DataType)>,
    timestamp_name: String,
}

impl SchemaBuilder {
    /// Begin building the schema for a named measurement
    pub fn new(measurement_name: String) -> SchemaBuilder {
        SchemaBuilder {
            measurement_name,
            tag_names: Vec::new(),
            field_defs: Vec::new(),
            timestamp_name: "timestamp".to_string(),
        }
    }

    /// Add a new tag name to the schema.
    pub fn tag(&mut self, tag_name: &str) -> &mut Self {
        // check for existing tag (TODO make this faster)
        if self.tag_names.iter().find(|&s| s == tag_name).is_none() {
            self.tag_names.push(tag_name.to_string());
        }
        self
    }

    /// Add a new typed field to the schema. Field names can not be repeated
    pub fn field(&mut self, field_name: &str, field_type: DataType) -> &mut Self {
        // check for existing fields (TODO make this faster)
        match self
            .field_defs
            .iter()
            .find(|(existing_name, _)| existing_name == field_name)
        {
            Some((_, existing_type)) => {
                if *existing_type != field_type {
                    panic!("Field '{}' type changed. Previously it had type {:?} but attempted to set type {:?}",
                           field_name, existing_type, field_type);
                }
            }
            None => {
                let new_field_def = (field_name.to_string(), field_type);
                self.field_defs.push(new_field_def);
            }
        }
        self
    }

    /// Create a new schema from a list of tag names and (field_name, type) pairs
    pub fn build(&mut self) -> Schema {
        // assign column indexes to all columns
        let mut indexer = IndexGenerator::new();

        Schema {
            measurement: self.measurement_name.to_string(),
            tags: self
                .tag_names
                .iter()
                .map(|name| (name.clone(), Tag::new(name.clone(), indexer.next())))
                .collect(),
            fields: self
                .field_defs
                .iter()
                .map(|(name, typ)| (name.clone(), Field::new(name.clone(), *typ, indexer.next())))
                .collect(),
            timestamp: Timestamp::new(self.timestamp_name.to_string(), indexer.next()),
        }
    }
}

struct IndexGenerator {
    val: u32,
}
impl IndexGenerator {
    fn new() -> IndexGenerator {
        IndexGenerator { val: 0 }
    }

    fn next(&mut self) -> u32 {
        let t = self.val;
        self.val += 1;
        t
    }
}

#[cfg(test)]
mod schema_test {
    use super::*;

    #[test]
    fn construct() {
        let mut builder = SchemaBuilder::new(String::from("my_measurement"));
        builder
            .tag("tag1")
            .field("field1", DataType::Float)
            .field("field2", DataType::Boolean)
            .tag("tag2");

        let schema = builder.build();

        assert_eq!(schema.measurement, "my_measurement");
        assert_eq!(schema.tags.len(), 2);
        assert_eq!(
            schema.tags.get("tag1").unwrap(),
            &Tag::new(String::from("tag1"), 0)
        );
        assert_eq!(
            schema.tags.get("tag2").unwrap(),
            &Tag::new(String::from("tag2"), 1)
        );
        assert_eq!(
            schema.fields.get("field1").unwrap(),
            &Field::new(String::from("field1"), DataType::Float, 2)
        );
        assert_eq!(
            schema.fields.get("field2").unwrap(),
            &Field::new(String::from("field2"), DataType::Boolean, 3)
        );
        assert_eq!(
            schema.timestamp,
            Timestamp::new(String::from("timestamp"), 4)
        );
    }

    #[test]
    fn duplicate_tag_names() {
        let schema = SchemaBuilder::new(String::from("my_measurement"))
            .tag("tag1")
            .tag("tag1")
            .build();

        let cols = schema.get_col_defs();
        assert_eq!(cols.len(), 2);
        assert_eq!(
            cols[0],
            ColumnDefinition::new("tag1", 0, DataType::String)
        );
        assert_eq!(
            cols[1],
            ColumnDefinition::new("timestamp", 1, DataType::Timestamp)
        );
    }

    #[test]
    fn duplicate_field_name_same_type() {
        let schema = SchemaBuilder::new(String::from("my_measurement"))
            .field("field1", DataType::Float)
            .field("field1", DataType::Float)
            .build();

        let cols = schema.get_col_defs();
        assert_eq!(cols.len(), 2);
        assert_eq!(
            cols[0],
            ColumnDefinition::new("field1", 0, DataType::Float)
        );
        assert_eq!(
            cols[1],
            ColumnDefinition::new("timestamp", 1, DataType::Timestamp)
        );
    }

    #[test]
    #[should_panic]
    fn duplicate_field_name_different_type() {
        SchemaBuilder::new(String::from("my_measurement"))
            .field("field1", DataType::Float)
            .field("field1", DataType::Integer)
            .build();
        // TBD better error handling -- what should happen if there is
        // a new type seen for an existing field?
    }

    #[test]
    fn get_col_defs() {
        let schema = SchemaBuilder::new(String::from("my_measurement"))
            .tag("tag1")
            .field("field1", DataType::Float)
            .field("field2", DataType::Boolean)
            .tag("tag2")
            .build();

        let cols = schema.get_col_defs();
        assert_eq!(cols.len(), 5);
        assert_eq!(
            cols[0],
            ColumnDefinition::new("tag1", 0, DataType::String)
        );
        assert_eq!(
            cols[1],
            ColumnDefinition::new("tag2", 1, DataType::String)
        );
        assert_eq!(
            cols[2],
            ColumnDefinition::new("field1", 2, DataType::Float)
        );
        assert_eq!(
            cols[3],
            ColumnDefinition::new("field2", 3, DataType::Boolean)
        );
        assert_eq!(
            cols[4],
            ColumnDefinition::new("timestamp", 4, DataType::Timestamp)
        );
    }

    #[test]
    fn get_col_defs_empty() {
        let schema = SchemaBuilder::new(String::from("my_measurement")).build();

        let cols = schema.get_col_defs();
        assert_eq!(cols.len(), 1);
        assert_eq!(
            cols[0],
            ColumnDefinition::new("timestamp", 0, DataType::Timestamp)
        );
    }

    #[test]
    fn get_col_defs_sort() {
        // Test that get_col_defs sorts its output
        let mut schema = SchemaBuilder::new(String::from("my_measurement"))
            .tag("tag1")
            .field("field1", DataType::Float)
            .build();

        let cols = schema.get_col_defs();
        assert_eq!(cols.len(), 3);
        assert_eq!(
            cols[0],
            ColumnDefinition::new("tag1", 0, DataType::String)
        );
        assert_eq!(
            cols[1],
            ColumnDefinition::new("field1", 1, DataType::Float)
        );
        assert_eq!(
            cols[2],
            ColumnDefinition::new("timestamp", 2, DataType::Timestamp)
        );

        // Now, if we somehow have changed how the indexes are
        // assigned, the columns should still appear in order
        schema.tags.get_mut("tag1").unwrap().column_index = 2;
        schema.timestamp.column_index = 0;

        let cols = schema.get_col_defs();
        assert_eq!(cols.len(), 3);
        assert_eq!(
            cols[0],
            ColumnDefinition::new("timestamp", 0, DataType::Timestamp)
        );
        assert_eq!(
            cols[1],
            ColumnDefinition::new("field1", 1, DataType::Float)
        );
        assert_eq!(
            cols[2],
            ColumnDefinition::new("tag1", 2, DataType::String)
        );
    }
}
