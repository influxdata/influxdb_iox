/// This module has structs that can represent the abstract "schema"
/// of line protocol as defined in the
/// [documentation](https://docs.influxdata.com/influxdb/v1.8/write_protocols/line_protocol_tutorial)
/// into a series of columns (indexed by 0)
///
use std::collections::HashMap;

#[derive(Debug)]
pub struct Tag {
    pub tag_name: String,
    pub column_index: u32,
}

impl Tag {
    pub fn new(name: &str, idx: u32) -> Tag {
        Tag {
            tag_name: name.to_string(),
            column_index: idx,
        }
    }
}

#[derive(Debug, PartialEq, Clone, Copy)]
pub enum LineProtocolType {
    // Line Protocol Data Types from
    // https://docs.influxdata.com/influxdb/v1.8/write_protocols/line_protocol_tutorial/#data-types
    LPFloat,
    LPInteger,
    LPString,
    //LPBoolean,
    LPTimestamp,
}

#[derive(Debug)]
pub struct Field {
    pub field_name: String,
    pub field_type: LineProtocolType,
    pub column_index: u32,
}

impl Field {
    pub fn new(name: &str, field_type: LineProtocolType, idx: u32) -> Field {
        Field {
            field_name: name.to_string(),
            field_type: field_type,
            column_index: idx,
        }
    }
}

#[derive(Debug)]
pub struct Timestamp {
    pub timestamp_name: String,
    pub column_index: u32,
}

/// Represents the "schema" of line protocol data (specifically how to
/// find tag values and field values in a set of columns)
#[derive(Debug)]
pub struct Schema {
    pub measurement: String,
    pub tags: HashMap<String, Tag>,
    pub fields: HashMap<String, Field>,
    pub timestamp: Timestamp,
    // TODO: add some way to represent tags and fields we haven't seen
    // before.
}

#[derive(Debug)]
pub struct ColumnDefinition {
    pub column_name: String,
    pub column_index: u32,
    pub column_type: LineProtocolType,
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
                column_type: LineProtocolType::LPString,
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
            column_type: LineProtocolType::LPTimestamp,
        });

        // TODO: test the sort works correctly
        cols.sort_by(|a, b| a.column_index.cmp(&b.column_index));

        cols
    }
}
