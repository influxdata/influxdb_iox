/// Code responsible for understanding `ParsedLine` data and deducing
///  line_prococol_schema::Schema and packing ParsedLines
use delorean_line_parser::{self, FieldValue, ParsedLine};
use std::collections::{HashMap, HashSet};

use crate::line_protocol_schema::{Field, LineProtocolType, Schema, Tag, Timestamp};
use crate::packers::Packer;

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

pub struct LineProtocolConverter {
    pub schema: Schema,
}

impl LineProtocolConverter {
    /// This module contains code to extract the observed schema out of a
    /// bunch of `ParsedLine` structures.
    pub fn new(parsed_lines: &Vec<ParsedLine>) -> LineProtocolConverter {
        LineProtocolConverter {
            schema: deduce_schema(parsed_lines),
        }
    }

    /// Packs a bunch of ParsedLines into a format suitable for
    /// writing out column by column.
    // Specifically, buffers holding *columns* of data, suitable to
    // pass to the encoder as the parquet writer requires you to write
    // and close one column at a time,
    pub fn pack_lines(&self, parsed_lines: &Vec<ParsedLine>) -> Vec<Packer> {
        // Need to build up columnar in-memory data for all tags, fields and timestamps
        // so we can write to parquet.
        let mut packers: Vec<Packer> = Vec::new();

        let col_defs = self.schema.get_col_defs();
        for (idx, col_def) in col_defs.iter().enumerate() {
            debug!("  Column definition [{}] = {:?}", idx, col_def);
            packers.push(Packer::new(col_def.column_type));
        }

        // map col_name -> Packer;
        let mut packer_map: HashMap<&String, &mut Packer> = col_defs
            .iter()
            .map(|x| &x.column_name)
            .zip(packers.iter_mut())
            .collect();

        // for each parsed input line
        // for each tag we expect to see, add an appropriate entry

        for line in parsed_lines.iter() {
            let timestamp_col_name = &self.schema.timestamp.timestamp_name;
            debug!("Packing line {:?}", line);

            // all packers should be the same size
            let starting_len = packer_map
                .get_mut(timestamp_col_name)
                .expect("should always have timestamp column")
                .len();
            assert!(
                packer_map.values().all(|x| x.len() == starting_len),
                "All packers should have started at the same size"
            );

            let series = &line.series;

            // TODO handle data from different measurements
            assert_eq!(
                series.measurement.to_string(),
                self.schema.measurement,
                "Different measurements in same line protocol stream not supported now"
            );

            if let Some(tag_set) = &series.tag_set {
                for (tag_name, tag_value) in tag_set {
                    let tag_name_str = tag_name.to_string();
                    if let Some(packer) = packer_map.get_mut(&tag_name_str) {
                        packer.pack_str(Some(&tag_value.to_string()));
                    } else {
                        panic!(
                            "tag {} seen in input that has no matching column in schema",
                            tag_name
                        )
                    }
                }
                for (field_name, field_value) in &line.field_set {
                    let field_name_str = field_name.to_string();
                    if let Some(packer) = packer_map.get_mut(&field_name_str) {
                        match field_value {
                            delorean_line_parser::FieldValue::F64(f) => {
                                packer.pack_f64(Some(*f));
                            }
                            delorean_line_parser::FieldValue::I64(i) => {
                                packer.pack_i64(Some(*i));
                            }
                        };
                    } else {
                        panic!(
                            "field {} seen in input that has no matching column in schema",
                            field_name
                        )
                    }
                }
                if let Some(packer) = packer_map.get_mut(timestamp_col_name) {
                    packer.pack_i64(line.timestamp);
                } else {
                    panic!("No {} field present in schema...", timestamp_col_name);
                }
            }

            // Now, go over all packers and add missing values if needed
            for (_, packer) in packer_map.iter_mut() {
                if packer.len() < starting_len {
                    assert_eq!(packer.len(), starting_len, "packer should be unchanged");
                    packer.pack_none();
                } else {
                    assert_eq!(
                        packer.len(),
                        starting_len + 1,
                        "packer should have only one value packed"
                    )
                }
            }

            // Should have added one value to all packers
            assert!(
                packer_map.values().all(|x| x.len() == starting_len + 1),
                "Should have added 1 row to all packers"
            );
        }

        packers
    }
}

/// Creates a Schema from an iteraterator over ParsedLines
// TODO add error handling...
fn deduce_schema(parsed_lines: &Vec<ParsedLine>) -> Schema {
    let mut tag_names = HashSet::new();

    let mut field_types: HashMap<String, LineProtocolType> = HashMap::new();

    let mut measurement_name: Option<String> = None;

    for line in parsed_lines.iter() {
        //println!("Considering line: {:?} for schema", line);
        let series = &line.series;
        match &measurement_name {
            // TODO: handle different measurements....
            Some(existing_measurement) => {
                // TODO: avoid this to_string copy by implementing == for EscapedStr and string
                assert_eq!(
                    *existing_measurement,
                    series.measurement.to_string(),
                    "more than one measurement name not supported"
                );
            }
            None => measurement_name = Some(series.measurement.to_string()),
        }
        if let Some(tag_set) = &series.tag_set {
            for (tag_name, _) in tag_set {
                // TODO: implement hashing on EscapedStr to avoid this
                // conversion /copy .
                let tag_name_str = tag_name.to_string();
                tag_names.insert(tag_name_str);
            }
        }
        for (field_name, field_value) in &line.field_set {
            let field_name_str = field_name.to_string();
            let field_type = match field_value {
                FieldValue::F64(_) => LineProtocolType::LPFloat,
                FieldValue::I64(_) => LineProtocolType::LPInteger,
            };

            if field_types.contains_key(&field_name_str) {
                let existing_field_type = field_types.get(&field_name_str).unwrap();
                assert_eq!(
                    *existing_field_type, field_type,
                    "Field  {} previously had a different type {:?} but has type {:?}",
                    field_name, existing_field_type, field_type
                );
            } else {
                field_types.insert(field_name_str, field_type);
            }
        }
    }

    // assign column indexes to all columns
    let mut indexer = IndexGenerator::new();

    Schema {
        measurement: measurement_name.expect("no measurement name found!"),
        tags: tag_names
            .iter()
            .map(|name| (name.clone(), Tag::new(&name, indexer.next())))
            .collect(),
        fields: field_types
            .iter()
            .map(|(name, typ)| (name.clone(), Field::new(&name, *typ, indexer.next())))
            .collect(),
        timestamp: Timestamp {
            timestamp_name: "timestamp".to_string(),
            column_index: indexer.next(),
        },
    }
}
