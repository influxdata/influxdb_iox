/// This module contains the interface for writing parquet files
/// using data packed using Packer
use crate::line_protocol_schema;

use snafu::Snafu;

use parquet::{
    basic::{LogicalType, Repetition, Type as PhysicalType},
    schema::{printer, types::Type},
};
use parquet::{
    //schema::parser::parse_message_type,
    errors::ParquetError,
    file::{properties::WriterProperties, writer::SerializedFileWriter},
};
use std::rc::Rc;

use crate::packers::Packer;

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display(r#"Feature is not yet implemented: {} :("#, feature_name))]
    NotYetImplemented { feature_name: String },
    #[snafu(display(r#" Error from parquet library: {}"#, error_source))]
    ParquetLibraryError { error_source: ParquetError },
}

pub struct ParquetWriter {
    file_writer: SerializedFileWriter<std::fs::File>,
}

fn convert_to_parquet_schema(
    schema: &line_protocol_schema::Schema,
) -> parquet::schema::types::Type {
    let mut parquet_columns = Vec::new();

    let col_defs = schema.get_col_defs();
    for col_def in col_defs {
        let (physical_type, logical_type) = match col_def.column_type {
            line_protocol_schema::LineProtocolType::LPFloat => (PhysicalType::DOUBLE, None),
            line_protocol_schema::LineProtocolType::LPInteger => {
                (PhysicalType::INT64, Some(LogicalType::UINT_64))
            }
            line_protocol_schema::LineProtocolType::LPString => {
                (PhysicalType::BYTE_ARRAY, Some(LogicalType::UTF8))
            }
            line_protocol_schema::LineProtocolType::LPTimestamp => {
                eprintln!("WARNING WARNING: writing parquet using MICROS not NANOS (no support for NANOs..)");
                (PhysicalType::INT64, Some(LogicalType::TIMESTAMP_MICROS))
            }
        };

        let mut parquet_column_builder =
            Type::primitive_type_builder(&col_def.column_name, physical_type)
                .with_repetition(Repetition::OPTIONAL); // All fields are optional

        if logical_type.is_some() {
            parquet_column_builder =
                parquet_column_builder.with_logical_type(logical_type.unwrap());
        }

        let parquet_column = parquet_column_builder.build().unwrap();
        parquet_columns.push(Rc::new(parquet_column));
    }

    Type::group_type_builder(&schema.measurement)
        .with_fields(&mut parquet_columns)
        .build()
        .unwrap()
}

fn create_writer_props(_schema: &line_protocol_schema::Schema) -> WriterProperties {
    let builder = WriterProperties::builder();

    eprintln!("TODO: setup column encoding for parquet writer.");
    builder.set_created_by("Andrew".to_string()).build()
}

impl ParquetWriter {
    /// Creates a new ParquetWriter suitable for writing data that conforms to
    /// the specified schema.
    pub fn new(
        schema: &line_protocol_schema::Schema,
        file: std::fs::File,
    ) -> Result<ParquetWriter, Error> {
        let parquet_schema = convert_to_parquet_schema(&schema);
        let mut parquet_schema_string: Vec<u8> = Vec::new();
        printer::print_schema(&mut parquet_schema_string, &parquet_schema);
        debug!(
            "Parquet schema: {:#?}",
            String::from_utf8_lossy(&parquet_schema_string)
        );

        let writer_props = create_writer_props(&schema);

        if false {
            return NotYetImplemented {
                feature_name: "Writer creation",
            }
            .fail();
        }

        match SerializedFileWriter::new(file, Rc::new(parquet_schema), Rc::new(writer_props)) {
            Ok(file_writer) => Ok(ParquetWriter { file_writer }),
            Err(e) => ParquetLibraryError { error_source: e }.fail(),
        }
    }

    /// Writes a batch of ParsedLines to the output file in a single
    /// column chunk TODO: should we allow multiple column chunks?
    /// TODO: Figure out how to pass an iterator / something other than a
    /// direct vector of lines
    pub fn write_batch(self: &mut Self, packers: &Vec<Packer>) {
        // now write out the data
        use parquet::file::writer::FileWriter;
        let mut row_group_writer = self.file_writer.next_row_group().unwrap();

        use parquet::column::writer::ColumnWriter::*;
        let mut column_number = 0;
        while let Some(mut col_writer) = row_group_writer.next_column().unwrap() {
            let packer = packers
                .get(column_number)
                .expect("mismatch in parquet columns and number of packers");
            match col_writer {
                BoolColumnWriter(_) => panic!("unhandled parquet column writer type: BOOL"),
                Int32ColumnWriter(_) => panic!("unhandled parquet column writer type: INT32"),
                Int64ColumnWriter(ref mut w) => {
                    let int_packer = packer.as_int_packer();
                    let n = w
                        .write_batch(
                            &int_packer.values,
                            Some(&int_packer.def_levels),
                            Some(&int_packer.rep_levels),
                        )
                        .expect("can't write ints to parquet column");
                    debug!("wrote {} rows of int64 data", n);
                }
                Int96ColumnWriter(_) => panic!("unhandled parquet column writer type: INT96"),
                FloatColumnWriter(_) => panic!("unhandled parquet column writer type: FLOAT"),
                DoubleColumnWriter(ref mut w) => {
                    let float_packer = packer.as_float_packer();
                    let n = w
                        .write_batch(
                            &float_packer.values,
                            Some(&float_packer.def_levels),
                            Some(&float_packer.rep_levels),
                        )
                        .expect("can't write f64 to parquet column");
                    debug!("wrote {} rows of f64 data", n);
                }
                ByteArrayColumnWriter(ref mut w) => {
                    let string_packer = packer.as_string_packer();
                    let n = w
                        .write_batch(
                            &string_packer.values,
                            Some(&string_packer.def_levels),
                            Some(&string_packer.rep_levels),
                        )
                        .expect("can't write bytes");
                    debug!("wrote {} rows of byte data", n);
                }
                FixedLenByteArrayColumnWriter(_) => {
                    panic!("unhandled parquet column writer type: FIXED_LEN_BYTE_ARRAY")
                }
            };
            debug!("Closing column writer for {}", column_number);
            row_group_writer.close_column(col_writer).unwrap();
            column_number += 1;
        }
        self.file_writer.close_row_group(row_group_writer).unwrap(); // TODO error handling
    }
    /// Closes this writer, and finalizes the underlying file
    pub fn close(self: &mut Self) {
        use parquet::file::writer::FileWriter;
        self.file_writer.close().unwrap();
    }
}
