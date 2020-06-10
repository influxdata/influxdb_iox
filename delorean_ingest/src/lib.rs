#![deny(rust_2018_idioms)]
#![warn(missing_debug_implementations, clippy::explicit_iter_loop)]

//! Library with code for (aspirationally) ingesting various data formats into Delorean
//! TODO move this to delorean/src/ingest/line_protocol.rs?
use log::debug;
use snafu::{ResultExt, Snafu};

use std::collections::BTreeMap;

use delorean_line_parser::{FieldValue, ParsedLine};
use delorean_table::{DeloreanTableWriter, DeloreanTableWriterSource, Error as TableError, Packer};
use delorean_table_schema::{DataType, Schema, SchemaBuilder};

#[derive(Snafu, Debug)]
pub enum Error {
    #[snafu(display(r#"Conversion needs at least one line of data"#))]
    NeedsAtLeastOneLine,

    // Only a single line protocol measurement field is currently supported
    #[snafu(display(r#"More than one measurement not yet supported: {}"#, message))]
    OnlyOneMeasurementSupported { message: String },

    #[snafu(display(r#"Error writing to TableWriter: {}"#, source))]
    Writing { source: TableError },

    #[snafu(display(r#"Error creating TableWriter: {}"#, source))]
    WriterCreation { source: TableError },
}

/// Buffers `ParsedLine` objects and then deduces a schema
/// from that sample. "Sampling" mode
struct MeasurementSampler<'a> {
    /// How many `ParsedLine` structures to buffer before determing the sample size
    sample_size: usize,

    /// The buffered lines to use as a sample
    schema_sample: Vec<ParsedLine<'a>>,
}

/// Once the schema is known, this converter handles the packing of
/// the data.
struct MeasurementWriter {
    /// Schema which describes the lines being written
    schema: Schema,

    /// The sink to which tables are being written
    table_writer: Box<dyn DeloreanTableWriter>,
}

/// Handles packing data from a single one measurement.
#[derive(Debug)]
enum MeasurementConverter<'a> {
    Unknown(MeasurementSampler<'a>),
    Known(MeasurementWriter),
}

/// Handles converting raw line protocol `ParsedLine` structures into Delorean format.
pub struct LineProtocolConverter<'a> {
    // The current converter.
    // Is UnknownSchema When in "Sampling" mode
    /// Changes to KnownSchema when in "Writing" mode
    converter: MeasurementConverter<'a>,

    table_writer_source: Box<dyn DeloreanTableWriterSource>,
}

impl std::fmt::Debug for MeasurementSampler<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("LineProtocolConverter")
            .field("sample_size", &self.sample_size)
            .field("schema_sample", &self.schema_sample)
            .finish()
    }
}

impl std::fmt::Debug for MeasurementWriter {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("LineProtocolConverter")
            .field("schema", &self.schema)
            .field("table_writer", &"DYNAMIC")
            .finish()
    }
}

impl std::fmt::Debug for LineProtocolConverter<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("LineProtocolConverter")
            .field("converter", &self.converter)
            .field("table_writer", &"DYNAMIC")
            .finish()
    }
}

/// `LineProtocolConverter` is used to convert `ParsedLines` (from the same measurement) into the
/// delorean_table internal columnar data format and write the
/// converted data to a `DeloreanTableWriter`
impl<'a> LineProtocolConverter<'a> {
    pub fn schema(&self) -> Option<&Schema> {
        match &self.converter {
            MeasurementConverter::Unknown(_) => None,
            MeasurementConverter::Known(known_converter) => Some(&known_converter.schema),
        }
    }

    /// Construct a converter which will write the finished
    /// delorean_table data using writers created from
    /// `table_writer_source`. Up to the first sample_size records
    /// will be buffered and used to determine the schema.
    pub fn new(
        sample_size: usize,
        table_writer_source: Box<dyn DeloreanTableWriterSource>,
    ) -> Result<LineProtocolConverter<'a>, Error> {
        Ok(LineProtocolConverter {
            converter: MeasurementConverter::Unknown(MeasurementSampler {
                sample_size,
                schema_sample: Vec::new(),
            }),
            table_writer_source,
        })
    }

    /// process  `ParesdLine`s, convert them to the underlying table
    /// format, and write them out. Consumes all lines in iter
    pub fn convert(&mut self, lines: impl Iterator<Item = ParsedLine<'a>>) -> Result<(), Error> {
        let mut peekable = lines.peekable();

        while peekable.peek().is_some() {
            self.convert_some(peekable.by_ref())?
        }
        Ok(())
    }

    /// process some `ParsedLines` for a single measurement from an
    /// interator () lines. May return before consuming the entire iterator
    pub fn convert_some(
        &mut self,
        lines: &mut impl Iterator<Item = ParsedLine<'a>>,
    ) -> Result<(), Error> {
        match &mut self.converter {
            MeasurementConverter::Unknown(unknown_converter) => {
                let num_lines = unknown_converter.sample(lines)?;
                debug!("Convert: buffered {} lines of data", num_lines);
            }
            MeasurementConverter::Known(known_converter) => {
                known_converter.ingest_lines(lines)?;
            }
        };
        self.prepare_for_writing(false)?;
        Ok(())
    }

    /// If in sampling mode and sample is full, changes self.converter
    /// from Sample -> Writing mode and writes all the samples so
    /// far. If in Writing mode does nothing.
    pub fn prepare_for_writing(&mut self, force: bool) -> Result<(), Error> {
        // When are switching modes, holds the sample we need to write
        let converter_and_sample = match &mut self.converter {
            MeasurementConverter::Unknown(unknown_converter) => {
                if force || unknown_converter.sample_full() {
                    debug!(
                        "Preparing for write, dededucing schema (sample_full={}, force={})",
                        unknown_converter.sample_full(),
                        force
                    );

                    let schema = unknown_converter.deduce_schema_from_sample()?;
                    debug!("Deduced line protocol schema: {:#?}", schema);
                    let table_writer = self
                        .table_writer_source
                        .next_writer(&schema)
                        .context(WriterCreation)?;

                    let known_converter = MeasurementWriter {
                        schema,
                        table_writer,
                    };

                    // steal out the schema sample by swapping it with an empty vector
                    let mut taken_sample: Vec<delorean_line_parser::ParsedLine<'_>> = Vec::new();
                    std::mem::swap(&mut unknown_converter.schema_sample, &mut taken_sample);

                    Some((known_converter, taken_sample))
                } else {
                    debug!("Schema sample not yet full, waiting for more lines");
                    None
                }
            }
            MeasurementConverter::Known(_) => None,
        };

        if let Some((known_converter, schema_sample)) = converter_and_sample {
            debug!("Completed change to writing mode");
            self.converter = MeasurementConverter::Known(known_converter);
            // write all lines of the sample
            self.convert(&mut schema_sample.into_iter())?;
        }
        Ok(())
    }

    /// Finalizes all work of this converter and closes/flush the underlying writer.
    pub fn finalize(&mut self) -> Result<(), Error> {
        // If we havent' yet switched to writing mode, do so now
        self.prepare_for_writing(true)?;

        match &mut self.converter {
            MeasurementConverter::Unknown(_) => {
                unreachable!("Should be prepared for writing");
            }
            MeasurementConverter::Known(known_converter) => known_converter.finalize(),
        }
    }
}

impl<'a> MeasurementSampler<'a> {
    fn sample_full(&self) -> bool {
        self.schema_sample.len() >= self.sample_size
    }

    /// Reads lines from the `lines` iterator, stopping at the end of
    /// the iterator or the sample size is reached, whichever is
    /// first. The iterator may not be entirely consumed.
    ///
    /// Returns `Result` with the number of lines which were consumed
    /// into the sample
    fn sample(&mut self, lines: &mut impl Iterator<Item = ParsedLine<'a>>) -> Result<usize, Error> {
        let mut num_sampled: usize = 0;

        if self.sample_full() {
            return Ok(0);
        }

        for line in lines {
            self.schema_sample.push(line);
            num_sampled += 1;
            if self.sample_full() {
                break;
            }
        }
        return Ok(num_sampled);
    }

    /// Use the contents of self.schema_sample to deduce the Schema of `ParsedLine`s.
    fn deduce_schema_from_sample(&mut self) -> Result<Schema, Error> {
        if self.schema_sample.len() < 1 {
            return Err(Error::NeedsAtLeastOneLine {});
        }

        let mut builder = SchemaBuilder::new(&self.schema_sample[0].series.measurement);

        for line in &self.schema_sample {
            let series = &line.series;
            if &series.measurement != builder.get_measurement_name() {
                return Err(Error::OnlyOneMeasurementSupported {
                    message: format!(
                        "Saw new measurement {}, had been using measurement {}",
                        builder.get_measurement_name(),
                        series.measurement
                    ),
                });
            }
            if let Some(tag_set) = &series.tag_set {
                for (tag_name, _) in tag_set {
                    // FIXME avoid the copy / creation of a string!
                    builder = builder.tag(&tag_name.to_string());
                }
            }
            for (field_name, field_value) in &line.field_set {
                let field_type = match field_value {
                    FieldValue::F64(_) => DataType::Float,
                    FieldValue::I64(_) => DataType::Integer,
                };
                // FIXME: avoid the copy!
                builder = builder.field(&field_name.to_string(), field_type);
            }
        }

        Ok(builder.build())
    }
}

impl MeasurementWriter {
    /// Packs a sequence of `ParsedLine`s (which are row-based) from a
    /// single measurement into a columnar memory format and writes
    /// them to the underlying table writer.
    pub fn ingest_lines<'a>(
        &mut self,
        lines: &mut impl Iterator<Item = ParsedLine<'a>>,
    ) -> Result<(), Error> {
        let packers = self.pack_lines(lines);
        self.table_writer.write_batch(&packers).context(Writing)
    }

    /// Internal implementation: packs the `ParsedLine` structures into a Vec for writing
    /// TODO: performance reuse the Vec<Packer> rather than always making new ones
    fn pack_lines<'a>(&mut self, lines: &mut impl Iterator<Item = ParsedLine<'a>>) -> Vec<Packer> {
        let col_defs = self.schema.get_col_defs();
        let mut packers: Vec<Packer> = col_defs
            .iter()
            .enumerate()
            .map(|(idx, col_def)| {
                debug!("  Column definition [{}] = {:?}", idx, col_def);
                Packer::new(col_def.data_type)
            })
            .collect();

        // map col_name -> Packer;
        let mut packer_map: BTreeMap<&String, &mut Packer> = col_defs
            .iter()
            .map(|x| &x.name)
            .zip(packers.iter_mut())
            .collect();

        // for each parsed input line
        // for each tag we expect to see, add an appropriate entry

        for line in lines {
            let timestamp_col_name = self.schema.timestamp();

            // all packers should be the same size
            let starting_len = packer_map
                .get(timestamp_col_name)
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
                self.schema.measurement(),
                "Different measurements in same line protocol stream not supported"
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
            }

            for (field_name, field_value) in &line.field_set {
                let field_name_str = field_name.to_string();
                if let Some(packer) = packer_map.get_mut(&field_name_str) {
                    match *field_value {
                        FieldValue::F64(f) => packer.pack_f64(Some(f)),
                        FieldValue::I64(i) => packer.pack_i64(Some(i)),
                    }
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

            // Now, go over all packers and add missing values if needed
            for packer in packer_map.values_mut() {
                if packer.len() < starting_len + 1 {
                    assert_eq!(packer.len(), starting_len, "packer should be unchanged");
                    packer.pack_none();
                } else {
                    assert_eq!(
                        starting_len + 1,
                        packer.len(),
                        "packer should have only one value packed for a total of {}, instead had {}",
                        starting_len+1, packer.len(),
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

    /// Finalizes all work of this converter and closes the underlying writer.
    pub fn finalize(&mut self) -> Result<(), Error> {
        self.table_writer.close().context(Writing)
    }
}

#[cfg(test)]
mod delorean_ingest_tests {
    use super::*;
    use delorean_table::{DeloreanTableWriter, DeloreanTableWriterSource, Error as TableError};
    use delorean_table_schema::ColumnDefinition;
    use delorean_test_helpers::approximately_equal;

    struct NoOpWriter {}

    impl DeloreanTableWriter for NoOpWriter {
        fn write_batch(&mut self, _packers: &[Packer]) -> Result<(), TableError> {
            Ok(())
        }

        fn close(&mut self) -> Result<(), TableError> {
            Ok(())
        }
    }

    // Constructs NoOpWriters
    struct NoOpWriterSource {
        made: bool,
    }

    impl NoOpWriterSource {
        fn new() -> Box<NoOpWriterSource> {
            Box::new(NoOpWriterSource { made: false })
        }
    }

    impl DeloreanTableWriterSource for NoOpWriterSource {
        fn next_writer(
            &mut self,
            _schema: &Schema,
        ) -> Result<Box<dyn DeloreanTableWriter>, TableError> {
            assert!(!self.made);
            self.made = true;
            Ok(Box::new(NoOpWriter {}))
        }
    }

    fn only_good_lines(data: &str) -> Vec<ParsedLine<'_>> {
        delorean_line_parser::parse_lines(data)
            .filter_map(|r| {
                assert!(r.is_ok());
                r.ok()
            })
            .collect()
    }

    #[test]
    fn no_lines() {
        let parsed_lines = only_good_lines("");
        let converter_result = LineProtocolConverter::new(&parsed_lines, NoOpWriterSource::new());

        assert!(matches!(converter_result, Err(Error::NeedsAtLeastOneLine)));
    }

    #[test]
    fn one_line() {
        let parsed_lines =
            only_good_lines("cpu,host=A,region=west usage_system=64i 1590488773254420000");

        let converter = LineProtocolConverter::new(&parsed_lines, NoOpWriterSource::new())
            .expect("conversion successful");
        assert_eq!(converter.schema.measurement(), "cpu");

        let cols = converter.schema.get_col_defs();
        println!("Converted to {:#?}", cols);
        assert_eq!(cols.len(), 4);
        assert_eq!(cols[0], ColumnDefinition::new("host", 0, DataType::String));
        assert_eq!(
            cols[1],
            ColumnDefinition::new("region", 1, DataType::String)
        );
        assert_eq!(
            cols[2],
            ColumnDefinition::new("usage_system", 2, DataType::Integer)
        );
        assert_eq!(
            cols[3],
            ColumnDefinition::new("timestamp", 3, DataType::Timestamp)
        );
    }

    #[test]
    fn multi_line_same_schema() {
        let parsed_lines = only_good_lines(
            r#"
            cpu,host=A,region=west usage_system=64i 1590488773254420000
            cpu,host=A,region=east usage_system=67i 1590488773254430000"#,
        );

        let converter = LineProtocolConverter::new(&parsed_lines, NoOpWriterSource::new())
            .expect("conversion successful");
        assert_eq!(converter.schema.measurement(), "cpu");

        let cols = converter.schema.get_col_defs();
        println!("Converted to {:#?}", cols);
        assert_eq!(cols.len(), 4);
        assert_eq!(cols[0], ColumnDefinition::new("host", 0, DataType::String));
        assert_eq!(
            cols[1],
            ColumnDefinition::new("region", 1, DataType::String)
        );
        assert_eq!(
            cols[2],
            ColumnDefinition::new("usage_system", 2, DataType::Integer)
        );
        assert_eq!(
            cols[3],
            ColumnDefinition::new("timestamp", 3, DataType::Timestamp)
        );
    }

    #[test]
    fn multi_line_new_field() {
        // given two lines of protocol data that have different field names
        let parsed_lines = only_good_lines(
            r#"
            cpu,host=A,region=west usage_system=64i 1590488773254420000
            cpu,host=A,region=east usage_user=61.32 1590488773254430000"#,
        );

        // when we extract the schema
        let converter = LineProtocolConverter::new(&parsed_lines, NoOpWriterSource::new())
            .expect("conversion successful");
        assert_eq!(converter.schema.measurement(), "cpu");

        // then both field names appear in the resulting schema
        let cols = converter.schema.get_col_defs();
        println!("Converted to {:#?}", cols);
        assert_eq!(cols.len(), 5);
        assert_eq!(cols[0], ColumnDefinition::new("host", 0, DataType::String));
        assert_eq!(
            cols[1],
            ColumnDefinition::new("region", 1, DataType::String)
        );
        assert_eq!(
            cols[2],
            ColumnDefinition::new("usage_system", 2, DataType::Integer)
        );
        assert_eq!(
            cols[3],
            ColumnDefinition::new("usage_user", 3, DataType::Float)
        );
        assert_eq!(
            cols[4],
            ColumnDefinition::new("timestamp", 4, DataType::Timestamp)
        );
    }

    #[test]
    fn multi_line_new_tags() {
        // given two lines of protocol data that have different tags
        let parsed_lines = only_good_lines(
            r#"
            cpu,host=A usage_system=64i 1590488773254420000
            cpu,host=A,fail_group=Z usage_system=61i 1590488773254430000"#,
        );

        // when we extract the schema
        let converter = LineProtocolConverter::new(&parsed_lines, NoOpWriterSource::new())
            .expect("conversion successful");
        assert_eq!(converter.schema.measurement(), "cpu");

        // Then both tag names appear in the resulting schema
        let cols = converter.schema.get_col_defs();
        println!("Converted to {:#?}", cols);
        assert_eq!(cols.len(), 4);
        assert_eq!(cols[0], ColumnDefinition::new("host", 0, DataType::String));
        assert_eq!(
            cols[1],
            ColumnDefinition::new("fail_group", 1, DataType::String)
        );
        assert_eq!(
            cols[2],
            ColumnDefinition::new("usage_system", 2, DataType::Integer)
        );
        assert_eq!(
            cols[3],
            ColumnDefinition::new("timestamp", 3, DataType::Timestamp)
        );
    }

    #[test]
    fn multi_line_field_changed() {
        // given two lines of protocol data that have apparently different data types for the field:
        let parsed_lines = only_good_lines(
            r#"
            cpu,host=A usage_system=64i 1590488773254420000
            cpu,host=A usage_system=61.1 1590488773254430000"#,
        );

        // when we extract the schema
        let converter = LineProtocolConverter::new(&parsed_lines, NoOpWriterSource::new())
            .expect("conversion successful");
        assert_eq!(converter.schema.measurement(), "cpu");

        // Then the first field type appears in the resulting schema (TBD is this what we want??)
        let cols = converter.schema.get_col_defs();
        println!("Converted to {:#?}", cols);
        assert_eq!(cols.len(), 3);
        assert_eq!(cols[0], ColumnDefinition::new("host", 0, DataType::String));
        assert_eq!(
            cols[1],
            ColumnDefinition::new("usage_system", 1, DataType::Integer)
        );
        assert_eq!(
            cols[2],
            ColumnDefinition::new("timestamp", 2, DataType::Timestamp)
        );
    }

    #[test]
    fn multi_line_measurement_changed() {
        // given two lines of protocol data for two different measurements
        let parsed_lines = only_good_lines(
            r#"
            cpu,host=A usage_system=64i 1590488773254420000
            vcpu,host=A usage_system=61i 1590488773254430000"#,
        );

        // when we extract the schema
        let converter_result = LineProtocolConverter::new(&parsed_lines, NoOpWriterSource::new());

        // Then the converter does not support it
        assert!(matches!(
            converter_result,
            Err(Error::OnlyOneMeasurementSupported { message: _ })
        ));
    }

    // given protocol data for each datatype, ensure it is packed
    // as expected.  NOTE the line protocol parser only handles
    // Float and Int field values at the time of this writing so I
    // can't test bool and string here.
    //
    // TODO: add test coverage for string and bool fields when that is available
    static LP_DATA: &str = r#"
               cpu,tag1=A int_field=64i,float_field=100.0 1590488773254420000
               cpu,tag1=B int_field=65i,float_field=101.0 1590488773254430000
               cpu        int_field=66i,float_field=102.0 1590488773254440000
               cpu,tag1=C               float_field=103.0 1590488773254450000
               cpu,tag1=D int_field=67i                   1590488773254460000
               cpu,tag1=E int_field=68i,float_field=104.0
               cpu,tag1=F int_field=69i,float_field=105.0 1590488773254470000
             "#;
    static EXPECTED_NUM_LINES: usize = 7;

    #[test]
    fn pack_data_schema() -> Result<(), Error> {
        let parsed_lines = only_good_lines(LP_DATA);
        assert_eq!(parsed_lines.len(), EXPECTED_NUM_LINES);

        // when we extract the schema
        let converter = LineProtocolConverter::new(&parsed_lines, NoOpWriterSource::new())?;

        // Then the correct schema is extracted
        let cols = converter.schema.get_col_defs();
        println!("Converted to {:#?}", cols);
        assert_eq!(cols.len(), 4);
        assert_eq!(cols[0], ColumnDefinition::new("tag1", 0, DataType::String));
        assert_eq!(
            cols[1],
            ColumnDefinition::new("int_field", 1, DataType::Integer)
        );
        assert_eq!(
            cols[2],
            ColumnDefinition::new("float_field", 2, DataType::Float)
        );

        Ok(())
    }

    // gets the packer's value as a string.
    fn get_string_val(packer: &Packer, idx: usize) -> &str {
        packer.as_string_packer().values[idx].as_utf8().unwrap()
    }

    // gets the packer's value as an int
    fn get_int_val(packer: &Packer, idx: usize) -> i64 {
        packer.as_int_packer().values[idx]
    }

    // gets the packer's value as an int
    fn get_float_val(packer: &Packer, idx: usize) -> f64 {
        packer.as_float_packer().values[idx]
    }

    #[test]
    fn pack_data_value() -> Result<(), Error> {
        let parsed_lines = only_good_lines(LP_DATA);
        assert_eq!(parsed_lines.len(), EXPECTED_NUM_LINES);

        // when we extract the schema and pack the values
        let mut converter = LineProtocolConverter::new(&parsed_lines, NoOpWriterSource::new())?;
        let packers = converter.pack_lines(parsed_lines.into_iter());

        // 4 columns so 4 packers
        assert_eq!(packers.len(), 4);

        // all packers should have packed all lines
        for p in &packers {
            assert_eq!(p.len(), EXPECTED_NUM_LINES);
        }

        // Tag values
        let tag_packer = &packers[0];
        assert_eq!(get_string_val(tag_packer, 0), "A");
        assert_eq!(get_string_val(tag_packer, 1), "B");
        assert!(packers[0].is_null(2));
        assert_eq!(get_string_val(tag_packer, 3), "C");
        assert_eq!(get_string_val(tag_packer, 4), "D");
        assert_eq!(get_string_val(tag_packer, 5), "E");
        assert_eq!(get_string_val(tag_packer, 6), "F");

        // int_field values
        let int_field_packer = &packers[1];
        assert_eq!(get_int_val(int_field_packer, 0), 64);
        assert_eq!(get_int_val(int_field_packer, 1), 65);
        assert_eq!(get_int_val(int_field_packer, 2), 66);
        assert!(int_field_packer.is_null(3));
        assert_eq!(get_int_val(int_field_packer, 4), 67);
        assert_eq!(get_int_val(int_field_packer, 5), 68);
        assert_eq!(get_int_val(int_field_packer, 6), 69);

        // float_field values
        let float_field_packer = &packers[2];
        assert!(approximately_equal(
            get_float_val(float_field_packer, 0),
            100.0
        ));
        assert!(approximately_equal(
            get_float_val(float_field_packer, 1),
            101.0
        ));
        assert!(approximately_equal(
            get_float_val(float_field_packer, 2),
            102.0
        ));
        assert!(approximately_equal(
            get_float_val(float_field_packer, 3),
            103.0
        ));
        assert!(float_field_packer.is_null(4));
        assert!(approximately_equal(
            get_float_val(float_field_packer, 5),
            104.0
        ));
        assert!(approximately_equal(
            get_float_val(float_field_packer, 6),
            105.0
        ));

        // timestamp values
        let timestamp_packer = &packers[3];
        assert_eq!(get_int_val(timestamp_packer, 0), 1_590_488_773_254_420_000);
        assert_eq!(get_int_val(timestamp_packer, 1), 1_590_488_773_254_430_000);
        assert_eq!(get_int_val(timestamp_packer, 2), 1_590_488_773_254_440_000);
        assert_eq!(get_int_val(timestamp_packer, 3), 1_590_488_773_254_450_000);
        assert_eq!(get_int_val(timestamp_packer, 4), 1_590_488_773_254_460_000);
        assert!(timestamp_packer.is_null(5));
        assert_eq!(get_int_val(timestamp_packer, 6), 1_590_488_773_254_470_000);

        Ok(())
    }
}
