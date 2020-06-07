#![deny(rust_2018_idioms)]
#![warn(missing_debug_implementations, clippy::explicit_iter_loop)]

use std::fs;
use log::{debug, info, warn};

use std::collections::{HashMap, HashSet};

use clap::{crate_authors, crate_version, App, Arg, SubCommand};
use snafu::Snafu;
use delorean::storage::tsm::{TSMReader, InfluxID, IndexEntry};
use delorean::storage::StorageError;

use delorean_ingest::LineProtocolConverter;
use delorean_line_parser::{parse_lines, ParsedLine};
use delorean_parquet::writer::{DeloreanTableWriter, Error as DeloreanTableWriterError};

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display(r#"IO Error: {} ({})"#, message, source))]
    IO {
        message: String,
        source: std::io::Error,
    },
    Parsing {
        source: delorean_line_parser::Error,
    },
    Conversion {
        source: delorean_ingest::Error,
    },
    Writing {
        source: DeloreanTableWriterError,
    },
}

type Result<T, E = Error> = std::result::Result<T, E>;

impl From<std::io::Error> for Error {
    fn from(other: std::io::Error) -> Self {
        Error::IO {
            message: String::from("io error"),
            source: other,
        }
    }
}

impl From<delorean_line_parser::Error> for Error {
    fn from(other: delorean_line_parser::Error) -> Self {
        Error::Parsing { source: other }
    }
}

impl From<delorean_ingest::Error> for Error {
    fn from(other: delorean_ingest::Error) -> Self {
        Error::Conversion { source: other }
    }
}

impl From<DeloreanTableWriterError> for Error {
    fn from(other: DeloreanTableWriterError) -> Self {
        Error::Writing { source: other }
    }
}

enum ReturnCode {
    InternalError = 1,
    ConversionFailed = 2,
    MetadataDumpFailed = 3,
}

static SCHEMA_SAMPLE_SIZE: usize = 5;

fn convert(input_filename: &str, output_filename: &str) -> Result<()> {
    info!("dstool convert starting");
    debug!("Reading from input file {}", input_filename);
    debug!("Writing to output file {}", output_filename);

    // TODO: make a streaming parser that you can stream data through in blocks.
    // for now, just read the whole input file into RAM...
    let buf = fs::read_to_string(input_filename).map_err(|source| {
        let message = format!("Error reading {}", input_filename);
        Error::IO { message, source }
    })?;
    info!("Read {} bytes from {}", buf.len(), input_filename);

    // FIXME: Design something sensible to do with lines that don't
    // parse rather than just dropping them on the floor
    let mut only_good_lines = parse_lines(&buf).filter_map(|r| match r {
        Ok(line) => Some(line),
        Err(e) => {
            warn!("Ignorning line with parse error: {}", e);
            None
        }
    });

    let schema_sample: Vec<ParsedLine<'_>> =
        only_good_lines.by_ref().take(SCHEMA_SAMPLE_SIZE).collect();

    // The idea here is to use the first few parsed lines to deduce the schema
    let converter = LineProtocolConverter::new(&schema_sample)?;
    debug!("Using schema deduced from sample: {:?}", converter.schema());

    info!("Schema deduced. Writing output to {} ...", output_filename);
    let output_file = fs::File::create(output_filename)?;

    let mut writer = DeloreanTableWriter::new(converter.schema(), output_file)?;

    // Write the sample and then the remaining lines
    writer.write_batch(&converter.pack_lines(schema_sample.into_iter()))?;
    writer.write_batch(&converter.pack_lines(only_good_lines))?;
    writer.close()?;
    info!("Completing writing {} successfully", output_filename);
    Ok(())
}

struct MeasurementMetadata {
    // tag name --> list of seen tag values
    tags : HashMap<String, HashSet<String>>,
    // List of field names seen
    fields: HashSet<String>,

}

impl MeasurementMetadata {
    fn new() -> MeasurementMetadata {
        MeasurementMetadata {
            tags: HashMap::new(),
            fields: HashSet::new(),
        }
    }

    fn update_for_entry(&mut self, index_entry :&mut IndexEntry) {
        let tagset = index_entry.tagset().expect("error decoding tagset");
        for (tag_name, tag_value) in tagset {
            let tag_entry = self.tags.entry(tag_name).or_insert(HashSet::new());
            tag_entry.insert(tag_value);
        }
        let field_name = index_entry.field_key().expect("error decoding field name");
        self.fields.insert(field_name);
    }

    fn print_report(&self, prefix: &str) {
        for (tag_name, tag_values) in self.tags.iter() {
            println!("{} tag {} = {:?}", prefix, tag_name, tag_values);
        }
        for field_name in self.fields.iter() {
            println!("{} field {}", prefix, field_name);
        }

    }
}

// Represents stats for a single bucket
struct BucketMetadata {
    /// How many index entries have been seen
    count : u64,

    // Total 'records' (aka sum of the lengths of all timeseries)
    total_records: u64,

    // measurement ->
    measurements: HashMap<String, MeasurementMetadata>
}

impl BucketMetadata {
    fn new() -> BucketMetadata {
        BucketMetadata {
            count: 0,
            total_records: 0,
            measurements: HashMap::new(),
        }
    }

    fn update_for_entry(&mut self, index_entry :&mut IndexEntry) {
        self.count += 1;
        self.total_records += index_entry.count as u64;
        let measurement = index_entry.measurement().expect("error decoding measurement name");
        let meta = self.measurements.entry(measurement).or_insert(MeasurementMetadata::new());
        meta.update_for_entry(index_entry);
    }

    fn print_report(&self, prefix: &str) {
        for (measurement, meta) in self.measurements.iter() {
            println!("{}{}", prefix, measurement);
            let indent = format!("{}  ", prefix);
            meta.print_report(&indent);
        }
    }


}

struct TSMMetadataBuilder {

    num_good_entries : u32,
    num_bad_entries : u32,

    // (org_id, bucket_id) --> Bucket Metadata
    bucket_stats : HashMap<(InfluxID, InfluxID), BucketMetadata>
}

impl TSMMetadataBuilder {
    fn new() -> TSMMetadataBuilder {
        TSMMetadataBuilder {
            num_good_entries : 0,
            num_bad_entries : 0,
            bucket_stats : HashMap::new(),
        }
    }

    fn process_entry(&mut self, entry : &mut Result<IndexEntry, StorageError>) {
        match entry {
            Ok(index_entry) => {
                self.num_good_entries += 1;
                let key = (index_entry.org_id(), index_entry.bucket_id());
                let stats = self.bucket_stats.entry(key).or_insert(BucketMetadata::new());
                stats.update_for_entry(index_entry);
            },
            Err(e) => {
                warn!("Ignoring entry decoding error {:?}", e);
                self.num_bad_entries += 1;
            },
        }
    }

    fn print_report(&self) {
        println!("TSM Metadata Report:");
        println!("  Valid Index Entries: {}", self.num_good_entries);
        if self.num_bad_entries > 0 {
            println!("  Invalid Entries: {}", self.num_bad_entries);
        }
        println!("  Organizations/Bucket Stats:");
        for (k, stats) in self.bucket_stats.iter() {
            let (org_id, bucket_id) = k;
            println!("    ({}, {}) {} index entries, {} total records",
                     org_id, bucket_id, stats.count, stats.total_records);
            println!("    Measurements:");
            stats.print_report("      ");
        }
    }
}

fn dump_meta(input_filename: &str) -> Result<()> {
    info!("dstool meta starting");
    debug!("Reading from input file {}", input_filename);

    // TODO: figure out the format in some intelligent way. Right now assume it is TSM.
    let file = fs::File::open(input_filename).map_err(|e| {
        let msg = format!("Error reading {}", input_filename);
        Error::IOError {
            message: msg,
            source: Arc::new(e),
        }
    })?;

    let file_size = file.metadata()
        .map_err(|e| {
            let message = format!("Error reading metadata for {}", input_filename);
            Error::IOError {
                message,
                source: Arc::new(e),
            }
        })?.len();

    let mut reader = TSMReader::new(BufReader::new(file), file_size as usize);

    let index = reader.index().expect("Error reading index");

    let mut stats_builder = TSMMetadataBuilder::new();

    for mut entry in index {
        stats_builder.process_entry(&mut entry);
    }

    stats_builder.print_report();

    Ok(())
}

fn main() {
    let help = r#"Delorean Storage Tool

Examples:
    # converts line protocol formatted data in temperature.lp to out.parquet
    dstool convert temperature.lp out.parquet

"#;

    let matches = App::new(help)
        .version(crate_version!())
        .author(crate_authors!())
        .about("Storage file manipulation and inspection utility")
        .subcommand(
            SubCommand::with_name("convert")
                .about("Convert one storage format to another")
                .arg(
                    Arg::with_name("INPUT")
                        .help("The input filename to read from")
                        .required(true)
                        .index(1),
                )
                .arg(
                    Arg::with_name("OUTPUT")
                        .takes_value(true)
                        .help("The filename to write the output.")
                        .required(true)
                        .index(2),
                ),
        )
        .subcommand(
            SubCommand::with_name("meta")
                .about("Print out metadata information about a storage file")
                .arg(
                    Arg::with_name("INPUT")
                        .help("The input filename to read from")
                        .required(true)
                        .index(1),
                )
        )
        .arg(
            Arg::with_name("verbose")
                .short("v")
                .long("verbose")
                .help("Enables verbose output"),
        )
        .get_matches();

    if matches.is_present("verbose") {
        std::env::set_var("RUST_LOG", "debug");
    } else {
        std::env::set_var("RUST_LOG", "info");
    }
    env_logger::init();

    if let Some(matches) = matches.subcommand_matches("convert") {
        // clap says Calling .unwrap() is safe here because "INPUT"
        // and "OUTPUT" are required (if "INPUT" wasn't required we
        // could have used an 'if let' to conditionally get the value)
        let input_filename = matches.value_of("INPUT").unwrap();
        let output_filename = matches.value_of("OUTPUT").unwrap();
        match convert(&input_filename, &output_filename) {
            Ok(()) => debug!("Conversion completed successfully"),
            Err(e) => {
                eprintln!("Conversion failed: {}", e);
                std::process::exit(ReturnCode::ConversionFailed as _)
            }
        }
    } else if let Some(matches) = matches.subcommand_matches("meta") {
        let input_filename = matches.value_of("INPUT").unwrap();
        match dump_meta(&input_filename) {
            Ok(()) => debug!("Metadata dump completed successfully"),
            Err(e) => {
                eprintln!("Metadata dump failed: {}", e);
                std::process::exit(ReturnCode::MetadataDumpFailed as _)
            }
        }
    } else {
        eprintln!("Internal error: no command found");
        std::process::exit(ReturnCode::InternalError as _);
    }
}
