use delorean_line_parser::ParsedLine;
use std::fs::File;

use delorean_line_parser::parse_lines;
use line_protocol_conversion::LineProtocolConverter;

mod line_protocol_conversion;
mod line_protocol_schema;
mod packers;
mod parquet;
mod schema_builder;

use crate::parquet::ParquetWriter;

extern crate clap;

#[macro_use]
extern crate log;

use clap::{App, Arg, SubCommand};

// TODO:
// different format support
use std::io;
use std::io::Read;

fn convert(input_filename: &str, output_filename: &str) -> Result<(), io::Error> {
    info!("dstool starting");
    debug!("reading from input file {}", input_filename);
    debug!("writing to output file {}", output_filename);

    // TODO: make a streaming parser that you can stream data through in blocks.
    // for now, just read the whole thing into RAM...
    let mut input_file = File::open(input_filename)?;
    let mut buf = String::new();

    let len = input_file.read_to_string(&mut buf)?;
    info!("Read {} bytes from {}", len, input_filename);

    // scan over all valid lines, and find the unique tag name and field names
    // to build up a column schema
    // scan over all the valid lines again and write to parquet.
    let parsed_lines: Vec<ParsedLine> = match parse_lines(&buf).collect() {
        Ok(v) => v,
        Err(err) => panic!("Error parsing some line {}", err),
    };
    debug!("Found {} line protocol records", parsed_lines.len());

    let converter = LineProtocolConverter::new(&parsed_lines);
    debug!(
        "Deduced schema from {} records of input: {:#?}",
        parsed_lines.len(),
        converter.schema
    );

    info!("Schema deduced. Writing output to {} ...", output_filename);
    let output_file = File::create(output_filename)?;

    let mut writer = match ParquetWriter::new(&converter.schema, output_file) {
        Ok(w) => w,
        // TODO have a better error story
        Err(err) => {
            let msg = format!("Can't make parquet writer: {}", err);
            return Err(io::Error::new(io::ErrorKind::Other, msg));
        }
    };

    writer.write_batch(&converter.pack_lines(&parsed_lines));
    writer.close();
    Ok(())
}

fn main() {
    let help = r#"Delorian Storage Tool

Examples:
    # dumps the contents of line protocol formatted data to stdout
    dstool convert tests/data/lineproto/temperature.txt

"#;

    let matches = App::new(help)
        .version("1.0")
        .author("Andrew Lamb <alamb@influxdata.com>")
        .about("Storage file manipulation and inspection utility")
        .subcommand(
            SubCommand::with_name("convert")
                .about("Convert one storage format to another")
                .arg(
                    Arg::with_name("INPUT")
                        .help("THe input input filename to read from")
                        .required(true)
                        .index(1),
                )
                .arg(
                    Arg::with_name("OUTPUT")
                        .takes_value(true)
                        .help("The filename to write the output.")
                        .required(true)
                        .index(2),
                ), // .arg(Arg::with_name("format")
                   //       .short("f")
                   //       .long("format")
                   //       .help("Sets the output file format. 'line' for line protocol (the default), 'parquet' for parquet")
                   //       .takes_value(true)
                   // )
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
            Ok(()) => {
                debug!("Conversion completed successfully");
            }
            Err(e) => {
                eprintln!("Conversion failed: {}", e);
                std::process::exit(2)
            }
        }
    } else {
        eprintln!("error: No command specified");
        std::process::exit(1);
    }
}
