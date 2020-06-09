#![deny(rust_2018_idioms)]
#![warn(missing_debug_implementations, clippy::explicit_iter_loop)]
use log::{debug, warn};

use clap::{crate_authors, crate_version, App, Arg, SubCommand};

pub mod commands {
    pub mod convert;
    pub mod error;
    pub mod file_meta;
    pub mod input;
}
mod rpc;
mod server;

enum ReturnCode {
    ConversionFailed = 2,
    MetadataDumpFailed = 3,
    ServerExitedAbnormally = 4,
}

fn main() {
    let help = r#"Delorean server and command line tools

Examples:
    # Run the Delorean server:
    delorean

    # Run the Delorean server with extra verbose logging
    delorean -v

    # converts line protocol formatted data in temperature.lp to out.parquet
    delorean convert temperature.lp out.parquet

    # Dumps metadata information about 000000000013.tsm to stdout
    delorean meta 000000000013.tsm
"#;

    let matches = App::new(help)
        .version(crate_version!())
        .author(crate_authors!())
        .about("Delorean server and command line tools")
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
                ),
        )
        .subcommand(SubCommand::with_name("server").about("Runs in server mode (default)"))
        .arg(
            Arg::with_name("verbose")
                .short("v")
                .long("verbose")
                .help("Enables verbose output"),
        )
        .get_matches();

    // TODO: do we want to setup different logging levels for different components?
    // env::set_var("RUST_LOG", "delorean=debug,hyper=info");

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
        match commands::convert::convert(&input_filename, &output_filename) {
            Ok(()) => debug!("Conversion completed successfully"),
            Err(e) => {
                eprintln!("Conversion failed: {}", e);
                std::process::exit(ReturnCode::ConversionFailed as _)
            }
        }
    } else if let Some(matches) = matches.subcommand_matches("meta") {
        let input_filename = matches.value_of("INPUT").unwrap();
        match commands::file_meta::dump_meta(&input_filename) {
            Ok(()) => debug!("Metadata dump completed successfully"),
            Err(e) => {
                eprintln!("Metadata dump failed: {}", e);
                std::process::exit(ReturnCode::MetadataDumpFailed as _)
            }
        }
    } else {
        println!("Staring delorean server...");
        match server::main() {
            Ok(()) => eprintln!("Shutdown OK"),
            Err(e) => {
                warn!("Server shutdown with error: {:?}", e);
                std::process::exit(ReturnCode::ServerExitedAbnormally as _);
            }
        }
    }
}
