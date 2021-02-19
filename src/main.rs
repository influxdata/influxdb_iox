//! Entrypoint of InfluxDB IOx binary
#![deny(rust_2018_idioms)]
#![warn(
    missing_debug_implementations,
    clippy::explicit_iter_loop,
    clippy::use_self,
    clippy::clone_on_ref_ptr
)]

use std::error::Error;

use clap::{crate_authors, crate_version, value_t, App, Arg, ArgMatches, SubCommand};
use dotenv::dotenv;
use structopt::StructOpt;
use tokio::runtime::Runtime;
use tracing::{debug, error, info, warn};

use commands::{config::Config, logging::LoggingLevel};
use generated_types::influxdata::iox::management::v1::DatabaseRules;
use influxdb_iox_client::ClientBuilder;
use ingest::parquet::writer::CompressionLevel;

mod commands {
    pub mod config;
    pub mod convert;
    pub mod file_meta;
    mod input;
    pub mod logging;
    pub mod stats;
}

pub mod influxdb_ioxd;

enum ReturnCode {
    ConversionFailed = 1,
    MetadataDumpFailed = 2,
    StatsFailed = 3,
    ServerExitedAbnormally = 4,
}

fn main() -> Result<(), std::io::Error> {
    let help = r#"InfluxDB IOx server and command line tools

Examples:
    # Run the InfluxDB IOx server:
    influxdb_iox

    # Display all server settings
    influxdb_iox server --help

    # Run the InfluxDB IOx server with extra verbose logging
    influxdb_iox -v

    # Run InfluxDB IOx with full debug logging specified with RUST_LOG
    RUST_LOG=debug influxdb_iox

    # converts line protocol formatted data in temperature.lp to out.parquet
    influxdb_iox convert temperature.lp out.parquet

    # Dumps metadata information about 000000000013.tsm to stdout
    influxdb_iox meta 000000000013.tsm

    # Dumps storage statistics about out.parquet to stdout
    influxdb_iox stats out.parquet
"#;
    // load all environment variables from .env before doing anything
    load_dotenv();

    let database_name = Arg::with_name("NAME")
        .help("Database name")
        .required(true)
        .index(1);

    let iox_address = Arg::with_name("address")
        .help("Address of the IOx gRPC endpoint")
        .long("address")
        .env("IOX_ADDR")
        .default_value("http://127.0.0.1:8082");

    let matches = App::new(help)
        .version(crate_version!())
        .author(crate_authors!())
        .about("InfluxDB IOx server and command line tools")
        .subcommand(
            SubCommand::with_name("convert")
                .about("Convert one storage format to another")
                .arg(
                    Arg::with_name("INPUT")
                        .help("The input files to read from")
                        .required(true)
                        .index(1),
                )
                .arg(
                    Arg::with_name("OUTPUT")
                        .takes_value(true)
                        .help("The filename or directory to write the output.")
                        .required(true)
                        .index(2),
                )
                .arg(
                    Arg::with_name("compression_level")
                        .short("c")
                        .long("compression-level")
                        .help("How much to compress the output data. 'max' compresses the most; 'compatibility' compresses in a manner more likely to be readable by other tools.")
                        .takes_value(true)
                        .possible_values(&["max", "compatibility"])
                        .default_value("compatibility"),
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
        .subcommand(
            SubCommand::with_name("stats")
                .about("Print out storage statistics information to stdout. \
                        If a directory is specified, checks all files recursively")
                .arg(
                    Arg::with_name("INPUT")
                        .help("The input filename or directory to read from")
                        .required(true)
                        .index(1),
                )
                .arg(
                    Arg::with_name("per-column")
                        .long("per-column")
                        .help("Include detailed information per column")
                )
                .arg(
                    Arg::with_name("per-file")
                        .long("per-file")
                        .help("Include detailed information per file")
                ),
        )
        .subcommand(
            SubCommand::with_name("operator")
                .about("Commands for operating an IOx server")
                .subcommand(
                    SubCommand::with_name("ping")
                        .about("Check connectivity to server")
                        .arg(iox_address.clone())
                )

                .subcommand(
                    SubCommand::with_name("set-writer")
                        .about("Set writer ID")
                        .arg(iox_address.clone())
                        .arg(Arg::with_name("ID")
                            .required(true)
                            .help("Writer ID")
                        )
                )
                .subcommand(
                    SubCommand::with_name("get-writer")
                        .about("Get writer ID")
                        .arg(iox_address.clone())
                )
        )
        .subcommand(
            SubCommand::with_name("database")
                .about("Commands for interacting with IOx databases")
                .subcommand(
                    SubCommand::with_name("create")
                        .about("Create a new database")
                        .arg(iox_address.clone())
                        .arg(database_name.clone())
                )
                .subcommand(
                    SubCommand::with_name("list")
                        .about("List the configured databases")
                        .arg(iox_address.clone())
                )
                .subcommand(
                    SubCommand::with_name("show")
                        .about("Show the database configuration")
                        .arg(iox_address)
                        .arg(database_name)
                )
        )
        .subcommand(
            commands::config::Config::clap(),
        )
        .arg(Arg::with_name("verbose").short("v").long("verbose").multiple(true).help(
            "Enables verbose logging (use 'vv' for even more verbosity). You can also set log level via \
                       the environment variable RUST_LOG=<value>",
        ))
        .arg(Arg::with_name("num-threads").long("num-threads").takes_value(true).help(
            "Set the maximum number of threads to use. Defaults to the number of cores on the system",
        ))
        .get_matches();

    let tokio_runtime = get_runtime(matches.value_of("num-threads"))?;
    tokio_runtime.block_on(dispatch_args(matches));

    info!("InfluxDB IOx server shutting down");
    Ok(())
}

async fn dispatch_args(matches: ArgMatches<'_>) {
    // Logging level is determined via:
    // 1. If RUST_LOG environment variable is set, use that value
    // 2. if `-vv` (multiple instances of verbose), use DEFAULT_DEBUG_LOG_LEVEL
    // 2. if `-v` (single instances of verbose), use DEFAULT_VERBOSE_LOG_LEVEL
    // 3. Otherwise use DEFAULT_LOG_LEVEL
    let logging_level = LoggingLevel::new(matches.occurrences_of("verbose"));

    match matches.subcommand() {
        ("convert", Some(sub_matches)) => {
            logging_level.setup_basic_logging();
            let input_path = sub_matches.value_of("INPUT").unwrap();
            let output_path = sub_matches.value_of("OUTPUT").unwrap();
            let compression_level =
                value_t!(sub_matches, "compression_level", CompressionLevel).unwrap();
            match commands::convert::convert(&input_path, &output_path, compression_level) {
                Ok(()) => debug!("Conversion completed successfully"),
                Err(e) => {
                    eprintln!("Conversion failed: {}", e);
                    std::process::exit(ReturnCode::ConversionFailed as _)
                }
            }
        }
        ("meta", Some(sub_matches)) => {
            logging_level.setup_basic_logging();
            let input_filename = sub_matches.value_of("INPUT").unwrap();
            match commands::file_meta::dump_meta(&input_filename) {
                Ok(()) => debug!("Metadata dump completed successfully"),
                Err(e) => {
                    eprintln!("Metadata dump failed: {}", e);
                    std::process::exit(ReturnCode::MetadataDumpFailed as _)
                }
            }
        }
        ("stats", Some(sub_matches)) => {
            logging_level.setup_basic_logging();
            let config = commands::stats::StatsConfig {
                input_path: sub_matches.value_of("INPUT").unwrap().into(),
                per_file: sub_matches.is_present("per-file"),
                per_column: sub_matches.is_present("per-column"),
            };

            match commands::stats::stats(&config).await {
                Ok(()) => debug!("Storage statistics dump completed successfully"),
                Err(e) => {
                    eprintln!("Stats dump failed: {}", e);
                    std::process::exit(ReturnCode::StatsFailed as _)
                }
            }
        }

        //TODO: Better CLI output system than Vec<String>
        //TODO: Better error handling
        ("database", Some(matches)) => {
            if let (subcommand, Some(matches)) = matches.subcommand() {
                match handle_database_command(subcommand, matches).await {
                    Ok(data) => println!("{}", data.join(", ")),
                    Err(e) => {
                        eprintln!("Error: {}", e);
                        std::process::exit(1)
                    }
                }
            }
        }
        ("operator", Some(matches)) => {
            if let (subcommand, Some(matches)) = matches.subcommand() {
                match handle_operator_command(subcommand, matches).await {
                    Ok(data) => println!("{}", data.join(", ")),
                    Err(e) => {
                        eprintln!("Error: {}", e);
                        std::process::exit(1)
                    }
                }
            }
        }

        // Handle the case where the user explicitly specified the server command
        ("server", Some(sub_matches)) => {
            // Note don't set up basic logging here, different logging rules apply in server
            // mode
            let res =
                influxdb_ioxd::main(logging_level, Some(Config::from_clap(sub_matches))).await;

            if let Err(e) = res {
                error!("Server shutdown with error: {}", e);
                std::process::exit(ReturnCode::ServerExitedAbnormally as _);
            }
        }
        // handle the case where the user didn't specify a command
        (_, _) => {
            // Note don't set up basic logging here, different logging rules apply in server
            // mode
            let res = influxdb_ioxd::main(logging_level, None).await;
            if let Err(e) = res {
                error!("Server shutdown with error: {}", e);
                std::process::exit(ReturnCode::ServerExitedAbnormally as _);
            }
        }
    }
}

async fn handle_operator_command(
    subcommand: &str,
    matches: &ArgMatches<'_>,
) -> Result<Vec<String>, Box<dyn Error>> {
    let url = matches.value_of("address").unwrap();
    let mut client = ClientBuilder::default().build(url).await?;

    match subcommand {
        "ping" => {
            client.ping().await?;

            Ok(vec!["Ok".to_string()])
        }
        "set-writer" => {
            let writer_id = matches.value_of("ID").unwrap().parse()?;
            client.update_writer_id(writer_id).await?;

            Ok(vec!["Ok".to_string()])
        }
        "get-writer" => {
            let id = client.get_writer_id().await?;

            Ok(vec![format!("Writer ID: {}", id)])
        }
        _ => unreachable!(),
    }
}

async fn handle_database_command(
    subcommand: &str,
    matches: &ArgMatches<'_>,
) -> Result<Vec<String>, Box<dyn Error>> {
    let url = matches.value_of("address").unwrap();
    let mut client = ClientBuilder::default().build(url).await?;

    match subcommand {
        "create" => {
            let database_name = matches.value_of("NAME").unwrap();
            client
                .create_database(DatabaseRules {
                    name: database_name.to_string(),
                    ..Default::default()
                })
                .await?;

            Ok(vec!["Ok".to_string()])
        }
        "show" => {
            let database_name = matches.value_of("NAME").unwrap();
            let rules = client.get_database(database_name).await?;

            // TODO: Do something better than this
            Ok(vec![format!("{:?}", rules)])
        }
        "list" => Ok(client.list_databases().await?),
        _ => unreachable!(),
    }
}

/// Creates the tokio runtime for executing IOx
///
/// if nthreads is none, uses the default scheduler
/// otherwise, creates a scheduler with the number of threads
fn get_runtime(num_threads: Option<&str>) -> Result<Runtime, std::io::Error> {
    // NOTE: no log macros will work here!
    //
    // That means use eprintln!() instead of error!() and so on. The log emitter
    // requires a running tokio runtime and is initialised after this function.

    use tokio::runtime::Builder;
    let kind = std::io::ErrorKind::Other;
    match num_threads {
        None => Runtime::new(),
        Some(num_threads) => {
            println!(
                "Setting number of threads to '{}' per command line request",
                num_threads
            );
            let n = num_threads.parse::<usize>().map_err(|e| {
                let msg = format!(
                    "Invalid num-threads: can not parse '{}' as an integer: {}",
                    num_threads, e
                );
                std::io::Error::new(kind, msg)
            })?;

            match n {
                0 => {
                    let msg = format!(
                        "Invalid num-threads: '{}' must be greater than zero",
                        num_threads
                    );
                    Err(std::io::Error::new(kind, msg))
                }
                1 => Builder::new_current_thread().enable_all().build(),
                _ => Builder::new_multi_thread()
                    .enable_all()
                    .worker_threads(n)
                    .build(),
            }
        }
    }
}

/// Source the .env file before initialising the Config struct - this sets
/// any envs in the file, which the Config struct then uses.
///
/// Precedence is given to existing env variables.
fn load_dotenv() {
    match dotenv() {
        Ok(_) => {}
        Err(dotenv::Error::Io(err)) if err.kind() == std::io::ErrorKind::NotFound => {
            // Ignore this - a missing env file is not an error, defaults will
            // be applied when initialising the Config struct.
        }
        Err(e) => {
            eprintln!("FATAL Error loading config from: {}", e);
            eprintln!("Aborting");
            std::process::exit(1);
        }
    };
}
