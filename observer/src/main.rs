//! Script to scrape some basic information from an IOx server
//!
//! Assumes port forwarding loca port 1234 to the iox cluster in tools with a
//! command such as:
//!
//! ```shell
//! kubectl -n iox port-forward svc/iox 1234:8082
//! ```
//!
//! Other possibly useful commands:
//!
//! ```shell
//!  ./target/debug/influxdb_iox --host http://localhost:1234 database query 844910ece80be8bc_4bed41a6ff7f0ee0 "select * from system.chunks"
use command::Command;
use context::Context;
use query::LocalQuery;
use remote::RemoteLoad;
use rustyline::Editor;
use structopt::StructOpt;

use influxdb_iox_client::connection::{Builder, Connection};

mod command;
mod context;
mod query;
mod remote;

#[derive(Debug, StructOpt)]
#[structopt(
    name = "observer",
    about = "InfluxDB IOx interactive SQL Client",
    long_about = r#"InfluxDB IOx interactive SQL Client

Examples:
    # connect
    observer --host http
"#
)]
struct Config {
    /// gRPC address of IOx server to connect to
    #[structopt(short, long, global = true, default_value = "http://127.0.0.1:8082")]
    host: String,
}

#[tokio::main]
async fn main() {
    let config = Config::from_args();
    let url = &config.host;
    println!("Connecting to {}...", url);

    let connection = match Builder::default().build(url).await {
        Ok(c) => c,
        Err(e) => {
            eprint!("Can not connect to IOx at {}: {}", url, e);
            return;
        }
    };

    println!("Connected to IOx at {}", url);

    let context = Context::new(connection.clone());

    check_health(connection.clone()).await;

    println!(
        r#"Starting interactive command line...
hint: try

REMOTE LOAD;
"#
    );

    repl(context).await
}

async fn check_health(connection: Connection) {
    print!("Checking storage ...");
    let mut client = influxdb_iox_client::health::Client::new(connection);

    match client.check_storage().await {
        Ok(()) => println!(" Health Check: OK"),
        Err(e) => {
            println!(" Health Check FAILED: {}", e);
            panic!("Health check failed, aborting");
        }
    }
}

/// Read Evaluate Print Loop (interactive command line) for inspect
///
/// Inspired / based on repl.rs from DataFusion
async fn repl(mut context: Context) {
    let mut rl = Editor::<()>::new();
    rl.load_history(".history").ok();

    // The order of the commands is the order we try to run them in
    let mut commands: Vec<Box<dyn Command>> = vec![
        Box::new(RemoteLoad::default()),
        Box::new(LocalQuery::default()),
    ];

    let mut request = "".to_owned();
    loop {
        let readline = rl.readline("> ");
        match readline {
            Ok(ref line) if is_exit_command(line) && request.is_empty() => {
                break;
            }
            Ok(ref line) if line.trim_end().ends_with(';') => {
                request.push_str(line.trim_end());
                rl.add_history_entry(request.clone());

                run_command(&mut context, &request, &mut commands).await;
                request = "".to_owned();
            }
            Ok(ref line) => {
                request.push_str(line);
                request.push(' ');
            }
            Err(_) => {
                break;
            }
        }
    }

    rl.save_history(".history").ok();
}

fn is_exit_command(line: &str) -> bool {
    let line = line.trim_end().to_lowercase();
    line == "quit" || line == "exit"
}

async fn run_command(ctx: &mut Context, request: &str, commands: &mut [Box<dyn Command>]) {
    let request = request.strip_suffix(";").unwrap_or(request).trim_end();

    for command in commands {
        match command.matches(request, ctx).await {
            Ok(true) => return,
            // command did not know how to handle request, try next one
            Ok(false) => {}
            // command handled request, but got error
            Err(e) => {
                println!("Error running command: {}", e);
                return;
            }
        };
    }

    println!("Error unknown request {}", request);
}
