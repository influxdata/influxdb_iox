//! This module implements the `chunk` CLI command
use data_types::chunk::ChunkSummary;
use generated_types::google::FieldViolation;
use influxdb_iox_client::{
    connection::Builder,
    management::{self, ListChunksError},
};
use std::convert::TryFrom;
use structopt::StructOpt;
use thiserror::Error;

#[derive(Debug, Error)]
pub enum Error {
    #[error("Error listing chunks: {0}")]
    ListChunkError(#[from] ListChunksError),

    #[error("Error interpreting server response: {:?}", .0)]
    ConvertingResponse(FieldViolation),

    #[error("Error rendering response as JSON: {0}")]
    WritingJson(#[from] serde_json::Error),

    #[error("Error connecting to IOx: {0}")]
    ConnectionError(#[from] influxdb_iox_client::connection::Error),
}

pub type Result<T, E = Error> = std::result::Result<T, E>;

/// Manage IOx databases
#[derive(Debug, StructOpt)]
pub struct Config {
    #[structopt(subcommand)]
    command: Command,
}

/// Get list of chunks for the specified database in JSON format
#[derive(Debug, StructOpt)]
struct Get {
    /// The name of the database
    db_name: String,
}

/// All possible subcommands for chunk
#[derive(Debug, StructOpt)]
enum Command {
    Get(Get),
}

pub async fn command(url: String, config: Config) -> Result<()> {
    let connection = Builder::default().build(url).await?;

    match config.command {
        Command::Get(get) => {
            let Get { db_name } = get;

            let mut client = management::Client::new(connection);

            let chunks = client
                .list_chunks(db_name)
                .await
                .map_err(Error::ListChunkError)?;

            let chunks = chunks
                .into_iter()
                .map(|c| ChunkSummary::try_from(c).map_err(Error::ConvertingResponse))
                .collect::<Result<Vec<_>>>()?;

            serde_json::to_writer_pretty(std::io::stdout(), &chunks).map_err(Error::WritingJson)?;
        }
    }

    Ok(())
}
