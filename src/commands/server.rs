//! Implementation of command line option for manipulating and showing server
//! config

use crate::commands::server_remote;
use structopt::StructOpt;
use thiserror::Error;

#[derive(Debug, Error)]
pub enum Error {
    #[error("Remote: {0}")]
    RemoteError(#[from] server_remote::Error),

    #[error("Error getting server ID: {0}")]
    GetServerIdError(#[from] GetServerIdError),

    #[error("Error updating server ID: {0}")]
    UpdateServerIdError(#[from] UpdateServerIdError),

    #[error("Error connecting to IOx: {0}")]
    ConnectionError(#[from] influxdb_iox_client::connection::Error),
}

pub type Result<T, E = Error> = std::result::Result<T, E>;

#[derive(Debug, StructOpt)]
#[structopt(name = "server", about = "IOx server commands")]
pub struct Config {
    #[structopt(subcommand)]
    command: Command,
}

#[derive(Debug, StructOpt)]
enum Command {
    /// Set server ID
    Set(Set),

    /// Get server ID
    Get,

    Remote(crate::commands::server_remote::Config),
}

/// Set server ID
#[derive(Debug, StructOpt)]
struct Set {
    /// The server ID to set
    id: u32,
}

use influxdb_iox_client::{connection::Builder, management::*};

pub async fn command(url: String, config: Config) -> Result<()> {
    let connection = Builder::default().build(&url).await?;
    let mut client = Client::new(connection);

    match config.command {
        Command::Set(command) => {
            client.update_server_id(command.id).await?;
            println!("Ok");
            Ok(())
        }
        Command::Get => {
            let id = client.get_server_id().await?;
            println!("{}", id);
            Ok(())
        }
        Command::Remote(config) => Ok(server_remote::command(url, config).await?),
    }
}
