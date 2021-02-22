use std::num::NonZeroU32;

use thiserror::Error;

use self::generated_types::{management_service_client::ManagementServiceClient, *};

use crate::connection::Connection;

/// Re-export generated_types
pub mod generated_types {
    pub use generated_types::influxdata::iox::management::v1::*;
}

/// Errors returned by the management API
#[derive(Debug, Error)]
pub enum Error {
    /// Writer ID is not set
    #[error("Writer ID not set")]
    NoWriterId,

    /// Database already exists
    #[error("Database not found")]
    DatabaseNotFound,

    /// Database already exists
    #[error("Database already exists")]
    DatabaseAlreadyExists,

    /// Response contained no payload
    #[error("Server returned an empty response")]
    EmptyResponse,

    /// Server returned an invalid argument error
    #[error("Unexpected server error: {}: {}", .0.code(), .0.message())]
    InvalidArgument(tonic::Status),

    /// Client received an unexpected error from the server
    #[error("Unexpected server error: {}: {}", .0.code(), .0.message())]
    UnexpectedError(#[from] tonic::Status),
}

/// Result type for the management client
pub type Result<T, E = Error> = std::result::Result<T, E>;

/// An IOx Management API client.
///
/// ```no_run
/// #[tokio::main]
/// # async fn main() {
/// use influxdb_iox_client::{
///     management::{Client, generated_types::DatabaseRules},
///     connection::Builder,
/// };
///
/// let mut connection = Builder::default()
///     .build("http://127.0.0.1:8082")
///     .await
///     .unwrap();
///
/// let mut client = Client::new(connection);
///
/// // Create a new database!
/// client
///     .create_database(DatabaseRules{
///     name: "bananas".to_string(),
///     ..Default::default()
/// })
///     .await
///     .expect("failed to create database");
/// # }
/// ```
#[derive(Debug, Clone)]
pub struct Client {
    inner: ManagementServiceClient<Connection>,
}

impl Client {
    /// Creates a new client with the provided connection
    pub fn new(channel: tonic::transport::Channel) -> Self {
        Self {
            inner: ManagementServiceClient::new(channel),
        }
    }

    /// Set the server's writer ID.
    pub async fn update_writer_id(&mut self, id: NonZeroU32) -> Result<(), Error> {
        self.inner
            .update_writer_id(UpdateWriterIdRequest { id: id.into() })
            .await?;
        Ok(())
    }

    /// Get the server's writer ID.
    pub async fn get_writer_id(&mut self) -> Result<u32, Error> {
        let response = self
            .inner
            .get_writer_id(GetWriterIdRequest {})
            .await
            .map_err(|status| match status.code() {
                tonic::Code::NotFound => Error::NoWriterId,
                _ => Error::UnexpectedError(status),
            })?;
        Ok(response.get_ref().id)
    }

    /// Creates a new IOx database.
    pub async fn create_database(&mut self, rules: DatabaseRules) -> Result<(), Error> {
        self.inner
            .create_database(CreateDatabaseRequest { rules: Some(rules) })
            .await
            .map_err(|status| match status.code() {
                tonic::Code::AlreadyExists => Error::DatabaseAlreadyExists,
                tonic::Code::FailedPrecondition => Error::NoWriterId,
                tonic::Code::InvalidArgument => Error::InvalidArgument(status),
                _ => Error::UnexpectedError(status),
            })?;

        Ok(())
    }

    /// List databases.
    pub async fn list_databases(&mut self) -> Result<Vec<String>, Error> {
        let response = self.inner.list_databases(ListDatabasesRequest {}).await?;
        Ok(response.into_inner().names)
    }

    /// Get database configuration
    pub async fn get_database(&mut self, name: impl Into<String>) -> Result<DatabaseRules, Error> {
        let response = self
            .inner
            .get_database(GetDatabaseRequest { name: name.into() })
            .await
            .map_err(|status| match status.code() {
                tonic::Code::NotFound => Error::DatabaseNotFound,
                tonic::Code::FailedPrecondition => Error::NoWriterId,
                _ => Error::UnexpectedError(status),
            })?;

        let rules = response.into_inner().rules.ok_or(Error::EmptyResponse)?;
        Ok(rules)
    }
}
