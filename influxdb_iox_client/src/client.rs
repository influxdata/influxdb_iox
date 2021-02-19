use std::num::NonZeroU32;

use thiserror::Error;

// can't combine these into one statement that uses `{}` because of this bug in
// the `unreachable_pub` lint: https://github.com/rust-lang/rust/issues/64762
#[cfg(feature = "flight")]
pub use flight::FlightClient;
#[cfg(feature = "flight")]
pub use flight::PerformQuery;

use generated_types::google::protobuf::Empty;
use generated_types::influxdata::iox::management::v1::*;

#[cfg(feature = "flight")]
mod flight;

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

/// An IOx HTTP API client.
///
/// ```no_run
/// #[tokio::main]
/// # async fn main() {
/// use influxdb_iox_client::{ClientBuilder, generated_types::DatabaseRules};
///
/// let mut client = ClientBuilder::default()
///     .build("http://127.0.0.1:8082")
///     .await
///     .unwrap();
///
/// // Ping the IOx server
/// client.ping().await.expect("server is down :(");
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
    pub(crate) client: management_client::ManagementClient<tonic::transport::Channel>,
}

impl Client {
    /// Ping the IOx server, checking for a HTTP 200 response.
    pub async fn ping(&mut self) -> Result<(), Error> {
        self.client.ping(Empty {}).await?;
        Ok(())
    }

    /// Set the server's writer ID.
    pub async fn update_writer_id(&mut self, id: NonZeroU32) -> Result<(), Error> {
        self.client
            .update_writer_id(UpdateWriterIdRequest { id: id.into() })
            .await?;
        Ok(())
    }

    /// Get the server's writer ID.
    pub async fn get_writer_id(&mut self) -> Result<u32, Error> {
        let response = self
            .client
            .get_writer_id(Empty {})
            .await
            .map_err(|status| match status.code() {
                tonic::Code::NotFound => Error::NoWriterId,
                _ => Error::UnexpectedError(status),
            })?;
        Ok(response.get_ref().id)
    }

    /// Creates a new IOx database.
    pub async fn create_database(&mut self, rules: DatabaseRules) -> Result<(), Error> {
        self.client
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
        let response = self.client.list_databases(Empty {}).await?;
        Ok(response.into_inner().names)
    }

    /// Get database configuration
    pub async fn get_database(&mut self, name: impl Into<String>) -> Result<DatabaseRules, Error> {
        let response = self
            .client
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

#[cfg(test)]
mod tests {
    use rand::{distributions::Alphanumeric, thread_rng, Rng};

    use crate::ClientBuilder;

    use super::*;

    /// If `TEST_IOX_ENDPOINT` is set, load the value and return it to the
    /// caller.
    ///
    /// If `TEST_IOX_ENDPOINT` is not set, skip the calling test by returning
    /// early. Additionally if `TEST_INTEGRATION` is set, turn this early return
    /// into a panic to force a hard fail for skipped integration tests.
    macro_rules! maybe_skip_integration {
        () => {
            match (
                std::env::var("TEST_IOX_ENDPOINT").is_ok(),
                std::env::var("TEST_INTEGRATION").is_ok(),
            ) {
                (true, _) => std::env::var("TEST_IOX_ENDPOINT").unwrap(),
                (false, true) => {
                    panic!("TEST_INTEGRATION is set which requires running integration tests, but TEST_IOX_ENDPOINT is not")
                }
                _ => {
                    eprintln!("skipping integration test - set TEST_IOX_ENDPOINT to run");
                    return;
                }
            }
        };
    }

    #[tokio::test]
    async fn test_ping() {
        let endpoint = maybe_skip_integration!();
        let mut c = ClientBuilder::default().build(endpoint).await.unwrap();
        c.ping().await.expect("ping failed");
    }

    #[tokio::test]
    async fn test_set_get_writer_id() {
        const TEST_ID: u32 = 42;

        let endpoint = maybe_skip_integration!();
        let mut c = ClientBuilder::default().build(endpoint).await.unwrap();

        c.update_writer_id(NonZeroU32::new(TEST_ID).unwrap())
            .await
            .expect("set ID failed");

        let got = c.get_writer_id().await.expect("get ID failed");

        assert_eq!(got, TEST_ID);
    }

    #[tokio::test]
    async fn test_create_database() {
        let endpoint = maybe_skip_integration!();
        let mut c = ClientBuilder::default().build(endpoint).await.unwrap();

        c.update_writer_id(NonZeroU32::new(42).unwrap())
            .await
            .expect("set ID failed");

        c.create_database(DatabaseRules {
            name: rand_name(),
            ..Default::default()
        })
        .await
        .expect("create database failed");
    }

    #[tokio::test]
    async fn test_create_database_duplicate_name() {
        let endpoint = maybe_skip_integration!();
        let mut c = ClientBuilder::default().build(endpoint).await.unwrap();

        c.update_writer_id(NonZeroU32::new(42).unwrap())
            .await
            .expect("set ID failed");

        let db_name = rand_name();

        c.create_database(DatabaseRules {
            name: db_name.clone(),
            ..Default::default()
        })
        .await
        .expect("create database failed");

        let err = c
            .create_database(DatabaseRules {
                name: db_name,
                ..Default::default()
            })
            .await
            .expect_err("create database failed");

        assert!(matches!(dbg!(err), Error::DatabaseAlreadyExists))
    }

    #[tokio::test]
    async fn test_create_database_invalid_name() {
        let endpoint = maybe_skip_integration!();
        let mut c = ClientBuilder::default().build(endpoint).await.unwrap();

        c.update_writer_id(NonZeroU32::new(42).unwrap())
            .await
            .expect("set ID failed");

        let err = c
            .create_database(DatabaseRules {
                name: "my_example\ndb".to_string(),
                ..Default::default()
            })
            .await
            .expect_err("expected request to fail");

        assert!(matches!(dbg!(err), Error::InvalidArgument(_)));
    }

    #[tokio::test]
    async fn test_list_databases() {
        let endpoint = maybe_skip_integration!();
        let mut c = ClientBuilder::default().build(endpoint).await.unwrap();

        c.update_writer_id(NonZeroU32::new(42).unwrap())
            .await
            .expect("set ID failed");

        let name = rand_name();
        c.create_database(DatabaseRules {
            name: name.clone(),
            ..Default::default()
        })
        .await
        .expect("create database failed");
        let names = c.list_databases().await.expect("list databases failed");
        assert!(names.contains(&name));
    }

    fn rand_name() -> String {
        thread_rng()
            .sample_iter(&Alphanumeric)
            .take(10)
            .map(char::from)
            .collect()
    }
}
