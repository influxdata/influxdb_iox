use http::{uri::InvalidUri, Uri};
use std::convert::TryInto;
use std::time::Duration;
use thiserror::Error;
use tonic::transport::Endpoint;

/// The connection type used for clients
pub type Connection = tonic::transport::Channel;

/// The default User-Agent header sent by the HTTP client.
pub const USER_AGENT: &str = concat!(env!("CARGO_PKG_NAME"), "/", env!("CARGO_PKG_VERSION"));
/// The default connection timeout
pub const DEFAULT_CONNECT_TIMEOUT: Duration = Duration::from_secs(1);
/// The default request timeout
pub const DEFAULT_TIMEOUT: Duration = Duration::from_secs(30);

/// Errors returned by the ConnectionBuilder
#[derive(Debug, Error)]
pub enum Error {
    /// Server returned an invalid argument error
    #[error("Connection error: {}", .0)]
    TransportError(#[from] tonic::transport::Error),

    /// Client received an unexpected error from the server
    #[error("Invalid URI: {}", .0)]
    InvalidUri(#[from] InvalidUri),
}

/// Result type for the ConnectionBuilder
pub type Result<T, E = Error> = std::result::Result<T, E>;

/// A builder that produces a connection that can be used with any of the gRPC
/// clients
///
/// ```no_run
/// #[tokio::main]
/// # async fn main() {
/// use influxdb_iox_client::{connection::Builder, management::Client};
/// use std::time::Duration;
///
/// let connection = Builder::default()
///     .timeout(Duration::from_secs(42))
///     .user_agent("my_awesome_client")
///     .build("http://127.0.0.1:8082/")
///     .await
///     .expect("connection must succeed");
///
/// let client = Client::new(connection);
/// # }
/// ```
#[derive(Debug)]
pub struct Builder {
    user_agent: String,
    connect_timeout: Duration,
    timeout: Duration,
}

impl std::default::Default for Builder {
    fn default() -> Self {
        Self {
            user_agent: USER_AGENT.into(),
            connect_timeout: DEFAULT_CONNECT_TIMEOUT,
            timeout: DEFAULT_TIMEOUT,
        }
    }
}

impl Builder {
    /// Construct the [`Connection`] instance using the specified base URL.
    pub async fn build<D>(self, dst: D) -> Result<Connection>
    where
        D: TryInto<Uri, Error = InvalidUri>,
    {
        let endpoint = Endpoint::from(dst.try_into()?)
            .user_agent(self.user_agent)?
            .timeout(self.timeout);

        // Manually construct connector to workaround https://github.com/hyperium/tonic/issues/498
        let mut connector = hyper::client::HttpConnector::new();
        connector.set_connect_timeout(Some(self.connect_timeout));

        // Defaults from from tonic::channel::Endpoint
        connector.enforce_http(false);
        connector.set_nodelay(true);
        connector.set_keepalive(None);

        Ok(endpoint.connect_with_connector(connector).await?)
    }

    /// Set the `User-Agent` header sent by this client.
    pub fn user_agent(self, user_agent: impl Into<String>) -> Self {
        Self {
            user_agent: user_agent.into(),
            ..self
        }
    }

    /// Sets the maximum duration of time the client will wait for the IOx
    /// server to accept the TCP connection before aborting the request.
    ///
    /// Note this does not bound the request duration - see
    /// [`timeout`][Self::timeout].
    pub fn connect_timeout(self, timeout: Duration) -> Self {
        Self {
            connect_timeout: timeout,
            ..self
        }
    }

    /// Bounds the total amount of time a single client HTTP request take before
    /// being aborted.
    ///
    /// This timeout includes:
    ///
    ///  - Establishing the TCP connection (see [`connect_timeout`])
    ///  - Sending the HTTP request
    ///  - Waiting for, and receiving the entire HTTP response
    ///
    /// [`connect_timeout`]: Self::connect_timeout
    pub fn timeout(self, timeout: Duration) -> Self {
        Self { timeout, ..self }
    }
}
