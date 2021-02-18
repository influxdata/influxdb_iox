use std::time::Duration;

use crate::Client;

#[cfg(feature = "flight")]
use crate::FlightClient;
use generated_types::influxdata::iox::management::v1::management_client::ManagementClient;
use tonic::transport::Endpoint;

/// The default User-Agent header sent by the HTTP client.
pub const USER_AGENT: &str = concat!(env!("CARGO_PKG_NAME"), "/", env!("CARGO_PKG_VERSION"));

/// Configure and construct a new [`Client`] instance for using the IOx HTTP
/// API.
///
/// ```
/// # use influxdb_iox_client::ClientBuilder;
/// use std::time::Duration;
///
/// let c = ClientBuilder::default()
///     .timeout(Duration::from_secs(42))
///     .user_agent("my_awesome_client")
///     .build("http://127.0.0.1:8082/");
/// ```
#[derive(Debug)]
pub struct ClientBuilder {
    user_agent: String,
    connect_timeout: Duration,
    timeout: Duration,
}

impl std::default::Default for ClientBuilder {
    fn default() -> Self {
        Self {
            user_agent: USER_AGENT.into(),
            connect_timeout: Duration::from_secs(1),
            timeout: Duration::from_secs(30),
        }
    }
}

impl ClientBuilder {
    /// Construct the [`Client`] instance using the specified base URL.
    pub async fn build<T>(self, dst: T) -> Result<Client, Box<dyn std::error::Error>>
    where
        T: Into<String>,
    {
        let endpoint = Endpoint::from_shared(dst.into())?
            .user_agent(self.user_agent)?
            .timeout(self.timeout);

        // Manually construct connector to workaround https://github.com/hyperium/tonic/issues/498
        let mut connector = hyper::client::HttpConnector::new();
        connector.set_connect_timeout(Some(self.connect_timeout));

        // Defaults from from tonic::channel::Endpoint
        connector.enforce_http(false);
        connector.set_nodelay(true);
        connector.set_keepalive(None);

        let channel = endpoint.connect_with_connector(connector).await?;

        let client = ManagementClient::new(channel);
        Ok(Client { client })
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

#[cfg(feature = "flight")]
#[derive(Debug)]
/// Configure and construct a new [`FlightClient`] instance for using the IOx
/// Arrow Flight API.
///
/// ```
/// # use influxdb_iox_client::FlightClientBuilder;
///
/// let c = FlightClientBuilder::default()
///     .build("http://127.0.0.1:8080/");
/// ```
pub struct FlightClientBuilder {}

#[cfg(feature = "flight")]
impl Default for FlightClientBuilder {
    fn default() -> Self {
        Self {}
    }
}

#[cfg(feature = "flight")]
impl FlightClientBuilder {
    /// Construct the [`FlightClient`] instance using the specified URL to the
    /// server and port where the Arrow Flight API is available.
    pub async fn build<T>(self, flight_url: T) -> Result<FlightClient, Box<dyn std::error::Error>>
    where
        T: std::convert::TryInto<tonic::transport::Endpoint>,
        T::Error: Into<tonic::codegen::StdError>,
    {
        Ok(FlightClient::connect(flight_url).await.map_err(Box::new)?)
    }
}
