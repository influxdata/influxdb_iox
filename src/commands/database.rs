use generated_types::influxdata::iox::management::v1::{management_client::ManagementClient, DatabaseRules};
use snafu::Snafu;

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(context(false))]
    TransportError{source: tonic::transport::Error},

    #[snafu(context(false))]
    #[snafu(display("RequestError: {}: {}", source.code(), source.message()))]
    RequestError{source: tonic::Status}
}

pub type Result<T, E = Error> = std::result::Result<T, E>;

pub async fn create_database(url: String, name: String) -> Result<()> {
    let mut client = ManagementClient::connect(url).await?;

    client.create_database(DatabaseRules{
        name,
        ..Default::default()
    }).await?;

    Ok(())
}