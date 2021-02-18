use snafu::{ResultExt, Snafu};

use generated_types::google::protobuf::Empty;
use generated_types::influxdata::iox::management::v1::{
    management_client::ManagementClient, UpdateWriterIdRequest,
};

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("ID must be a non-zero, unsigned integer"))]
    InvalidWriterId { source: std::num::ParseIntError },

    #[snafu(context(false))]
    TransportError { source: tonic::transport::Error },

    #[snafu(context(false))]
    #[snafu(display("RequestError: {}: {}", source.code(), source.message()))]
    RequestError { source: tonic::Status },
}

pub type Result<T, E = Error> = std::result::Result<T, E>;

pub async fn ping(url: String) -> Result<()> {
    let mut client = ManagementClient::connect(url).await?;

    client
        .ping(Empty{})
        .await?;

    Ok(())
}

pub async fn set_writer_id(url: String, id: &str) -> Result<()> {
    let mut client = ManagementClient::connect(url).await?;

    let id: u32 = id.parse().context(InvalidWriterId)?;

    client
        .update_writer_id(UpdateWriterIdRequest { id })
        .await?;

    Ok(())
}

pub async fn get_writer_id(url: String) -> Result<u32> {
    let mut client = ManagementClient::connect(url).await?;

    let id = client.get_writer_id(Empty {}).await?;

    Ok(id.get_ref().id)
}
