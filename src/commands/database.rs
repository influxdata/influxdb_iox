use snafu::{OptionExt, Snafu};

use generated_types::google::protobuf::Empty;
use generated_types::influxdata::iox::management::v1::*;

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(context(false))]
    TransportError { source: tonic::transport::Error },

    #[snafu(context(false))]
    #[snafu(display("RequestError: {}: {}", source.code(), source.message()))]
    RequestError { source: tonic::Status },

    #[snafu(display("Missing field in response: {}", field))]
    MissingResponseField { field: String },
}

pub type Result<T, E = Error> = std::result::Result<T, E>;

pub async fn create_database(url: String, name: String) -> Result<()> {
    let mut client = management_client::ManagementClient::connect(url).await?;

    client
        .create_database(CreateDatabaseRequest {
            rules: Some(DatabaseRules {
                name,
                ..Default::default()
            }),
        })
        .await?;

    Ok(())
}

pub async fn list_databases(url: String) -> Result<Vec<String>> {
    let mut client = management_client::ManagementClient::connect(url).await?;

    let databases = client.list_databases(Empty {}).await?.into_inner().names;

    Ok(databases)
}

pub async fn get_database(url: String, name: String) -> Result<DatabaseRules> {
    let mut client = management_client::ManagementClient::connect(url).await?;

    let response = client.get_database(GetDatabaseRequest { name }).await?;

    response
        .into_inner()
        .rules
        .context(MissingResponseField { field: "rules" })
}
