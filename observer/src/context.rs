//! Command context (aka the state passed to each command)
use arrow_deps::datafusion::prelude::{ExecutionConfig, ExecutionContext};
use influxdb_iox_client::connection::Connection;

pub struct Context {
    /// DataFusion execution context
    pub ctx: ExecutionContext,
    /// The connection to the remote server
    pub connection: Connection,
    /// A preconfigured management client
    pub management_client: influxdb_iox_client::management::Client,
}

impl Context {
    pub fn new(connection: Connection) -> Self {
        let ctx =
            ExecutionContext::with_config(ExecutionConfig::new().with_information_schema(true));

        let management_client = influxdb_iox_client::management::Client::new(connection.clone());

        Self {
            ctx,
            connection,
            management_client,
        }
    }
}
