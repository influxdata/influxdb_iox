use tonic::{Request, Response, Status};

use data_types::database_rules::DatabaseRules;
use generated_types::{
    google::protobuf::Empty,
    influxdata::iox::management::v1::{
        self as pb,
        management_server::{Management, ManagementServer},
    },
};
use std::convert::TryInto;
use generated_types::google::{AlreadyExists, InternalError, PreconditionViolation};
use server::{Server, ConnectionManager, Error};
use std::sync::Arc;
use std::fmt::Debug;
use data_types::DatabaseName;
use tracing::error;

fn id_precondition() -> PreconditionViolation {
    PreconditionViolation{
        category: "Writer ID".to_string(),
        subject: "influxdata.com/iox".to_string(),
        description: "Writer ID must be set".to_string()
    }
}

struct ManagementService<M: ConnectionManager> {
    server: Arc<Server<M>>
}

#[tonic::async_trait]
impl <M> Management for ManagementService<M> where
    M: ConnectionManager + Send + Sync + Debug + 'static, {
    async fn create_database(
        &self,
        req: Request<pb::DatabaseRules>,
    ) -> Result<Response<Empty>, Status> {
        let rules: DatabaseRules = req.into_inner().try_into()?;
        let name = DatabaseName::new(rules.name.clone()).expect("protobuf mapping didn't validate name");

        match self.server.create_database(name, rules).await {
            Ok(_) => Ok(Response::new(Empty{})),
            Err(Error::DatabaseAlreadyExists { db_name }) => Err(AlreadyExists{
                resource_type: "database".to_string(),
                resource_name: db_name,
                ..Default::default()
            })?,
            Err(Error::IdNotSet) => Err(id_precondition())?,
            Err(error) => {
                error!(?error, "Unexpected error creating database");
                Err(InternalError{})?
            }
        }
    }
}

pub fn make_server<M>(server: Arc<Server<M>>) -> ManagementServer<impl Management> where
    M: ConnectionManager + Send + Sync + Debug + 'static,{
    ManagementServer::new(ManagementService { server })
}
