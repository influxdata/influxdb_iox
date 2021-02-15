use tonic::{Request, Response, Status};

use generated_types::{
    google::protobuf::Empty,
    influxdata::iox::management::v1::{
        DatabaseRules,
        management_server::{Management, ManagementServer},
    }
};

struct ManagementService {}

#[tonic::async_trait]
impl Management for ManagementService
{
    async fn create_database(&self, _: Request<DatabaseRules>) -> Result<Response<Empty>, Status> {
        unimplemented!()
    }
}

pub fn make_server() -> ManagementServer<impl Management> {
    ManagementServer::new(ManagementService {})
}
