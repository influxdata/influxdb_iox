use generated_types::i_ox_testing_server::{IOxTesting, IOxTestingServer};
use generated_types::{TestErrorRequest, TestErrorResponse};
use tracing::warn;

struct IOxTestingService {}

#[tonic::async_trait]
/// Implements the protobuf defined IOx testing service for IOxTestingService
impl IOxTesting for IOxTestingService {
    async fn test_error(
        &self,
        _req: tonic::Request<TestErrorRequest>,
    ) -> Result<tonic::Response<TestErrorResponse>, tonic::Status> {
        warn!("Got a test_error request. About to panic");
        panic!("This is a test panic");
    }
}

pub fn make_server() -> IOxTestingServer<impl IOxTesting> {
    IOxTestingServer::new(IOxTestingService {})
}
