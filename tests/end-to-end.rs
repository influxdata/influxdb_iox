// The test in this file runs the server in a separate thread and makes HTTP
// requests as a smoke test for the integration of the whole system.
//
// The servers under test are managed using [`ServerFixture`]
//
// Other rust tests are defined in the various submodules of end_to_end_cases

pub mod common;
mod end_to_end_cases;

use end_to_end_cases::{scenario::Scenario, *};
use generated_types::storage_client::StorageClient;

use common::server_fixture::*;

#[tokio::test]
async fn read_and_write_data() {
    let fixture = ServerFixture::create_shared().await;

    let influxdb2 = fixture.influxdb2_client();
    let mut storage_client = StorageClient::new(fixture.grpc_channel());
    let mut management_client =
        influxdb_iox_client::management::Client::new(fixture.grpc_channel());

    // These tests share data; TODO: a better way to indicate this
    {
        let scenario = Scenario::new();
        scenario.create_database(&mut management_client).await;

        let expected_read_data = scenario.load_data(&influxdb2).await;
        let sql_query = "select * from cpu_load_short";

        storage_api::test(&mut storage_client, &scenario).await;
        flight_api::test(&fixture, &scenario, sql_query, &expected_read_data).await;
    }
}
