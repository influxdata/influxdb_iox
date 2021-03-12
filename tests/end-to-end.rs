// The test in this file runs the server in a separate thread and makes HTTP
// requests as a smoke test for the integration of the whole system.
//
// The servers under test are managed using [`ServerFixture`]
//
// Other rust tests are defined in the various submodules of end_to_end_cases

use end_to_end_cases::*;
use generated_types::storage_client::StorageClient;

pub mod common;

mod end_to_end_cases;

use end_to_end_cases::scenario::Scenario;

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
        let scenario = Scenario::default()
            .set_org_id("0000111100001111")
            .set_bucket_id("1111000011110000");

        Scenario::create_database(&mut management_client, &scenario.database_name()).await;

        let expected_read_data = scenario.load_data(&influxdb2).await;
        let sql_query = "select * from cpu_load_short";

        read_api::test(&fixture, &scenario, sql_query, &expected_read_data).await;
        storage_api::test(&mut storage_client, &scenario).await;
        flight_api::test(&fixture, &scenario, sql_query, &expected_read_data).await;
    }
}

#[tokio::test]
async fn test_http_error_messages() {
    let server_fixture = ServerFixture::create_shared().await;
    let client = server_fixture.influxdb2_client();

    // send malformed request (bucket id is invalid)
    let result = client
        .write_line_protocol("Bar", "Foo", "arbitrary")
        .await
        .expect_err("Should have errored");

    let expected_error = "HTTP request returned an error: 400 Bad Request, `{\"error\":\"Error parsing line protocol: A generic parsing error occurred: TakeWhile1\",\"error_code\":100}`";
    assert_eq!(result.to_string(), expected_error);
}
