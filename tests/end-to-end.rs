// The test in this file runs the server in a separate thread and makes HTTP requests as a smoke
// test for the integration of the whole system.
//
// As written, only one test of this style can run at a time. Add more data to the existing test to
// test more scenarios rather than adding more tests in the same style.
//
// Or, change the way this test behaves to create isolated instances by:
//
// - Finding an unused port for the server to run on and using that port in the URL
// - Creating a temporary directory for an isolated database path
//
// Or, change the tests to use one server and isolate through `org_id` by:
//
// - Starting one server before all the relevant tests are run
// - Creating a unique org_id per test
// - Stopping the server after all relevant tests are run

use assert_cmd::prelude::*;
use futures::prelude::*;
use prost::Message;
use std::convert::TryInto;
use std::env;
use std::process::{Child, Command, Stdio};
use std::str;
use std::time::{Duration, SystemTime};
use std::u32;
use tempfile::TempDir;

const URL_BASE: &str = "http://localhost:8080/api/v2";
const GRPC_URL_BASE: &str = "http://localhost:8082/";

mod grpc {
    tonic::include_proto!("influxdata.platform.storage");
}

use grpc::{
    delorean_client::DeloreanClient,
    node::{Comparison, Value},
    read_group_request::Group,
    read_response::{frame::Data, DataType},
    storage_client::StorageClient,
    Bucket, CreateBucketRequest, Node, Organization, Predicate, ReadFilterRequest,
    ReadGroupRequest, ReadSource, Tag, TagKeysRequest, TagValuesRequest, TimestampRange,
};

type Error = Box<dyn std::error::Error>;
type Result<T, E = Error> = std::result::Result<T, E>;

macro_rules! assert_unwrap {
    ($e:expr, $p:path) => {
        match $e {
            $p(v) => v,
            _ => panic!("{} was not a {}", stringify!($e), stringify!($p)),
        }
    };
    ($e:expr, $p:path, $extra:tt) => {
        match $e {
            $p(v) => v,
            _ => {
                let extra = format_args!($extra);
                panic!("{} was not a {}: {}", stringify!($e), stringify!($p), extra);
            }
        }
    };
}

async fn read_data(
    client: &reqwest::Client,
    path: &str,
    org_id: &str,
    bucket_id: &str,
    predicate: &str,
    seconds_ago: u64,
) -> Result<String> {
    let url = format!("{}{}", URL_BASE, path);
    Ok(client
        .get(&url)
        .query(&[
            ("bucket", bucket_id),
            ("org", org_id),
            ("predicate", predicate),
            ("start", &format!("-{}s", seconds_ago)),
        ])
        .send()
        .await?
        .error_for_status()?
        .text()
        .await?)
}

async fn write_data(
    client: &reqwest::Client,
    path: &str,
    org_id: &str,
    bucket_id: &str,
    body: String,
) -> Result<()> {
    let url = format!("{}{}", URL_BASE, path);
    client
        .post(&url)
        .query(&[("bucket", bucket_id), ("org", org_id)])
        .body(body)
        .send()
        .await?
        .error_for_status()?;
    Ok(())
}

#[tokio::test]
async fn read_and_write_data() -> Result<()> {
    let _ = dotenv::dotenv(); // load .env file if present

    let root = env::var_os("TEST_DELOREAN_DB_DIR").unwrap_or_else(|| env::temp_dir().into());

    // The temporary directory **must** be first so that it is
    // dropped after the database closes.
    let dir = tempfile::Builder::new()
        .prefix("delorean")
        .tempdir_in(root)?;

    let server = TestServer::new(&dir)?;
    server.wait_until_ready().await;

    let org_id_str = "0000111100001111";
    let org_id = u64::from_str_radix(org_id_str, 16).unwrap();
    let bucket_id_str = "1111000011110000";
    let bucket_id = u64::from_str_radix(bucket_id_str, 16).unwrap();

    let client = reqwest::Client::new();
    let mut grpc_client = DeloreanClient::connect(GRPC_URL_BASE).await?;

    let get_buckets_request = tonic::Request::new(Organization {
        id: org_id,
        name: "test".into(),
        buckets: vec![],
    });
    let get_buckets_response = grpc_client.get_buckets(get_buckets_request).await?;
    let get_buckets_response = get_buckets_response.into_inner();
    let org_buckets = get_buckets_response.buckets;

    // This checks that gRPC is functioning and that we're starting from an org without buckets.
    assert!(org_buckets.is_empty());

    let create_bucket_request = tonic::Request::new(CreateBucketRequest {
        org_id,
        bucket: Some(Bucket {
            org_id,
            id: 0,
            name: bucket_id_str.to_string(),
            retention: "0".to_string(),
            posting_list_rollover: 10_000,
            index_levels: vec![],
        }),
    });
    grpc_client.create_bucket(create_bucket_request).await?;

    let start_time = SystemTime::now();
    let ns_since_epoch: i64 = start_time
        .duration_since(SystemTime::UNIX_EPOCH)
        .expect("System time should have been after the epoch")
        .as_nanos()
        .try_into()
        .expect("Unable to represent system time");

    // TODO: make a more extensible way to manage data for tests, such as in external fixture
    // files or with factories.
    write_data(
        &client,
        "/write",
        org_id_str,
        bucket_id_str,
        format!(
            "\
cpu_load_short,host=server01,region=us-west value=0.64 {}
cpu_load_short,host=server01 value=27.99 {}
cpu_load_short,host=server02,region=us-west value=3.89 {}
cpu_load_short,host=server01,region=us-east value=1234567.891011 {}
cpu_load_short,host=server01,region=us-west value=0.000003 {}",
            ns_since_epoch,
            ns_since_epoch + 1,
            ns_since_epoch + 2,
            ns_since_epoch + 3,
            ns_since_epoch + 4,
        ),
    )
    .await?;

    let end_time = SystemTime::now();
    let duration = end_time
        .duration_since(start_time)
        .expect("End time should have been after start time");
    let seconds_ago = duration.as_secs() + 1;

    let text = read_data(
        &client,
        "/read",
        org_id_str,
        bucket_id_str,
        r#"host="server01""#,
        seconds_ago,
    )
    .await?;

    // TODO: make a more sustainable way to manage expected data for tests, such as using the
    // insta crate to manage snapshots.
    assert_eq!(
        text,
        format!(
            "\
_m,host,region,_f,_time,_value
cpu_load_short,server01,us-west,value,{},0.64
cpu_load_short,server01,us-west,value,{},0.000003

_m,host,_f,_time,_value
cpu_load_short,server01,value,{},27.99

_m,host,region,_f,_time,_value
cpu_load_short,server01,us-east,value,{},1234567.891011

",
            ns_since_epoch,
            ns_since_epoch + 4,
            ns_since_epoch + 1,
            ns_since_epoch + 3,
        )
    );

    // Test the WAL by killing the server
    std::mem::drop(server);

    // Then restart and check that the entries are restored from the WAL
    let server = TestServer::new(&dir)?;
    server.wait_until_ready().await;

    let end_time = SystemTime::now();
    let duration = end_time
        .duration_since(start_time)
        .expect("End time should have been after start time");
    let seconds_ago = duration.as_secs() + 1;

    let text = read_data(
        &client,
        "/read",
        org_id_str,
        bucket_id_str,
        r#"host="server01""#,
        seconds_ago,
    )
    .await?;

    // TODO: make a more sustainable way to manage expected data for tests, such as using the
    // insta crate to manage snapshots.
    assert_eq!(
        text,
        format!(
            "\
_m,host,region,_f,_time,_value
cpu_load_short,server01,us-west,value,{},0.64
cpu_load_short,server01,us-west,value,{},0.000003

_m,host,_f,_time,_value
cpu_load_short,server01,value,{},27.99

_m,host,region,_f,_time,_value
cpu_load_short,server01,us-east,value,{},1234567.891011

",
            ns_since_epoch,
            ns_since_epoch + 4,
            ns_since_epoch + 1,
            ns_since_epoch + 3,
        )
    );

    let mut storage_client = StorageClient::connect(GRPC_URL_BASE).await?;

    let partition_id = u64::from(u32::MAX);
    let read_source = ReadSource {
        org_id,
        bucket_id,
        partition_id,
    };
    let mut d = Vec::new();
    read_source.encode(&mut d)?;
    let read_source = prost_types::Any {
        type_url: "/TODO".to_string(),
        value: d,
    };
    let read_source = Some(read_source);

    let range = TimestampRange {
        start: ns_since_epoch,
        end: ns_since_epoch + 10,
    };
    let range = Some(range);

    let predicate = Predicate {
        root: Some(Node {
            children: vec![
                Node {
                    children: vec![],
                    value: Some(Value::TagRefValue("host".into())),
                },
                Node {
                    children: vec![],
                    value: Some(Value::StringValue("server01".into())),
                },
            ],
            value: Some(Value::Comparison(Comparison::Equal as _)),
        }),
    };
    let predicate = Some(predicate);

    let read_filter_request = tonic::Request::new(ReadFilterRequest {
        read_source: read_source.clone(),
        range: range.clone(),
        predicate: predicate.clone(),
    });
    let read_response = storage_client.read_filter(read_filter_request).await?;

    let responses: Vec<_> = read_response.into_inner().try_collect().await?;
    let frames: Vec<_> = responses
        .into_iter()
        .flat_map(|r| r.frames)
        .flat_map(|f| f.data)
        .collect();

    assert_eq!(frames.len(), 6);

    let f = assert_unwrap!(&frames[0], Data::Series, "in frame 0");
    assert_eq!(f.data_type, DataType::Float as i32, "in frame 0");
    assert_eq!(
        tags_as_strings(&f.tags),
        vec![
            ("_field", "value"),
            ("_measurement", "cpu_load_short"),
            ("host", "server01"),
            ("region", "us-west"),
        ],
        "in frame 0",
    );

    let f = assert_unwrap!(&frames[1], Data::FloatPoints, "in frame 1");
    assert_eq!(
        f.timestamps,
        [ns_since_epoch, ns_since_epoch + 4],
        "in frame 1"
    );
    assert_eq!(f.values, [0.64, 0.000_003], "in frame 1");

    let f = assert_unwrap!(&frames[2], Data::Series, "in frame 2");
    assert_eq!(f.data_type, DataType::Float as i32, "in frame 2");
    assert_eq!(
        tags_as_strings(&f.tags),
        vec![
            ("_field", "value"),
            ("_measurement", "cpu_load_short"),
            ("host", "server01"),
        ],
        "in frame 2",
    );

    let f = assert_unwrap!(&frames[3], Data::FloatPoints, "in frame 3");
    assert_eq!(f.timestamps, [ns_since_epoch + 1], "in frame 3");
    assert_eq!(f.values, [27.99], "in frame 3");

    let f = assert_unwrap!(&frames[4], Data::Series, "in frame 4");
    assert_eq!(f.data_type, DataType::Float as i32, "in frame 4");
    assert_eq!(
        tags_as_strings(&f.tags),
        vec![
            ("_field", "value"),
            ("_measurement", "cpu_load_short"),
            ("host", "server01"),
            ("region", "us-east"),
        ],
        "in frame 4",
    );

    let f = assert_unwrap!(&frames[5], Data::FloatPoints, "in frame 5");
    assert_eq!(f.timestamps, [ns_since_epoch + 3], "in frame 5");
    assert_eq!(f.values, [1_234_567.891_011], "in frame 5");

    let tag_keys_request = tonic::Request::new(TagKeysRequest {
        tags_source: read_source.clone(),
        range: range.clone(),
        predicate: predicate.clone(),
    });

    let tag_keys_response = storage_client.tag_keys(tag_keys_request).await?;
    let responses: Vec<_> = tag_keys_response.into_inner().try_collect().await?;

    let keys = &responses[0].values;
    let keys: Vec<_> = keys.iter().map(|s| str::from_utf8(s).unwrap()).collect();

    assert_eq!(keys, vec!["_field", "_measurement", "host", "region"]);

    let tag_values_request = tonic::Request::new(TagValuesRequest {
        tags_source: read_source.clone(),
        range: range.clone(),
        predicate: predicate.clone(),
        tag_key: String::from("host"),
    });

    let tag_values_response = storage_client.tag_values(tag_values_request).await?;
    let responses: Vec<_> = tag_values_response.into_inner().try_collect().await?;

    let values = &responses[0].values;
    let values: Vec<_> = values.iter().map(|s| str::from_utf8(s).unwrap()).collect();

    assert_eq!(values, vec!["server01", "server02"]);

    let read_group_request = tonic::Request::new(ReadGroupRequest {
        read_source,
        range,
        predicate,
        group_keys: vec![String::from("region")],
        group: Group::By as _,
        aggregate: None,
    });
    let read_group_response = storage_client.read_group(read_group_request).await?;

    let responses: Vec<_> = read_group_response.into_inner().try_collect().await?;

    let frames: Vec<_> = responses
        .into_iter()
        .flat_map(|r| r.frames)
        .flat_map(|f| f.data)
        .collect();

    assert_eq!(frames.len(), 6);

    let f = assert_unwrap!(&frames[0], Data::Group, "in frame 0");
    assert_eq!(
        byte_vecs_to_strings(&f.tag_keys),
        vec!["_field", "_measurement", "host", "region"]
    );
    let partition_vals = byte_vecs_to_strings(&f.partition_key_vals);
    assert_eq!(partition_vals, vec!["us-east"], "in frame 0");

    let f = assert_unwrap!(&frames[1], Data::FloatPoints, "in frame 1");
    assert_eq!(f.timestamps, [ns_since_epoch + 3], "in frame 1");
    assert_eq!(f.values, [1_234_567.891_011], "in frame 1");

    let f = assert_unwrap!(&frames[2], Data::Group, "in frame 2");
    assert_eq!(
        byte_vecs_to_strings(&f.tag_keys),
        vec!["_field", "_measurement", "host", "region"]
    );
    let partition_vals = byte_vecs_to_strings(&f.partition_key_vals);
    assert_eq!(partition_vals, vec!["us-west"], "in frame 2");

    let f = assert_unwrap!(&frames[3], Data::FloatPoints, "in frame 3");
    assert_eq!(
        f.timestamps,
        [ns_since_epoch, ns_since_epoch + 4],
        "in frame 3"
    );
    assert_eq!(f.values, [0.64, 0.000_003], "in frame 3");

    let f = assert_unwrap!(&frames[4], Data::Group, "in frame 4");
    assert_eq!(
        byte_vecs_to_strings(&f.tag_keys),
        vec!["_field", "_measurement", "host"]
    );
    assert_eq!(f.partition_key_vals.len(), 1, "in frame 4");
    assert!(f.partition_key_vals[0].is_empty(), "in frame 4");

    let f = assert_unwrap!(&frames[5], Data::FloatPoints, "in frame 5");
    assert_eq!(f.timestamps, [ns_since_epoch + 1], "in frame 5");
    assert_eq!(f.values, [27.99], "in frame 5");

    Ok(())
}

fn tags_as_strings(tags: &[Tag]) -> Vec<(&str, &str)> {
    tags.iter()
        .map(|t| {
            (
                str::from_utf8(&t.key).unwrap(),
                str::from_utf8(&t.value).unwrap(),
            )
        })
        .collect()
}

struct TestServer {
    server_process: Child,
}

impl TestServer {
    fn new(dir: &TempDir) -> Result<Self> {
        let server_process = Command::cargo_bin("delorean")?
            .stdout(Stdio::null())
            .env("DELOREAN_DB_DIR", dir.path())
            .spawn()?;

        Ok(Self { server_process })
    }

    async fn wait_until_ready(&self) {
        // TODO: poll the server to see if it's ready instead of sleeping
        tokio::time::delay_for(Duration::from_secs(3)).await;
    }
}

impl Drop for TestServer {
    fn drop(&mut self) {
        self.server_process
            .kill()
            .expect("Should have been able to kill the test server");
    }
}

fn byte_vecs_to_strings(v: &[Vec<u8>]) -> Vec<&str> {
    v.iter().map(|i| str::from_utf8(i).unwrap()).collect()
}
