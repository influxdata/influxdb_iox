//! This module contains gRPC service implementatations for the WriteBuffer
//! storage implementation

// Something in instrument is causing lint warnings about unused braces
#![allow(unused_braces)]

// Something about how `tracing::instrument` works triggers a clippy
// warning about complex types
#![allow(clippy::type_complexity)]

use arrow::{array::UInt64Array, datatypes::DataType, record_batch::RecordBatch};
use std::{collections::HashMap, net::SocketAddr, sync::Arc};
use tracing::warn;

use snafu::{ResultExt, Snafu};

use delorean::generated_types::{
    delorean_server::{Delorean, DeloreanServer},
    storage_server::{Storage, StorageServer},
    CapabilitiesResponse, CreateBucketRequest, CreateBucketResponse, DeleteBucketRequest,
    DeleteBucketResponse, GetBucketsResponse, MeasurementFieldsRequest, MeasurementFieldsResponse,
    MeasurementNamesRequest, MeasurementTagKeysRequest, MeasurementTagValuesRequest, Organization,
    ReadFilterRequest, ReadGroupRequest, ReadResponse, StringValuesResponse, TagKeysRequest,
    TagValuesRequest,
};

use crate::server::rpc::input::GrpcInputs;
use delorean::storage::{org_and_bucket_to_database, Database, DatabaseStore};

use tokio::{sync::mpsc, task::JoinHandle};
use tonic::Status;

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("gRPC server error:  {}", source))]
    ServerError { source: tonic::transport::Error },

    #[snafu(display("Database not found: {}", db_name))]
    DatabaseNotFound { db_name: String },

    #[snafu(display("Joining task: {}", source))]
    JoinError { source: tokio::task::JoinError },

    #[snafu(display("Can not retrieve table list for '{}': {}", db_name, source))]
    GettingTables {
        db_name: String,
        source: Box<dyn std::error::Error + Send + Sync>,
    },

    // TODO add db_name
    #[snafu(display("Can not execute metadata query for '{}': {}", table_name, source))]
    ExecutingMetadataQuery {
        table_name: String,
        source: Box<dyn std::error::Error + Send + Sync>,
    },

    /// Something already created an existing tonic Status which we
    /// need to pass back
    #[snafu(display("TonicWrappper: {}", status))]
    TonicWrapper { status: tonic::Status },

    #[snafu(display("Operation not yet implemented:  {}", operation))]
    NotYetImplemented { operation: String },

    // generic errors that "should never happen"
    #[snafu(display("Internal Error:  {}", description))]
    InternalError { description: String },
}

pub type Result<T, E = Error> = std::result::Result<T, E>;

impl Error {
    /// Converts a result from the business logic into the appropriate tonic status
    fn to_status(&self) -> tonic::Status {
        match &self {
            Self::ServerError { .. } => Status::internal(self.to_string()),
            Self::DatabaseNotFound { .. } => Status::not_found(self.to_string()),
            Self::JoinError { .. } => Status::internal(self.to_string()),
            Self::GettingTables { .. } => Status::internal(self.to_string()),
            Self::ExecutingMetadataQuery { .. } => Status::internal(self.to_string()),
            Self::TonicWrapper { status } => status.clone(),
            Self::NotYetImplemented { .. } => Status::internal(self.to_string()),
            Self::InternalError { .. } => Status::internal(self.to_string()),
        }
    }
}

impl From<tonic::Status> for Error {
    fn from(status: Status) -> Self {
        Self::TonicWrapper { status }
    }
}

#[derive(Debug)]
pub struct GrpcService<T: DatabaseStore> {
    db_store: Arc<T>,
}

impl<T> GrpcService<T>
where
    T: DatabaseStore + 'static,
{
    /// Create a new GrpcService connected to `db_store`
    pub fn new(db_store: Arc<T>) -> Self {
        Self { db_store }
    }
}

#[tonic::async_trait]
/// Implements the protobuf defined Delorean rpc service for a DatabaseStore
impl<T> Delorean for GrpcService<T>
where
    T: DatabaseStore + 'static,
{
    // TODO: Do we want to keep this gRPC request?
    async fn create_bucket(
        &self,
        _req: tonic::Request<CreateBucketRequest>,
    ) -> Result<tonic::Response<CreateBucketResponse>, Status> {
        Err(Status::unimplemented("create_bucket"))
    }

    async fn delete_bucket(
        &self,
        _req: tonic::Request<DeleteBucketRequest>,
    ) -> Result<tonic::Response<DeleteBucketResponse>, Status> {
        Err(Status::unimplemented("delete_bucket"))
    }

    async fn get_buckets(
        &self,
        _req: tonic::Request<Organization>,
    ) -> Result<tonic::Response<GetBucketsResponse>, Status> {
        Err(Status::unimplemented("get_buckets"))
    }
}

/// Implementes the protobuf defined Storage service for a DatabaseStore
#[tonic::async_trait]
impl<T> Storage for GrpcService<T>
where
    T: DatabaseStore + 'static,
{
    type ReadFilterStream = mpsc::Receiver<Result<ReadResponse, Status>>;

    async fn read_filter(
        &self,
        _req: tonic::Request<ReadFilterRequest>,
    ) -> Result<tonic::Response<Self::ReadFilterStream>, Status> {
        Err(Status::unimplemented("read_filter"))
    }

    type ReadGroupStream = mpsc::Receiver<Result<ReadResponse, Status>>;

    #[tracing::instrument(level = "debug")]
    async fn read_group(
        &self,
        req: tonic::Request<ReadGroupRequest>,
    ) -> Result<tonic::Response<Self::ReadGroupStream>, Status> {
        Err(Status::unimplemented("read_group"))
    }

    type TagKeysStream = mpsc::Receiver<Result<StringValuesResponse, Status>>;

    #[tracing::instrument(level = "debug")]
    async fn tag_keys(
        &self,
        req: tonic::Request<TagKeysRequest>,
    ) -> Result<tonic::Response<Self::TagKeysStream>, Status> {
        Err(Status::unimplemented("tag_keys"))
    }

    type TagValuesStream = mpsc::Receiver<Result<StringValuesResponse, Status>>;

    #[tracing::instrument(level = "debug")]
    async fn tag_values(
        &self,
        req: tonic::Request<TagValuesRequest>,
    ) -> Result<tonic::Response<Self::TagValuesStream>, Status> {
        Err(Status::unimplemented("tag_values"))
    }

    #[tracing::instrument(level = "debug")]
    async fn capabilities(
        &self,
        req: tonic::Request<()>,
    ) -> Result<tonic::Response<CapabilitiesResponse>, Status> {
        // Full list of go capabilities in
        // idpe/storage/read/capabilities.go (aka window aggregate /
        // pushdown)
        //
        // For now, do not claim to support any capabilities
        let caps = CapabilitiesResponse {
            caps: HashMap::new(),
        };
        Ok(tonic::Response::new(caps))
    }

    type MeasurementNamesStream = mpsc::Receiver<Result<StringValuesResponse, Status>>;

    #[tracing::instrument(level = "debug")]
    async fn measurement_names(
        &self,
        req: tonic::Request<MeasurementNamesRequest>,
    ) -> Result<tonic::Response<Self::MeasurementNamesStream>, Status> {
        let (mut tx, rx) = mpsc::channel(4);

        let response = measurement_name_impl(self.db_store.clone(), req.into_inner())
            .await
            .map_err(|e| e.to_status());

        tx.send(response)
            .await
            .expect("sending measurement names response to server");

        Ok(tonic::Response::new(rx))
    }

    type MeasurementTagKeysStream = mpsc::Receiver<Result<StringValuesResponse, Status>>;

    #[tracing::instrument(level = "debug")]
    async fn measurement_tag_keys(
        &self,
        req: tonic::Request<MeasurementTagKeysRequest>,
    ) -> Result<tonic::Response<Self::MeasurementTagKeysStream>, Status> {
        Err(Status::unimplemented("measurement_tag_keys"))
    }

    type MeasurementTagValuesStream = mpsc::Receiver<Result<StringValuesResponse, Status>>;

    #[tracing::instrument(level = "debug")]
    async fn measurement_tag_values(
        &self,
        req: tonic::Request<MeasurementTagValuesRequest>,
    ) -> Result<tonic::Response<Self::MeasurementTagValuesStream>, Status> {
        Err(Status::unimplemented("measurement_tag_values"))
    }

    type MeasurementFieldsStream = mpsc::Receiver<Result<MeasurementFieldsResponse, Status>>;

    #[tracing::instrument(level = "debug")]
    async fn measurement_fields(
        &self,
        req: tonic::Request<MeasurementFieldsRequest>,
    ) -> Result<tonic::Response<Self::MeasurementFieldsStream>, Status> {
        Err(Status::unimplemented("measurement_fields"))
    }
}

// The following code implements the business logic of the requests as
// methods that return Results with module specific Errors (and thus
// can use ?, etc). The trait implemententations then handle mapping
// to the appropriate tonic Status
//
// TODO: Do this work on a separate worker pool, not the main tokio task pool
async fn measurement_name_impl<T>(
    db_store: Arc<T>,
    request: MeasurementNamesRequest,
) -> Result<StringValuesResponse>
where
    T: DatabaseStore + 'static,
{
    let db_name = org_and_bucket_to_database(request.org_id()?, &request.bucket_name()?);

    let db = db_store
        .db(&db_name)
        .await
        .ok_or_else(|| Error::DatabaseNotFound {
            db_name: db_name.clone(),
        })?;

    let table_names = db.table_names().await.map_err(|e| Error::GettingTables {
        db_name: db_name.clone(),
        source: Box::new(e),
    })?;

    // In theory this could be combined with the chain above, but
    // we break it into its own statement here for readability

    // This request can also have a time range. TODO handle this
    // reasonably somehow (select count(*) from table_name where <PREDICATE>???).
    // For now, return all measurements.
    if let Some(range) = request.range {
        warn!(
            "Timestamp ranges not yet supported in measurement_names storage gRPC request,\
             ignoring: {:?}",
            range
        );
    }

    // run a metadata query for each table to see if it has any rows
    // that match the predicates
    let mut values_futures = Vec::new();
    for table_name in table_names.iter() {
        let table_name = table_name.clone();
        let db = db.clone();
        let join_handle: JoinHandle<Result<Option<String>>> = tokio::task::spawn(async move {
            if has_any_rows(db, &table_name).await? {
                Ok(Some(table_name))
            } else {
                Ok(None)
            }
        });
        values_futures.push(join_handle);
    }

    let mut values: Vec<Vec<u8>> = Vec::new();

    // now, wait for all the values to resolve and build up the response
    for f in values_futures {
        let table_name = f.await.context(JoinError)??;

        // Convert into Vec<u8> for return
        if let Some(table_name) = table_name {
            values.push(table_name.bytes().collect())
        }
    }

    Ok(StringValuesResponse { values })
}

/// returns true if the specified table in the database
/// has any rows that match the predicate
///
/// TODO: actual predicate support
async fn has_any_rows<T>(db: Arc<T>, table_name: &str) -> Result<bool>
where
    T: Database,
{
    // TODO: build up a logical plan directly and avoid SQL
    // as building strings up is a potential security hole
    let sql = format!(r#"SELECT COUNT(*) FROM "{}""#, table_name);

    println!("Running SQL: {}", sql);

    let results = db
        .query(&sql)
        .await
        .map_err(|e| Error::ExecutingMetadataQuery {
            table_name: table_name.into(),
            source: Box::new(e),
        })?;

    // we expect a single record batch with a single count column
    match results.len() {
        1 => {
            // expect a schema with a single uint64 result
            let record_batch = &results[0];
            Ok(extract_row_count(record_batch)? > 0)
        }
        num => Err(Error::InternalError {
            description: format!(
                "Expected exactly 1 result when retrieving results of metadata query, got {}",
                num
            ),
        }),
    }
}

/// Expects a single u64 column with a single row and returns it
fn extract_row_count(record_batch: &RecordBatch) -> Result<u64> {
    let schema = record_batch.schema();
    let fields = schema.fields();
    match fields.len() {
        1 => {
            let field = &fields[0];
            match field.data_type() {
                DataType::UInt64 => {
                    let array = record_batch
                        .column(0)
                        .as_any()
                        .downcast_ref::<UInt64Array>();
                    match array {
                        Some(array) => extract_value(array),
                        None => Err(Error::InternalError {
                            description: "Can't downcast field to u64 metadata query".into(),
                        }),
                    }
                }
                dt => Err(Error::InternalError {
                    description: format!(
                        "Expected UInt64 field in result of metadata query, got {:?}",
                        dt
                    ),
                }),
            }
        }
        num => Err(Error::InternalError {
            description: format!(
                "Expected exactly 1 row in result of metadata query, got {}",
                num
            ),
        }),
    }
}

fn extract_value(array: &UInt64Array) -> Result<u64> {
    match array.len() {
        // here is the actual value we care about....
        1 => Ok(array.value(0)),
        num => Err(Error::InternalError {
            description: format!(
                "Expected exactly 1 row in result of metadata query, got {}",
                num
            ),
        }),
    }
}

/// Instantiate a server listening on the specified address
/// implementing the Delorean and Storage gRPC interfaces, the
/// underlying hyper server instance. Resolves when the server has
/// shutdown.
pub async fn make_server<T>(bind_addr: SocketAddr, storage: Arc<T>) -> Result<()>
where
    T: DatabaseStore + 'static,
{
    tonic::transport::Server::builder()
        .add_service(DeloreanServer::new(GrpcService::new(storage.clone())))
        .add_service(StorageServer::new(GrpcService::new(storage.clone())))
        .serve(bind_addr)
        .await
        .context(ServerError {})
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::{
        array::ArrayRef,
        array::UInt64Array,
        datatypes::{DataType, Field, Schema},
        record_batch::RecordBatch,
    };
    use delorean::{id::Id, storage::test_fixtures::TestDatabaseStore};
    use std::{
        convert::TryFrom,
        net::{IpAddr, Ipv4Addr, SocketAddr},
        time::Duration,
    };
    use tonic::Code;

    use futures::prelude::*;

    use delorean_generated_types::{delorean_client, storage_client, ReadSource};
    use prost::Message;

    type DeloreanClient = delorean_client::DeloreanClient<tonic::transport::Channel>;
    type StorageClient = storage_client::StorageClient<tonic::transport::Channel>;

    #[tokio::test]
    async fn test_delorean_rpc() -> Result<()> {
        let mut fixture = Fixture::new(11807)
            .await
            .expect("Connecting to test server");

        let org = Organization {
            id: 1337,
            name: "my non-existent-org".into(),
            buckets: Vec::new(),
        };

        // Test response from delorean server
        let res = fixture.delorean_client.get_buckets(org).await;

        match res {
            Err(e) => {
                assert_eq!(e.code(), Code::Unimplemented);
                assert_eq!(e.message(), "get_buckets");
            }
            Ok(buckets) => {
                assert!(false, "Unexpected delorean_client success: {:?}", buckets);
            }
        };

        Ok(())
    }

    #[tokio::test]
    async fn test_storage_rpc_capabilities() -> Result<(), tonic::Status> {
        // Note we use a unique port. TODO: let the OS pick the port
        let mut fixture = Fixture::new(11808)
            .await
            .expect("Connecting to test server");

        // Test response from storage server
        assert_eq!(HashMap::new(), fixture.storage_client.capabilities().await?);

        Ok(())
    }

    #[tokio::test]
    async fn test_storage_rpc_measurement_names() -> Result<(), tonic::Status> {
        // Note we use a unique port. TODO: let the OS pick the port
        let mut fixture = Fixture::new(11809)
            .await
            .expect("Connecting to test server");

        let db_info = OrgAndBucket::new(123, 456);
        let partition_id = 1;

        // Tell the system there are rows in h2o and o2, but not co2
        fixture
            .test_storage
            .add_query_answer(
                &db_info.db_name,
                r#"SELECT COUNT(*) FROM "h2o""#,
                Ok(vec![make_count_record_batch(&["count"], &[1])]),
            )
            .await;
        fixture
            .test_storage
            .add_query_answer(
                &db_info.db_name,
                r#"SELECT COUNT(*) FROM "o2""#,
                Ok(vec![make_count_record_batch(&["count"], &[1])]),
            )
            .await;
        fixture
            .test_storage
            .add_query_answer(
                &db_info.db_name,
                r#"SELECT COUNT(*) FROM "co2""#,
                Ok(vec![make_count_record_batch(&["count"], &[0])]),
            )
            .await;

        let lp_data = "h2o,state=CA temp=50.4 1568756160\no2,state=MA temp=50.4 1568756160\nco2,state=RI temp=50.4 1568756160";
        fixture
            .test_storage
            .add_lp_string(&db_info.db_name, lp_data)
            .await;

        let source = Some(StorageClientWrapper::read_source(
            db_info.org_id,
            db_info.bucket_id,
            partition_id,
        ));
        let request = MeasurementNamesRequest {
            source,
            range: None,
        };

        let actual_measurements = fixture.storage_client.measurement_names(request).await?;

        let expected_measurements = vec![String::from("h2o"), String::from("o2")];
        assert_eq!(actual_measurements, expected_measurements);

        Ok(())
    }

    /// Delorean deals with database names. The gRPC interface deals
    /// with org_id and bucket_id represented as 16 digit hex
    /// values. This struct manages creating the org_id, bucket_id,
    /// and database names to be consistent with the implementation
    struct OrgAndBucket {
        org_id: u64,
        bucket_id: u64,
        /// The delorean database name corresponding to `org_id` and `bucket_id`
        db_name: String,
    }

    impl OrgAndBucket {
        fn new(org_id: u64, bucket_id: u64) -> Self {
            let org_id_str = Id::try_from(org_id).expect("org_id was valid").to_string();

            let bucket_id_str = Id::try_from(bucket_id)
                .expect("bucket_id was valid")
                .to_string();

            let db_name = org_and_bucket_to_database(&org_id_str, &bucket_id_str);

            Self {
                org_id,
                bucket_id,
                db_name,
            }
        }
    }

    /// Wrapper around a StorageClient that does the various tonic /
    /// futures dance
    struct StorageClientWrapper {
        inner: StorageClient,
    }

    impl StorageClientWrapper {
        fn new(inner: StorageClient) -> Self {
            Self { inner }
        }

        /// Create a ReadSource suitable for constructing messages
        fn read_source(org_id: u64, bucket_id: u64, partition_id: u64) -> prost_types::Any {
            let read_source = ReadSource {
                org_id,
                bucket_id,
                partition_id,
            };
            let mut d = Vec::new();
            read_source
                .encode(&mut d)
                .expect("encoded read source appropriately");
            prost_types::Any {
                type_url: "/TODO".to_string(),
                value: d,
            }
        }

        /// return the capabilities of the server as a hash map
        async fn capabilities(&mut self) -> Result<HashMap<String, String>, tonic::Status> {
            let response = self.inner.capabilities(()).await?.into_inner();

            let CapabilitiesResponse { caps } = response;

            Ok(caps)
        }

        // Make a request to Storage::measurement_names and do the
        // required async dance to flatten the resulting stream to Strings
        async fn measurement_names(
            &mut self,
            request: MeasurementNamesRequest,
        ) -> Result<Vec<String>, tonic::Status> {
            let responses = self.inner.measurement_names(request).await?;

            // type annotations to help future readers
            let responses: Vec<StringValuesResponse> = responses.into_inner().try_collect().await?;

            let measurements = responses
                .into_iter()
                .map(|r| r.values.into_iter())
                .flatten()
                .map(|v| String::from_utf8(v).expect("measurement name was utf8"))
                .collect::<Vec<_>>();

            Ok(measurements)
        }
    }

    /// loop and try to make a client connection for 5 seconds,
    /// returning the result of the connection
    async fn connect_to_server<T>(bind_addr: SocketAddr) -> Result<T, tonic::transport::Error>
    where
        T: NewClient,
    {
        const MAX_RETRIES: u32 = 10;
        let mut retry_count = 0;
        loop {
            let mut interval = tokio::time::interval(Duration::from_millis(500));

            match T::connect(format!("http://{}", bind_addr)).await {
                Ok(client) => {
                    println!("Sucessfully connected to server. Client: {:?}", client);
                    return Ok(client);
                }
                Err(e) => {
                    retry_count += 1;

                    if retry_count > 10 {
                        println!("Server did not start in time: {}", e);
                        return Err(e);
                    } else {
                        println!(
                            "Server not yet up. Retrying ({}/{}): {}",
                            retry_count, MAX_RETRIES, e
                        );
                    }
                }
            };
            interval.tick().await;
        }
    }

    // Wrapper around raw clients and test database
    struct Fixture {
        delorean_client: DeloreanClient,
        storage_client: StorageClientWrapper,
        test_storage: Arc<TestDatabaseStore>,
    }

    impl Fixture {
        /// Start up a test rpc server listening on `port`, returning
        /// a fixture with the test server and clients
        async fn new(port: u16) -> Result<Self, tonic::transport::Error> {
            let test_storage = Arc::new(TestDatabaseStore::new());
            // TODO: specify port 0 to let the OS pick the port (need to
            // figure out how to get access to the actual addr from tonic)
            let bind_addr = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), port);

            println!("Starting delorean rpc test server on {:?}", bind_addr);

            let server = make_server(bind_addr, test_storage.clone());
            tokio::task::spawn(server);

            let delorean_client = connect_to_server::<DeloreanClient>(bind_addr).await?;
            let storage_client =
                StorageClientWrapper::new(connect_to_server::<StorageClient>(bind_addr).await?);

            Ok(Self {
                delorean_client,
                storage_client,
                test_storage,
            })
        }
    }

    /// Represents something that can make a connection to a server
    #[tonic::async_trait]
    trait NewClient: Sized + std::fmt::Debug {
        async fn connect(addr: String) -> Result<Self, tonic::transport::Error>;
    }

    #[tonic::async_trait]
    impl NewClient for DeloreanClient {
        async fn connect(addr: String) -> Result<Self, tonic::transport::Error> {
            Self::connect(addr).await
        }
    }

    #[tonic::async_trait]
    impl NewClient for StorageClient {
        async fn connect(addr: String) -> Result<Self, tonic::transport::Error> {
            Self::connect(addr).await
        }
    }

    /// Returns a record batch with column names and counts as specified in the argument
    fn make_count_record_batch(column_names: &[&str], counts: &[u64]) -> RecordBatch {
        let schema = Arc::new(Schema::new(
            column_names
                .iter()
                .map(|col_name| Field::new(col_name, DataType::UInt64, false))
                .collect::<Vec<_>>(),
        ));

        // now build the columns (with one int apiece for each of the columns)
        let columns = counts
            .iter()
            .map(|count| Arc::new(UInt64Array::from(vec![*count])) as ArrayRef)
            .collect::<Vec<ArrayRef>>();

        RecordBatch::try_new(schema, columns).expect("creatng record batch")
    }
}
