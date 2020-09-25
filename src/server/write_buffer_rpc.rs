//! This module contains gRPC service implementatations for the WriteBuffer
//! storage implementation

// Something in instrument is causing lint warnings about unused braces
#![allow(unused_braces)]

// Something about how `tracing::instrument` works triggers a clippy
// warning about complex types
#![allow(clippy::type_complexity)]

use std::{collections::BTreeSet, collections::HashMap, net::SocketAddr, sync::Arc};

use delorean_generated_types::{
    delorean_server::{Delorean, DeloreanServer},
    storage_server::{Storage, StorageServer},
    CapabilitiesResponse, CreateBucketRequest, CreateBucketResponse, DeleteBucketRequest,
    DeleteBucketResponse, GetBucketsResponse, MeasurementFieldsRequest, MeasurementFieldsResponse,
    MeasurementNamesRequest, MeasurementTagKeysRequest, MeasurementTagValuesRequest, Organization,
    Predicate, ReadFilterRequest, ReadGroupRequest, ReadResponse, StringValuesResponse,
    TagKeysRequest, TagValuesRequest, TestErrorRequest, TestErrorResponse, TimestampRange,
};

// For some reason rust thinks these imports are unused, but then
// complains of unresolved imports if they are not imported.
#[allow(unused_imports)]
use delorean_generated_types::{node, Node};

use crate::server::rpc::input::GrpcInputs;
use delorean_storage::{
    org_and_bucket_to_database, Database, DatabaseStore, Predicate as StoragePredicate,
    TimestampRange as StorageTimestampRange,
};

use snafu::{ResultExt, Snafu};

use tokio::sync::mpsc;
use tonic::Status;
use tracing::warn;

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("gRPC server error:  {}", source))]
    ServerError { source: tonic::transport::Error },

    #[snafu(display("Database not found: {}", db_name))]
    DatabaseNotFound { db_name: String },

    #[snafu(display("Can not retrieve table list for '{}': {}", db_name, source))]
    ListingTables {
        db_name: String,
        source: Box<dyn std::error::Error + Send + Sync>,
    },

    #[snafu(display("Can not retrieve column list '{}': {}", db_name, source))]
    ListingColumns {
        db_name: String,
        source: Box<dyn std::error::Error + Send + Sync>,
    },

    #[snafu(display("Operation not yet implemented:  {}", operation))]
    NotYetImplemented { operation: String },
}

pub type Result<T, E = Error> = std::result::Result<T, E>;

impl Error {
    /// Converts a result from the business logic into the appropriate tonic status
    fn to_status(&self) -> tonic::Status {
        match &self {
            Self::ServerError { .. } => Status::internal(self.to_string()),
            Self::DatabaseNotFound { .. } => Status::not_found(self.to_string()),
            Self::ListingTables { .. } => Status::internal(self.to_string()),
            Self::ListingColumns { .. } => {
                // TODO: distinguish between input errors and internal errors
                Status::internal(self.to_string())
            }
            Self::NotYetImplemented { .. } => Status::internal(self.to_string()),
        }
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

    async fn test_error(
        &self,
        _req: tonic::Request<TestErrorRequest>,
    ) -> Result<tonic::Response<TestErrorResponse>, Status> {
        warn!("Got a test_error request. About to panic");
        panic!("This is a test panic");
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
        let (mut tx, rx) = mpsc::channel(4);

        let tag_keys_request = req.into_inner();

        let db_name = get_database_name(&tag_keys_request)?;

        let TagKeysRequest {
            tags_source: _tag_source,
            range,
            predicate,
        } = tag_keys_request;

        let measurement = None;

        let response = tag_keys_impl(
            self.db_store.clone(),
            db_name,
            measurement,
            range,
            predicate,
        )
        .await
        .map_err(|e| e.to_status());

        tx.send(response)
            .await
            .expect("sending tag_keys response to server");

        Ok(tonic::Response::new(rx))
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

        let measurement_names_request = req.into_inner();

        let db_name = get_database_name(&measurement_names_request)?;

        let MeasurementNamesRequest {
            source: _source,
            range,
        } = measurement_names_request;

        let response = measurement_name_impl(self.db_store.clone(), db_name, range)
            .await
            .map_err(|e| e.to_status());

        tx.send(response)
            .await
            .expect("sending measurement names response to server");

        Ok(tonic::Response::new(rx))
    }

    type MeasurementTagKeysStream = mpsc::Receiver<Result<StringValuesResponse, Status>>;

    //#[tracing::instrument(level = "debug")]
    async fn measurement_tag_keys(
        &self,
        req: tonic::Request<MeasurementTagKeysRequest>,
    ) -> Result<tonic::Response<Self::MeasurementTagKeysStream>, Status> {
        let (mut tx, rx) = mpsc::channel(4);

        let measurement_tag_keys_request = req.into_inner();

        let db_name = get_database_name(&measurement_tag_keys_request)?;

        let MeasurementTagKeysRequest {
            source: _source,
            measurement,
            range,
            predicate,
        } = measurement_tag_keys_request;

        let measurement = Some(measurement);

        let response = tag_keys_impl(
            self.db_store.clone(),
            db_name,
            measurement,
            range,
            predicate,
        )
        .await
        .map_err(|e| e.to_status());

        tx.send(response)
            .await
            .expect("sending measurement_tag_keys response to server");

        Ok(tonic::Response::new(rx))
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

fn convert_range(range: Option<TimestampRange>) -> Option<StorageTimestampRange> {
    range.map(|TimestampRange { start, end }| StorageTimestampRange { start, end })
}

fn get_database_name(input: &impl GrpcInputs) -> Result<String, Status> {
    Ok(org_and_bucket_to_database(
        input.org_id()?,
        &input.bucket_name()?,
    ))
}

/// converts the Node (predicate tree) into a datafusion Expr for evaluation
fn convert_predicate(predicate: Predicate) -> Result<StoragePredicate> {
    warn!(
        "Not yet implemented: converting predicates: {:?}",
        predicate
    );
    Ok(StoragePredicate {})
}

// The following code implements the business logic of the requests as
// methods that return Results with module specific Errors (and thus
// can use ?, etc). The trait implemententations then handle mapping
// to the appropriate tonic Status

/// Gathers all measurement names that have data in the specified
/// (optional) range
///
/// TODO: Do this work on a separate worker pool, not the main tokio
/// task poo
async fn measurement_name_impl<T>(
    db_store: Arc<T>,
    db_name: String,
    range: Option<TimestampRange>,
) -> Result<StringValuesResponse>
where
    T: DatabaseStore,
{
    let range = convert_range(range);

    let table_names = db_store
        .db(&db_name)
        .await
        .ok_or_else(|| Error::DatabaseNotFound {
            db_name: db_name.clone(),
        })?
        .table_names(range)
        .await
        .map_err(|e| Error::ListingTables {
            db_name: db_name.clone(),
            source: Box::new(e),
        })?;

    // In theory this could be combined with the chain above, but
    // we break it into its own statement here for readability

    // Map the resulting collection of Strings into a Vec<Vec<u8>>for return
    let values = table_names
        .iter()
        .map(|name| name.bytes().collect())
        .collect::<Vec<_>>();

    Ok(StringValuesResponse { values })
}

/// Return tag key names by querying columns
async fn tag_keys_impl<T>(
    db_store: Arc<T>,
    db_name: String,
    measurement: Option<String>,
    range: Option<TimestampRange>,
    predicate: Option<Predicate>,
) -> Result<StringValuesResponse>
where
    T: DatabaseStore,
{
    let range = convert_range(range);

    let db = db_store
        .db(&db_name)
        .await
        .ok_or_else(|| Error::DatabaseNotFound {
            db_name: db_name.clone(),
        })?;

    let table_names = match predicate {
        Some(predicate) => {
            find_tag_keys_with_predicate(db, db_name, measurement, range, predicate).await
        }
        // no predicate, take fast path
        None => find_tag_keys_without_predicate(db, db_name, measurement, range).await,
    }?;

    // Map the resulting collection of Strings into a Vec<Vec<u8>>for return
    let values = table_names
        .iter()
        .map(|name| name.bytes().collect())
        .collect::<Vec<_>>();

    Ok(StringValuesResponse { values })
}

async fn find_tag_keys_without_predicate<D>(
    db: Arc<D>,
    db_name: String,
    measurement: Option<String>,
    range: Option<StorageTimestampRange>,
) -> Result<Arc<BTreeSet<String>>>
where
    D: Database,
{
    db.tag_column_names(measurement, range)
        .await
        .map_err(|e| Error::ListingColumns {
            db_name,
            source: Box::new(e),
        })
}

async fn find_tag_keys_with_predicate<D>(
    db: Arc<D>,
    db_name: String,
    measurement: Option<String>,
    range: Option<StorageTimestampRange>,
    predicate: Predicate,
) -> Result<Arc<BTreeSet<String>>>
where
    D: Database,
{
    let predicate = convert_predicate(predicate)?;

    // TODO: fire up some executors and run the predicate for real here
    db.tag_column_names_with_predicate(measurement, range, predicate)
        .await
        .map_err(|e| Error::ListingColumns {
            db_name,
            source: Box::new(e),
        })
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
    use crate::panic::SendPanicsToTracing;
    use delorean_storage::{id::Id, test::ColumnNamesRequest, test::TestDatabaseStore};
    use delorean_test_helpers::tracing::TracingCapture;
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

        let lp_data = "h2o,state=CA temp=50.4 100\n\
                       o2,state=MA temp=50.4 200";
        fixture
            .test_storage
            .add_lp_string(&db_info.db_name, lp_data)
            .await;

        let source = Some(StorageClientWrapper::read_source(
            db_info.org_id,
            db_info.bucket_id,
            partition_id,
        ));

        // --- No timestamps
        let request = MeasurementNamesRequest {
            source: source.clone(),
            range: None,
        };

        let actual_measurements = fixture.storage_client.measurement_names(request).await?;
        let expected_measurements = to_string_vec(&["h2o", "o2"]);
        assert_eq!(actual_measurements, expected_measurements);

        // --- Timestamp range
        let range = TimestampRange {
            start: 150,
            end: 200,
        };
        let request = MeasurementNamesRequest {
            source,
            range: Some(range),
        };

        let actual_measurements = fixture.storage_client.measurement_names(request).await?;
        let expected_measurements = to_string_vec(&["o2"]);
        assert_eq!(actual_measurements, expected_measurements);

        Ok(())
    }

    /// test the plumbing of the RPC layer for tag_keys -- specifically that
    /// the right parameters are passed into the Database interface
    /// and that the returned values are sent back via gRPC.
    #[tokio::test]
    async fn test_storage_rpc_tag_keys() -> Result<(), tonic::Status> {
        // Note we use a unique port. TODO: let the OS pick the port
        let mut fixture = Fixture::new(11810)
            .await
            .expect("Connecting to test server");

        let db_info = OrgAndBucket::new(123, 456);
        let partition_id = 1;

        let test_db = fixture
            .test_storage
            .db_or_create(&db_info.db_name)
            .await
            .expect("creating test database");

        let source = Some(StorageClientWrapper::read_source(
            db_info.org_id,
            db_info.bucket_id,
            partition_id,
        ));

        #[derive(Debug)]
        struct TestCase<'a> {
            /// The tag keys to load into the database
            tag_keys: Vec<&'a str>,
            request: TagKeysRequest,
            expected_request: ColumnNamesRequest,
        }

        let test_cases = vec![
            // ---
            // No predicates / timestamps
            // ---
            TestCase {
                tag_keys: vec!["k1"],
                request: TagKeysRequest {
                    tags_source: source.clone(),
                    range: None,
                    predicate: None,
                },
                expected_request: ColumnNamesRequest {
                    table: None,
                    range: None,
                    predicate: None,
                },
            },
            // ---
            // Timestamp range
            // ---
            TestCase {
                tag_keys: vec!["k1", "k2"],
                request: TagKeysRequest {
                    tags_source: source.clone(),
                    range: make_timestamp_range(150, 200),
                    predicate: None,
                },
                expected_request: ColumnNamesRequest {
                    table: None,
                    range: Some(StorageTimestampRange::new(150, 200)),
                    predicate: None,
                },
            },
            // ---
            // Predicate
            // ---
            TestCase {
                tag_keys: vec!["k1", "k2", "k3"],
                request: TagKeysRequest {
                    tags_source: source.clone(),
                    range: None,
                    predicate: make_state_ma_predicate(),
                },
                expected_request: ColumnNamesRequest {
                    table: None,
                    range: None,
                    predicate: Some(StoragePredicate {}), // TODO fill this in with the appropriate translation
                },
            },
            // ---
            // Timestamp + Predicate
            // ---
            TestCase {
                tag_keys: vec!["k1", "k2", "k3", "k4"],
                request: TagKeysRequest {
                    tags_source: source.clone(),
                    range: make_timestamp_range(150, 200),
                    predicate: make_state_ma_predicate(),
                },
                expected_request: ColumnNamesRequest {
                    table: None,
                    range: Some(StorageTimestampRange::new(150, 200)),
                    predicate: Some(StoragePredicate {}), // TODO fill this in with the appropriate translation
                },
            },
        ];

        for test_case in test_cases.into_iter() {
            let test_case_str = format!("{:?}", test_case);
            let TestCase {
                tag_keys,
                request,
                expected_request,
            } = test_case;

            test_db.set_column_names(to_string_vec(&tag_keys)).await;

            let actual_tag_keys = fixture.storage_client.tag_keys(request).await?;
            assert_eq!(
                actual_tag_keys, tag_keys,
                "unexpected tag keys while getting column names: {}",
                test_case_str
            );
            assert_eq!(
                test_db.get_column_names_request().await,
                Some(expected_request),
                "unexpected request while getting column names: {}",
                test_case_str
            );
        }

        // ---
        // test error
        // ---
        let request = TagKeysRequest {
            tags_source: source.clone(),
            range: None,
            predicate: None,
        };

        // Note we don't set the column_names on the test database, so we expect an error
        let response = fixture.storage_client.tag_keys(request).await;
        assert!(response.is_err());
        let response_string = format!("{:?}", response);
        let expected_error = "No saved column_names in TestDatabase";
        assert!(
            response_string.contains(expected_error),
            "'{}' did not contain expected content '{}'",
            response_string,
            expected_error
        );

        let expected_request = Some(ColumnNamesRequest {
            table: None,
            range: None,
            predicate: None,
        });
        assert_eq!(test_db.get_column_names_request().await, expected_request);

        Ok(())
    }

    /// test the plumbing of the RPC layer for measurement_tag_keys-- specifically that
    /// the right parameters are passed into the Database interface
    /// and that the returned values are sent back via gRPC.
    #[tokio::test]
    async fn test_storage_rpc_measurement_tag_keys() -> Result<(), tonic::Status> {
        // Note we use a unique port. TODO: let the OS pick the port
        let mut fixture = Fixture::new(11811)
            .await
            .expect("Connecting to test server");

        let db_info = OrgAndBucket::new(123, 456);
        let partition_id = 1;

        let test_db = fixture
            .test_storage
            .db_or_create(&db_info.db_name)
            .await
            .expect("creating test database");

        let source = Some(StorageClientWrapper::read_source(
            db_info.org_id,
            db_info.bucket_id,
            partition_id,
        ));

        #[derive(Debug)]
        struct TestCase<'a> {
            /// The tag keys to load into the database
            tag_keys: Vec<&'a str>,
            request: MeasurementTagKeysRequest,
            expected_request: ColumnNamesRequest,
        }

        let test_cases = vec![
            // ---
            // No predicates / timestamps
            // ---
            TestCase {
                tag_keys: vec!["k1"],
                request: MeasurementTagKeysRequest {
                    measurement: "m1".into(),
                    source: source.clone(),
                    range: None,
                    predicate: None,
                },
                expected_request: ColumnNamesRequest {
                    table: Some("m1".into()),
                    range: None,
                    predicate: None,
                },
            },
            // ---
            // Timestamp range
            // ---
            TestCase {
                tag_keys: vec!["k1", "k2"],
                request: MeasurementTagKeysRequest {
                    measurement: "m2".into(),
                    source: source.clone(),
                    range: make_timestamp_range(150, 200),
                    predicate: None,
                },
                expected_request: ColumnNamesRequest {
                    table: Some("m2".into()),
                    range: Some(StorageTimestampRange::new(150, 200)),
                    predicate: None,
                },
            },
            // ---
            // Predicate
            // ---
            TestCase {
                tag_keys: vec!["k1", "k2", "k3"],
                request: MeasurementTagKeysRequest {
                    measurement: "m3".into(),
                    source: source.clone(),
                    range: None,
                    predicate: make_state_ma_predicate(),
                },
                expected_request: ColumnNamesRequest {
                    table: Some("m3".into()),
                    range: None,
                    predicate: Some(StoragePredicate {}), // TODO fill this in with the appropriate translation
                },
            },
            // ---
            // Timestamp + Predicate
            // ---
            TestCase {
                tag_keys: vec!["k1", "k2", "k3", "k4"],
                request: MeasurementTagKeysRequest {
                    measurement: "m4".into(),
                    source: source.clone(),
                    range: make_timestamp_range(150, 200),
                    predicate: make_state_ma_predicate(),
                },
                expected_request: ColumnNamesRequest {
                    table: Some("m4".into()),
                    range: Some(StorageTimestampRange::new(150, 200)),
                    predicate: Some(StoragePredicate {}), // TODO fill this in with the appropriate translation
                },
            },
        ];

        for test_case in test_cases.into_iter() {
            let test_case_str = format!("{:?}", test_case);
            let TestCase {
                tag_keys,
                request,
                expected_request,
            } = test_case;

            test_db.set_column_names(to_string_vec(&tag_keys)).await;

            let actual_tag_keys = fixture.storage_client.measurement_tag_keys(request).await?;
            assert_eq!(
                actual_tag_keys, tag_keys,
                "unexpected tag keys while getting column names: {}",
                test_case_str
            );
            assert_eq!(
                test_db.get_column_names_request().await,
                Some(expected_request),
                "unexpected request while getting column names: {}",
                test_case_str
            );
        }

        // ---
        // test error
        // ---
        let request = MeasurementTagKeysRequest {
            measurement: "m5".into(),
            source: source.clone(),
            range: None,
            predicate: None,
        };

        // Note we don't set the column_names on the test database, so we expect an error
        let response = fixture.storage_client.measurement_tag_keys(request).await;
        assert!(response.is_err());
        let response_string = format!("{:?}", response);
        let expected_error = "No saved column_names in TestDatabase";
        assert!(
            response_string.contains(expected_error),
            "'{}' did not contain expected content '{}'",
            response_string,
            expected_error
        );

        let expected_request = Some(ColumnNamesRequest {
            table: Some("m5".into()),
            range: None,
            predicate: None,
        });
        assert_eq!(test_db.get_column_names_request().await, expected_request);

        Ok(())
    }

    #[tokio::test]
    async fn test_log_on_panic() -> Result<(), tonic::Status> {
        // Send a message to a route that causes a panic and ensure:
        // 1. We don't use up all executors 2. The panic message
        // message ends up in the log system

        // Normally, the global panic logger is set at program start
        let f = SendPanicsToTracing::new();

        // capture all tracing messages
        let tracing_capture = TracingCapture::new();

        // Note we use a unique port. TODO: let the OS pick the port
        let mut fixture = Fixture::new(11812)
            .await
            .expect("Connecting to test server");

        let request = TestErrorRequest {};

        // Test response from storage server
        let response = fixture.delorean_client.test_error(request).await;

        match &response {
            Ok(_) => {
                panic!("Unexpected success: {:?}", response);
            }
            Err(status) => {
                assert_eq!(status.code(), Code::Unknown);
                assert!(
                    status.message().contains("transport error"),
                    "could not find 'transport error' in '{}'",
                    status.message()
                );
            }
        };

        // Note: use f here to ensure it live at least this long
        std::mem::drop(f);

        // Ensure that the logs captured the panic (and drop f beforehand)
        let captured_logs = tracing_capture.to_string();
        // Note we don't include the actual line / column in the
        // expected panic message to avoid needing to update the test
        // whenever the source code file changed.
        let expected_error = "panicked at 'This is a test panic', src/server/write_buffer_rpc.rs:";
        assert!(
            captured_logs.contains(expected_error),
            "Logs did not contain expected panic message '{}'. They were\n{}",
            expected_error,
            captured_logs
        );

        // Ensure that panics don't exhaust the tokio executor by
        // running 100 times (success is if we can make a successful
        // call after this)
        for _ in 0usize..100 {
            let request = TestErrorRequest {};

            // Test response from storage server
            let response = fixture.delorean_client.test_error(request).await;
            assert!(response.is_err(), "Got an error response: {:?}", response);
        }

        // Ensure there are still threads to answer actual client queries
        assert_eq!(HashMap::new(), fixture.storage_client.capabilities().await?);

        Ok(())
    }

    fn make_timestamp_range(start: i64, end: i64) -> Option<TimestampRange> {
        Some(TimestampRange { start, end })
    }

    /// return a predicate like
    ///
    /// state="MA"
    fn make_state_ma_predicate() -> Option<Predicate> {
        use node::{Comparison, Value};
        let root = Node {
            value: Some(Value::Comparison(Comparison::Equal as i32)),
            children: vec![
                Node {
                    value: Some(Value::TagRefValue("state".to_string())),
                    children: vec![],
                },
                Node {
                    value: Some(Value::StringValue("MA".to_string())),
                    children: vec![],
                },
            ],
        };
        Some(Predicate { root: Some(root) })
    }

    /// Convert to a Vec<String> to facilitate comparison with results of client
    fn to_string_vec(v: &[&str]) -> Vec<String> {
        v.iter().map(|s| s.to_string()).collect()
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

        /// Make a request to Storage::measurement_names and do the
        /// required async dance to flatten the resulting stream to Strings
        async fn measurement_names(
            &mut self,
            request: MeasurementNamesRequest,
        ) -> Result<Vec<String>, tonic::Status> {
            let responses = self
                .inner
                .measurement_names(request)
                .await?
                .into_inner()
                .try_collect()
                .await?;

            Ok(self.to_string_vec(responses))
        }

        /// Make a request to Storage::tag_keys and do the
        /// required async dance to flatten the resulting stream to Strings
        async fn tag_keys(
            &mut self,
            request: TagKeysRequest,
        ) -> Result<Vec<String>, tonic::Status> {
            let responses = self
                .inner
                .tag_keys(request)
                .await?
                .into_inner()
                .try_collect()
                .await?;

            Ok(self.to_string_vec(responses))
        }

        /// Make a request to Storage::measurement_tag_keys and do the
        /// required async dance to flatten the resulting stream to Strings
        async fn measurement_tag_keys(
            &mut self,
            request: MeasurementTagKeysRequest,
        ) -> Result<Vec<String>, tonic::Status> {
            let responses = self
                .inner
                .measurement_tag_keys(request)
                .await?
                .into_inner()
                .try_collect()
                .await?;

            Ok(self.to_string_vec(responses))
        }

        /// Convert the StringValueResponses into rust Strings, sorting the values
        /// to ensure  consistency.
        fn to_string_vec(&self, responses: Vec<StringValuesResponse>) -> Vec<String> {
            let mut strings = responses
                .into_iter()
                .map(|r| r.values.into_iter())
                .flatten()
                .map(|v| String::from_utf8(v).expect("string value response was not utf8"))
                .collect::<Vec<_>>();

            strings.sort();

            strings
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
}
