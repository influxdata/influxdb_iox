use crate::commands::{
    logging::LoggingLevel,
    server::{ObjectStore as ObjStoreOpt, RunConfig},
};
use hyper::Server;
use object_store::{
    self, aws::AmazonS3, azure::MicrosoftAzure, gcp::GoogleCloudStorage, ObjectStore,
};
use panic_logging::SendPanicsToTracing;
use query::DatabaseStore;
use server::{ConnectionManagerImpl as ConnectionManager, Server as AppServer};
use snafu::{ResultExt, Snafu};
use std::{convert::TryFrom, fs, net::SocketAddr, path::PathBuf, sync::Arc};
use tracing::{error, info, warn};

mod http;
mod rpc;

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("Unable to create database directory {:?}: {}", path, source))]
    CreatingDatabaseDirectory {
        path: PathBuf,
        source: std::io::Error,
    },

    #[snafu(display(
        "Unable to bind to listen for HTTP requests on {}: {}",
        bind_addr,
        source
    ))]
    StartListeningHttp {
        bind_addr: SocketAddr,
        source: hyper::Error,
    },

    #[snafu(display(
        "Unable to bind to listen for gRPC requests on {}: {}",
        grpc_bind_addr,
        source
    ))]
    StartListeningGrpc {
        grpc_bind_addr: SocketAddr,
        source: std::io::Error,
    },

    #[snafu(display("Error serving HTTP: {}", source))]
    ServingHttp { source: hyper::Error },

    #[snafu(display("Error serving RPC: {}", source))]
    ServingRPC { source: self::rpc::Error },

    #[snafu(display(
        "Specified {} for the object store, required configuration missing for {}",
        object_store,
        missing
    ))]
    MissingObjectStoreConfig {
        object_store: ObjStoreOpt,
        missing: String,
    },

    // Creating a new S3 object store can fail if the region is *specified* but
    // not *parseable* as a rusoto `Region`. The other object store constructors
    // don't return `Result`.
    #[snafu(display("Amazon S3 configuration was invalid: {}", source))]
    InvalidS3Config { source: object_store::aws::Error },
}

pub type Result<T, E = Error> = std::result::Result<T, E>;

/// This is the entry point for the IOx server. `config` represents
/// command line arguments, if any
///
/// The logging_level passed in is the global setting (e.g. if -v or
/// -vv was passed in before 'server')
pub async fn main(logging_level: LoggingLevel, config: RunConfig) -> Result<()> {
    // Handle the case if -v/-vv is specified both before and after the server
    // command
    let logging_level = logging_level.combine(LoggingLevel::new(config.verbose_count));

    let _drop_handle = logging_level.setup_logging(&config);

    // Install custom panic handler and forget about it.
    //
    // This leaks the handler and prevents it from ever being dropped during the
    // lifetime of the program - this is actually a good thing, as it prevents
    // the panic handler from being removed while unwinding a panic (which in
    // turn, causes a panic - see #548)
    let f = SendPanicsToTracing::new();
    std::mem::forget(f);

    match config.object_store {
        Some(ObjStoreOpt::Memory) | None => {
            warn!("NO PERSISTENCE: using Memory for object storage");
        }
        Some(store) => {
            info!("Using {} for object storage", store);
        }
    }

    let object_store = ObjectStore::try_from(&config)?;
    let object_storage = Arc::new(object_store);

    let connection_manager = ConnectionManager {};
    let app_server = Arc::new(AppServer::new(connection_manager, object_storage));

    // if this ID isn't set the server won't be usable until this is set via an API
    // call
    if let Some(id) = config.writer_id {
        app_server.set_id(id);
        if let Err(e) = app_server.load_database_configs().await {
            error!(
                "unable to load database configurations from object storage: {}",
                e
            )
        }
    } else {
        warn!("server ID not set. ID must be set via the INFLUXDB_IOX_ID config or API before writing or querying data.");
    }

    // Construct and start up gRPC server

    let grpc_bind_addr = config.grpc_bind_address;
    let socket = tokio::net::TcpListener::bind(grpc_bind_addr)
        .await
        .context(StartListeningGrpc { grpc_bind_addr })?;

    let grpc_server = self::rpc::make_server(socket, Arc::clone(&app_server));

    info!(bind_address=?grpc_bind_addr, "gRPC server listening");

    // Construct and start up HTTP server

    let router_service = http::router_service(Arc::clone(&app_server));

    let bind_addr = config.http_bind_address;
    let http_server = Server::try_bind(&bind_addr)
        .context(StartListeningHttp { bind_addr })?
        .serve(router_service);
    info!(bind_address=?bind_addr, "HTTP server listening");

    let git_hash = option_env!("GIT_HASH").unwrap_or("UNKNOWN");
    info!(git_hash, "InfluxDB IOx server ready");

    // Start a background service, ChunkMover, that will repeat spawning other
    // background tasks to move eligible chunks of mutable buffer to read buffer
    tokio::task::spawn(async move { run_chunk_movers(Arc::clone(&app_server)).await }); 
    info!("Chunk Movers ready");

    // Wait for both the servers to complete
    let (grpc_server, server) = futures::future::join(grpc_server, http_server).await;

    grpc_server.context(ServingRPC)?;
    server.context(ServingHttp)?;

    info!("InfluxDB IOx server shutting down");

    Ok(())
}

/// ChunkMover: a background service (or a background task) that is responsible
/// for moving eligible chunks from mutable buffer to read buffer. This is a
/// repeating process that ensures all chunks are moved successfully
/// or move them again if they fail or are stuck for too long for some reasons.
/// Chunks that are already moved also need to get dropped.
/// The whole process is done through 3 independent sub-services, each will be
/// independently correct. To do so, we need to keep track of a state for each
/// chunk of the mutable buffer.
///
/// # State's values:
///   * `open`: this chunk is still accepting ingesting data and should not be
///     moved.
///   * `closing`: this chunk is in a closing process which might accept more
///     writes or be merged with other chunks or be split into many chunks.
///   * `closed`: this chunk is closed and becomes immutable which means it is
///     eligible to move to read buffer.
///   * `moving`: this chunk is in the moving-process to read buffer. All tables
///     of the chunk will be moved in parallel in different tasks.
///
/// # Sub-services
/// For each DB, four major sub-services will be spawned, each repeat their
/// below duty cycle after some sleep.
///   * `move_chunks`: to move "closed" chunks (eligible chunks) of mutable
///     buffer to read buffer. Before the process starts, the "closed" chunk
///     will be advanced to "moving".
///   * `move_moving_chunks`: to move chunks that have been moved & marked
///     "moving" but either failed or still running after a while.
///   * `drop_successful_moving_chunks`: to drop chunks whose tables have been
///     moved into read buffer.
async fn run_chunk_movers(server: Arc<AppServer<ConnectionManager>>) {
    // TODO
    // (1). DBs must be read inside each service below to make sure new DB and
    // dropped DBs are included (2).Instead of calling server.db_names_sorted()
    // there should probably be a server.dbs() method added to the DatabaseStore
    // trait. I imagine it would look something like:      async fn dbs(&self)
    // -> Vec<Arc<Self::Database>>;

    let database_names = server.db_names_sorted();
    for name in database_names {
        let db = match server.db_or_create(&name).await {
            Ok(db) => db,
            Err(e) => {
                // TODO - some errors should probably be ignored, e.g., if
                // the database doesn't exist due to a race between this and
                // above?
                warn!(error= ?e, error_message = ?e.to_string(), db_name = ?name, "Database not found");
                continue;
            }
        };

        // Move "closed" chunks
        let service_db = Arc::clone(&db);
        tokio::task::spawn(async move { service_db.move_chunks().await });

        // Move "moving" chunks
        // TODO: need to discuss this further with Edd and see if we can use
        // anything from Raphael's task trackers & cancel a long-running task
        // let service_db = Arc::clone(&db);
        // tokio::task::spawn(async move { service_db.move_moving_chunks().await } );

        // Advance "moving" chunks to "moved"
        let service_db = Arc::clone(&db);
        tokio::task::spawn(async move { service_db.drop_successful_moving_chunks().await });
    }
}

impl TryFrom<&RunConfig> for ObjectStore {
    type Error = Error;

    fn try_from(config: &RunConfig) -> Result<Self, Self::Error> {
        match config.object_store {
            Some(ObjStoreOpt::Memory) | None => {
                Ok(Self::new_in_memory(object_store::memory::InMemory::new()))
            }

            Some(ObjStoreOpt::Google) => {
                match (
                    config.bucket.as_ref(),
                    config.google_service_account.as_ref(),
                ) {
                    (Some(bucket), Some(service_account)) => Ok(Self::new_google_cloud_storage(
                        GoogleCloudStorage::new(service_account, bucket),
                    )),
                    (bucket, service_account) => {
                        let mut missing_args = vec![];

                        if bucket.is_none() {
                            missing_args.push("bucket");
                        }
                        if service_account.is_none() {
                            missing_args.push("google-service-account");
                        }
                        MissingObjectStoreConfig {
                            object_store: ObjStoreOpt::Google,
                            missing: missing_args.join(", "),
                        }
                        .fail()
                    }
                }
            }

            Some(ObjStoreOpt::S3) => {
                match (
                    config.bucket.as_ref(),
                    config.aws_access_key_id.as_ref(),
                    config.aws_secret_access_key.as_ref(),
                    config.aws_default_region.as_str(),
                ) {
                    (Some(bucket), Some(key_id), Some(secret_key), region) => {
                        Ok(Self::new_amazon_s3(
                            AmazonS3::new(key_id, secret_key, region, bucket)
                                .context(InvalidS3Config)?,
                        ))
                    }
                    (bucket, key_id, secret_key, _) => {
                        let mut missing_args = vec![];

                        if bucket.is_none() {
                            missing_args.push("bucket");
                        }
                        if key_id.is_none() {
                            missing_args.push("aws-access-key-id");
                        }
                        if secret_key.is_none() {
                            missing_args.push("aws-secret-access-key");
                        }

                        MissingObjectStoreConfig {
                            object_store: ObjStoreOpt::S3,
                            missing: missing_args.join(", "),
                        }
                        .fail()
                    }
                }
            }

            Some(ObjStoreOpt::Azure) => {
                match (
                    config.bucket.as_ref(),
                    config.azure_storage_account.as_ref(),
                    config.azure_storage_access_key.as_ref(),
                ) {
                    (Some(bucket), Some(storage_account), Some(access_key)) => {
                        Ok(Self::new_microsoft_azure(MicrosoftAzure::new(
                            storage_account,
                            access_key,
                            bucket,
                        )))
                    }
                    (bucket, storage_account, access_key) => {
                        let mut missing_args = vec![];

                        if bucket.is_none() {
                            missing_args.push("bucket");
                        }
                        if storage_account.is_none() {
                            missing_args.push("azure-storage-account");
                        }
                        if access_key.is_none() {
                            missing_args.push("azure-storage-access-key");
                        }

                        MissingObjectStoreConfig {
                            object_store: ObjStoreOpt::Azure,
                            missing: missing_args.join(", "),
                        }
                        .fail()
                    }
                }
            }

            Some(ObjStoreOpt::File) => match config.database_directory.as_ref() {
                Some(db_dir) => {
                    fs::create_dir_all(db_dir)
                        .context(CreatingDatabaseDirectory { path: db_dir })?;
                    Ok(Self::new_file(object_store::disk::File::new(&db_dir)))
                }
                None => MissingObjectStoreConfig {
                    object_store: ObjStoreOpt::File,
                    missing: "data-dir",
                }
                .fail(),
            },
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use object_store::ObjectStoreIntegration;
    use structopt::StructOpt;
    use tempfile::TempDir;

    #[test]
    fn default_object_store_is_memory() {
        let config = RunConfig::from_iter_safe(&["server"]).unwrap();

        let object_store = ObjectStore::try_from(&config).unwrap();

        assert!(matches!(
            object_store,
            ObjectStore(ObjectStoreIntegration::InMemory(_))
        ));
    }

    #[test]
    fn explicitly_set_object_store_to_memory() {
        let config = RunConfig::from_iter_safe(&["server", "--object-store", "memory"]).unwrap();

        let object_store = ObjectStore::try_from(&config).unwrap();

        assert!(matches!(
            object_store,
            ObjectStore(ObjectStoreIntegration::InMemory(_))
        ));
    }

    #[test]
    fn valid_s3_config() {
        let config = RunConfig::from_iter_safe(&[
            "server",
            "--object-store",
            "s3",
            "--bucket",
            "mybucket",
            "--aws-access-key-id",
            "NotARealAWSAccessKey",
            "--aws-secret-access-key",
            "NotARealAWSSecretAccessKey",
        ])
        .unwrap();

        let object_store = ObjectStore::try_from(&config).unwrap();

        assert!(matches!(
            object_store,
            ObjectStore(ObjectStoreIntegration::AmazonS3(_))
        ));
    }

    #[test]
    fn s3_config_missing_params() {
        let config = RunConfig::from_iter_safe(&["server", "--object-store", "s3"]).unwrap();

        let err = ObjectStore::try_from(&config).unwrap_err().to_string();

        assert_eq!(
            err,
            "Specified S3 for the object store, required configuration missing for \
            bucket, aws-access-key-id, aws-secret-access-key"
        );
    }

    #[test]
    fn valid_google_config() {
        let config = RunConfig::from_iter_safe(&[
            "server",
            "--object-store",
            "google",
            "--bucket",
            "mybucket",
            "--google-service-account",
            "~/Not/A/Real/path.json",
        ])
        .unwrap();

        let object_store = ObjectStore::try_from(&config).unwrap();

        assert!(matches!(
            object_store,
            ObjectStore(ObjectStoreIntegration::GoogleCloudStorage(_))
        ));
    }

    #[test]
    fn google_config_missing_params() {
        let config = RunConfig::from_iter_safe(&["server", "--object-store", "google"]).unwrap();

        let err = ObjectStore::try_from(&config).unwrap_err().to_string();

        assert_eq!(
            err,
            "Specified Google for the object store, required configuration missing for \
            bucket, google-service-account"
        );
    }

    #[test]
    fn valid_azure_config() {
        let config = RunConfig::from_iter_safe(&[
            "server",
            "--object-store",
            "azure",
            "--bucket",
            "mybucket",
            "--azure-storage-account",
            "NotARealStorageAccount",
            "--azure-storage-access-key",
            "NotARealKey",
        ])
        .unwrap();

        let object_store = ObjectStore::try_from(&config).unwrap();

        assert!(matches!(
            object_store,
            ObjectStore(ObjectStoreIntegration::MicrosoftAzure(_))
        ));
    }

    #[test]
    fn azure_config_missing_params() {
        let config = RunConfig::from_iter_safe(&["server", "--object-store", "azure"]).unwrap();

        let err = ObjectStore::try_from(&config).unwrap_err().to_string();

        assert_eq!(
            err,
            "Specified Azure for the object store, required configuration missing for \
            bucket, azure-storage-account, azure-storage-access-key"
        );
    }

    #[test]
    fn valid_file_config() {
        let root = TempDir::new().unwrap();

        let config = RunConfig::from_iter_safe(&[
            "server",
            "--object-store",
            "file",
            "--data-dir",
            root.path().to_str().unwrap(),
        ])
        .unwrap();

        let object_store = ObjectStore::try_from(&config).unwrap();

        assert!(matches!(
            object_store,
            ObjectStore(ObjectStoreIntegration::File(_))
        ));
    }

    #[test]
    fn file_config_missing_params() {
        let config = RunConfig::from_iter_safe(&["server", "--object-store", "file"]).unwrap();

        let err = ObjectStore::try_from(&config).unwrap_err().to_string();

        assert_eq!(
            err,
            "Specified File for the object store, required configuration missing for \
            data-dir"
        );
    }
}
