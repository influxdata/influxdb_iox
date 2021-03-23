use crate::commands::{
    logging::LoggingLevel,
    run::{Config, ObjectStore as ObjStoreOpt},
};
use futures::{future::FusedFuture, pin_mut, FutureExt};
use hyper::server::conn::AddrIncoming;
use object_store::{
    self, aws::AmazonS3, azure::MicrosoftAzure, gcp::GoogleCloudStorage, ObjectStore,
};
use panic_logging::SendPanicsToTracing;
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
    ServingRPC { source: tonic::transport::Error },

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

/// On unix platforms we want to intercept SIGINT and SIGTERM
/// This method returns if either are signalled
#[cfg(unix)]
async fn wait_for_signal() {
    use tokio::signal::unix::{signal, SignalKind};
    let mut term = signal(SignalKind::terminate()).expect("failed to register signal handler");
    let mut int = signal(SignalKind::interrupt()).expect("failed to register signal handler");

    tokio::select! {
        _ = term.recv() => info!("Received SIGTERM"),
        _ = int.recv() => info!("Received SIGINT"),
    }
}

#[cfg(windows)]
/// ctrl_c is the cross-platform way to intercept the equivalent of SIGINT
/// This method returns if this occurs
async fn wait_for_signal() {
    let _ = tokio::signal::ctrl_c().await;
}

/// This is the entry point for the IOx server. `config` represents
/// command line arguments, if any
///
/// The logging_level passed in is the global setting (e.g. if -v or
/// -vv was passed in before 'server')
pub async fn main(logging_level: LoggingLevel, config: Config) -> Result<()> {
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

    // An internal shutdown token for internal workers
    let internal_shutdown = tokio_util::sync::CancellationToken::new();

    // Construct a token to trigger shutdown of API services
    let frontend_shutdown = internal_shutdown.child_token();

    // Construct and start up gRPC server
    let grpc_bind_addr = config.grpc_bind_address;
    let socket = tokio::net::TcpListener::bind(grpc_bind_addr)
        .await
        .context(StartListeningGrpc { grpc_bind_addr })?;

    let grpc_server = rpc::serve(socket, Arc::clone(&app_server), frontend_shutdown.clone()).fuse();

    info!(bind_address=?grpc_bind_addr, "gRPC server listening");

    let bind_addr = config.http_bind_address;
    let addr = AddrIncoming::bind(&bind_addr).context(StartListeningHttp { bind_addr })?;

    let http_server = http::serve(addr, Arc::clone(&app_server), frontend_shutdown.clone()).fuse();
    info!(bind_address=?bind_addr, "HTTP server listening");

    let git_hash = option_env!("GIT_HASH").unwrap_or("UNKNOWN");
    info!(git_hash, "InfluxDB IOx server ready");

    // Get IOx background worker task
    let background_worker = app_server
        .background_worker(internal_shutdown.clone())
        .fuse();

    // Shutdown signal
    let signal = wait_for_signal().fuse();

    // There are two different select macros - tokio::select and futures::select
    //
    // tokio::select takes ownership of the passed future "moving" it into the
    // select block. This works well when not running select inside a loop, or
    // when using a future that can be dropped and recreated, often the case
    // with tokio's futures e.g. `channel.recv()`
    //
    // futures::select is more flexible as it doesn't take ownership of the provided
    // future. However, to safely provide this it imposes some additional
    // requirements
    //
    // All passed futures must implement FusedFuture - it is IB to poll a future
    // that has returned Poll::Ready(_). A FusedFuture has an is_terminated()
    // method that indicates if it is safe to poll - e.g. false if it has
    // returned Poll::Ready(_). futures::select uses this to implement its
    // functionality. futures::FutureExt adds a fuse() method that
    // wraps an arbitrary future and makes it a FusedFuture
    //
    // The additional requirement of futures::select is that if the future passed
    // outlives the select block, it must be Unpin or already Pinned

    // pin_mut constructs a Pin<&mut T> from a T by preventing moving the T
    // from the current stack frame and constructing a Pin<&mut T> to it
    pin_mut!(signal);
    pin_mut!(background_worker);
    pin_mut!(grpc_server);
    pin_mut!(http_server);

    // Return the first error encountered
    let mut res = Ok(());

    // Graceful shutdown can be triggered by sending SIGINT or SIGTERM to the
    // process, or by a background task exiting - most likely with an error
    //
    // Graceful shutdown should then proceed in the following order
    // 1. Stop accepting new HTTP and gRPC requests and drain existing connections
    // 2. Trigger shutdown of internal background workers loops
    //
    // This is important to ensure background tasks, such as polling the tracker
    // registry, don't exit before HTTP and gRPC requests dependent on them
    while !grpc_server.is_terminated() && !http_server.is_terminated() {
        futures::select! {
            _ = signal => info!("Shutdown requested"),
            _ = background_worker => {
                info!("background worker shutdown prematurely");
                internal_shutdown.cancel();
            },
            result = grpc_server => match result {
                Ok(_) => info!("gRPC server shutdown"),
                Err(error) => {
                    error!(%error, "gRPC server error");
                    res = res.and(Err(Error::ServingRPC{source: error}))
                }
            },
            result = http_server => match result {
                Ok(_) => info!("HTTP server shutdown"),
                Err(error) => {
                    error!(%error, "HTTP server error");
                    res = res.and(Err(Error::ServingHttp{source: error}))
                }
            },
        }

        frontend_shutdown.cancel()
    }

    info!("frontend shutdown completed");
    internal_shutdown.cancel();
    background_worker.await;

    info!("server completed shutting down");

    res
}

impl TryFrom<&Config> for ObjectStore {
    type Error = Error;

    fn try_from(config: &Config) -> Result<Self, Self::Error> {
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
                    (Some(bucket), key_id, secret_key, region) => Ok(Self::new_amazon_s3(
                        AmazonS3::new(key_id, secret_key, region, bucket)
                            .context(InvalidS3Config)?,
                    )),
                    (bucket, _, _, _) => {
                        let mut missing_args = vec![];

                        if bucket.is_none() {
                            missing_args.push("bucket");
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
        let config = Config::from_iter_safe(&["server"]).unwrap();

        let object_store = ObjectStore::try_from(&config).unwrap();

        assert!(matches!(
            object_store,
            ObjectStore(ObjectStoreIntegration::InMemory(_))
        ));
    }

    #[test]
    fn explicitly_set_object_store_to_memory() {
        let config = Config::from_iter_safe(&["server", "--object-store", "memory"]).unwrap();

        let object_store = ObjectStore::try_from(&config).unwrap();

        assert!(matches!(
            object_store,
            ObjectStore(ObjectStoreIntegration::InMemory(_))
        ));
    }

    #[test]
    fn valid_s3_config() {
        let config = Config::from_iter_safe(&[
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
        let config = Config::from_iter_safe(&["server", "--object-store", "s3"]).unwrap();

        let err = ObjectStore::try_from(&config).unwrap_err().to_string();

        assert_eq!(
            err,
            "Specified S3 for the object store, required configuration missing for bucket"
        );
    }

    #[test]
    fn valid_google_config() {
        let config = Config::from_iter_safe(&[
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
        let config = Config::from_iter_safe(&["server", "--object-store", "google"]).unwrap();

        let err = ObjectStore::try_from(&config).unwrap_err().to_string();

        assert_eq!(
            err,
            "Specified Google for the object store, required configuration missing for \
            bucket, google-service-account"
        );
    }

    #[test]
    fn valid_azure_config() {
        let config = Config::from_iter_safe(&[
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
        let config = Config::from_iter_safe(&["server", "--object-store", "azure"]).unwrap();

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

        let config = Config::from_iter_safe(&[
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
        let config = Config::from_iter_safe(&["server", "--object-store", "file"]).unwrap();

        let err = ObjectStore::try_from(&config).unwrap_err().to_string();

        assert_eq!(
            err,
            "Specified File for the object store, required configuration missing for \
            data-dir"
        );
    }
}
