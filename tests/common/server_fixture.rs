use std::{fs::File, str};
use std::{
    num::NonZeroU32,
    process::{Child, Command},
};

use assert_cmd::prelude::*;
use futures::prelude::*;
use std::time::Duration;
use tempfile::TempDir;

type Error = Box<dyn std::error::Error + Send + Sync + 'static>;
type Result<T, E = Error> = std::result::Result<T, E>;

// These port numbers are chosen to not collide with a development ioxd server
// running locally.
// TODO(786): allocate random free ports instead of hardcoding.
// TODO(785): we cannot use localhost here.
macro_rules! http_bind_addr {
    () => {
        "127.0.0.1:8090"
    };
}
macro_rules! grpc_bind_addr {
    () => {
        "127.0.0.1:8092"
    };
}

const HTTP_BIND_ADDR: &str = http_bind_addr!();
const GRPC_BIND_ADDR: &str = grpc_bind_addr!();

const HTTP_BASE: &str = concat!("http://", http_bind_addr!());
const IOX_API_V1_BASE: &str = concat!("http://", http_bind_addr!(), "/iox/api/v1");
pub const GRPC_URL_BASE: &str = concat!("http://", grpc_bind_addr!(), "/");

pub const TOKEN: &str = "InfluxDB IOx doesn't have authentication yet";

use std::sync::Arc;

use once_cell::sync::OnceCell;
use tokio::sync::Mutex;

/// Represents a server that has been started and is available for
/// testing.
pub struct ServerFixture {
    server: Arc<TestServer>,
}

impl ServerFixture {
    /// Create a new server fixture and wait for it to be ready. This
    /// is called "create" rather than new because it is async and waits
    ///
    /// This is currently implemented as a singleton so all tests *must*
    /// use a new database and not interfere with the existing database.
    pub async fn create_shared() -> Self {
        static SERVER: OnceCell<Arc<TestServer>> = OnceCell::new();

        let server = Arc::clone(SERVER.get_or_init(|| {
            let server = TestServer::new().expect("Could start test server");
            Arc::new(server)
        }));

        // ensure the server is ready
        server.wait_until_ready().await;
        ServerFixture { server }
    }

    /// Return a channel connected to the gRPC API. Panics if the
    /// server is not yet up
    pub async fn grpc_channel(&self) -> tonic::transport::Channel {
        self.server
            .grpc_channel()
            .await
            .expect("The server should be up")
    }

    /// Return the http base URL for the HTTP API
    pub fn http_base(&self) -> &str {
        self.server.http_base()
    }

    /// Return the base URL for the IOx V1 API
    pub fn iox_api_v1_base(&self) -> &str {
        self.server.iox_api_v1_base()
    }
}

struct TestServer {
    /// Is the server ready to accept connections?
    ready: Mutex<bool>,

    server_process: Child,

    // The temporary directory **must** be last so that it is
    // dropped after the database closes.
    #[allow(dead_code)]
    dir: TempDir,
}

impl TestServer {
    fn new() -> Result<Self> {
        let ready = Mutex::new(false);

        let dir = test_helpers::tmp_dir().unwrap();
        // Make a log file in the temporary dir (don't auto delete it to help debugging
        // efforts)
        let mut log_path = std::env::temp_dir();
        log_path.push("server_fixture.log");

        println!("****************");
        println!("Server Logging to {:?}", log_path);
        println!("****************");
        let log_file = File::create(log_path).expect("Opening log file");

        let stdout_log_file = log_file
            .try_clone()
            .expect("cloning file handle for stdout");
        let stderr_log_file = log_file;

        let server_process = Command::cargo_bin("influxdb_iox")
            .unwrap()
            // Can enable for debbugging
            //.arg("-vv")
            .env("INFLUXDB_IOX_ID", "1")
            .env("INFLUXDB_IOX_BIND_ADDR", HTTP_BIND_ADDR)
            .env("INFLUXDB_IOX_GRPC_BIND_ADDR", GRPC_BIND_ADDR)
            // redirect output to log file
            .stdout(stdout_log_file)
            .stderr(stderr_log_file)
            .spawn()
            .unwrap();

        Ok(Self {
            ready,
            dir,
            server_process,
        })
    }

    #[allow(dead_code)]
    fn restart(&mut self) -> Result<()> {
        self.server_process.kill().unwrap();
        self.server_process.wait().unwrap();
        self.server_process = Command::cargo_bin("influxdb_iox")
            .unwrap()
            // Can enable for debbugging
            //.arg("-vv")
            .env("INFLUXDB_IOX_DB_DIR", self.dir.path())
            .env("INFLUXDB_IOX_ID", "1")
            .spawn()
            .unwrap();
        Ok(())
    }

    async fn wait_until_ready(&self) {
        let mut ready = self.ready.lock().await;
        if *ready {
            return;
        }

        // Poll the RPC and HTTP servers separately as they listen on
        // different ports but both need to be up for the test to run
        let try_grpc_connect = async {
            let mut interval = tokio::time::interval(Duration::from_millis(500));

            loop {
                match self.grpc_channel().await {
                    Ok(channel) => {
                        println!("Successfully connected to server");

                        let mut health = influxdb_iox_client::health::Client::new(channel);

                        match health.check_storage().await {
                            Ok(_) => {
                                println!("Storage service is running");
                                return;
                            }
                            Err(e) => {
                                println!("Error checking storage service status: {}", e);
                            }
                        }
                    }
                    Err(e) => {
                        println!("Waiting for gRPC API to be up: {}", e);
                    }
                }
                interval.tick().await;
            }
        };

        let try_http_connect = async {
            let client = reqwest::Client::new();
            let url = format!("{}/health", HTTP_BASE);
            let mut interval = tokio::time::interval(Duration::from_millis(500));
            loop {
                match client.get(&url).send().await {
                    Ok(resp) => {
                        println!("Successfully got a response from HTTP: {:?}", resp);
                        return;
                    }
                    Err(e) => {
                        println!("Waiting for HTTP server to be up: {}", e);
                    }
                }
                interval.tick().await;
            }
        };

        let pair = future::join(try_http_connect, try_grpc_connect);

        let capped_check = tokio::time::timeout(Duration::from_secs(3), pair);

        match capped_check.await {
            Ok(_) => println!("Server is up correctly"),
            Err(e) => println!("WARNING: server was not ready: {}", e),
        }

        // Set the writer id, if requested (TODO if requested)
        let channel = self.grpc_channel().await.expect("gRPC should be running");
        let mut management_client = influxdb_iox_client::management::Client::new(channel);

        let id = NonZeroU32::new(42).expect("42 is non zero, among its other properties");

        management_client
            .update_writer_id(id)
            .await
            .expect("set ID failed");

        *ready = true;
    }

    /// Create a connection channel for the gRPR endpoing
    async fn grpc_channel(
        &self,
    ) -> influxdb_iox_client::connection::Result<tonic::transport::Channel> {
        influxdb_iox_client::connection::Builder::default()
            .build(GRPC_URL_BASE)
            .await
    }

    fn http_base(&self) -> &str {
        HTTP_BASE
    }

    fn iox_api_v1_base(&self) -> &str {
        IOX_API_V1_BASE
    }
}

impl Drop for TestServer {
    fn drop(&mut self) {
        self.server_process
            .kill()
            .expect("Should have been able to kill the test server");
    }
}
