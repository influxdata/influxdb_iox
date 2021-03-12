use assert_cmd::prelude::*;
use std::{
    fs::File,
    num::NonZeroU32,
    process::{Child, Command},
    str,
    sync::{
        atomic::{AtomicUsize, Ordering::SeqCst},
        Weak,
    },
};

use futures::prelude::*;
use std::time::Duration;
use tempfile::TempDir;

type Error = Box<dyn std::error::Error + Send + Sync + 'static>;
type Result<T, E = Error> = std::result::Result<T, E>;

// These port numbers are chosen to not collide with a development ioxd server
// running locally.
// TODO(786): allocate random free ports instead of hardcoding.
// TODO(785): we cannot use localhost here.
static NEXT_PORT: AtomicUsize = AtomicUsize::new(8090);

/// This structure contains all the addresses a test server should use
struct BindAddresses {
    http_port: usize,
    grpc_port: usize,

    http_bind_addr: String,
    grpc_bind_addr: String,

    http_base: String,
    iox_api_v1_base: String,
    grpc_base: String,
}

impl BindAddresses {
    /// return a new port assignment suitable for this test's use
    fn new() -> Self {
        let http_port = NEXT_PORT.fetch_add(1, SeqCst);
        let grpc_port = NEXT_PORT.fetch_add(1, SeqCst);

        let http_bind_addr = format!("127.0.0.1:{}", http_port);
        let grpc_bind_addr = format!("127.0.0.1:{}", grpc_port);

        let http_base = format!("http://{}", http_bind_addr);
        let iox_api_v1_base = format!("http://{}/iox/api/v1", http_bind_addr);
        let grpc_base = format!("http://{}", grpc_bind_addr);

        Self {
            http_port,
            grpc_port,
            http_bind_addr,
            grpc_bind_addr,
            http_base,
            iox_api_v1_base,
            grpc_base,
        }
    }
}

const TOKEN: &str = "InfluxDB IOx doesn't have authentication yet";

use std::sync::Arc;

use once_cell::sync::OnceCell;
use tokio::sync::Mutex;

/// Represents a server that has been started and is available for
/// testing.
pub struct ServerFixture {
    server: Arc<TestServer>,
    grpc_channel: tonic::transport::Channel,
}

/// Specifieds should we configure a server initially
enum InitialConfig {
    /// Set the writer id to something so it can accept writes
    SetWriterId,

    /// leave the writer id empty so the test can set it
    None,
}

impl ServerFixture {
    /// Create a new server fixture and wait for it to be ready. This
    /// is called "create" rather than new because it is async and
    /// waits. The shared database is configured with a writer id and
    /// can be used immediately
    ///
    /// This is currently implemented as a singleton so all tests *must*
    /// use a new database and not interfere with the existing database.
    pub async fn create_shared() -> Self {
        // Try and reuse the same shared server, if there is already
        // one present
        static SHARED_SERVER: OnceCell<parking_lot::Mutex<Weak<TestServer>>> = OnceCell::new();

        let shared_server = SHARED_SERVER.get_or_init(|| parking_lot::Mutex::new(Weak::new()));

        let mut shared_server = shared_server.lock();

        // is a shared server already present?
        let server = match shared_server.upgrade() {
            Some(server) => server,
            None => {
                // if not, create one
                let server = TestServer::new().expect("Could start test server");
                let server = Arc::new(server);

                // ensure the server is ready
                server.wait_until_ready(InitialConfig::SetWriterId).await;
                // save a reference for other threads that may want to
                // use this server, but don't prevent it from being
                // destroyed when going out of scope
                *shared_server = Arc::downgrade(&server);
                server
            }
        };
        std::mem::drop(shared_server);

        Self::create_common(server).await
    }

    /// Create a new server fixture and wait for it to be ready. This
    /// is called "create" rather than new because it is async and
    /// waits.  The database is left unconfigured (no writer id) and
    /// is not shared with any other tests.
    pub async fn create_single_use() -> Self {
        let server = TestServer::new().expect("Could start test server");
        let server = Arc::new(server);

        // ensure the server is ready
        server.wait_until_ready(InitialConfig::None).await;
        Self::create_common(server).await
    }

    async fn create_common(server: Arc<TestServer>) -> Self {
        let grpc_channel = server
            .grpc_channel()
            .await
            .expect("The server should have been up");

        ServerFixture {
            server,
            grpc_channel,
        }
    }

    /// Return a channel connected to the gRPC API. Panics if the
    /// server is not yet up
    pub fn grpc_channel(&self) -> tonic::transport::Channel {
        self.grpc_channel.clone()
    }

    /// Return the url base of the grpc management API
    pub fn grpc_base(&self) -> &str {
        &self.server.addrs().grpc_base
    }

    /// Return the http base URL for the HTTP API
    pub fn http_base(&self) -> &str {
        &self.server.addrs().http_base
    }

    /// Return the base URL for the IOx V1 API
    pub fn iox_api_v1_base(&self) -> &str {
        &self.server.addrs().iox_api_v1_base
    }

    /// Return an a http client suitable suitable for communicating with this
    /// server
    pub fn influxdb2_client(&self) -> influxdb2_client::Client {
        influxdb2_client::Client::new(self.http_base(), TOKEN)
    }

    /// Return a management client  suitable for communicating with this
    /// server
    pub fn management_client(&self) -> influxdb_iox_client::management::Client {
        influxdb_iox_client::management::Client::new(self.grpc_channel())
    }

    /// Return a write client suitable for communicating with this
    /// server
    pub fn write_client(&self) -> influxdb_iox_client::write::Client {
        influxdb_iox_client::write::Client::new(self.grpc_channel())
    }

    /// Return a flight client suitable for communicating with this
    /// server
    pub fn flight_client(&self) -> influxdb_iox_client::flight::Client {
        influxdb_iox_client::flight::Client::new(self.grpc_channel())
    }
}

#[derive(Debug)]
/// Represents the current known state of a TestServer
enum ServerState {
    Started,
    Ready,
    Error,
}

struct TestServer {
    /// Is the server ready to accept connections?
    ready: Mutex<ServerState>,

    /// Handle to the server process being controlled
    server_process: Child,

    /// Which ports this server should use
    addrs: BindAddresses,

    // The temporary directory **must** be last so that it is
    // dropped after the database closes.
    dir: TempDir,
}

impl TestServer {
    fn new() -> Result<Self> {
        let addrs = BindAddresses::new();
        let ready = Mutex::new(ServerState::Started);

        let dir = test_helpers::tmp_dir().unwrap();
        // Make a log file in the temporary dir (don't auto delete it to help debugging
        // efforts)
        let mut log_path = std::env::temp_dir();
        log_path.push(format!(
            "server_fixture_{}_{}.log",
            addrs.http_port, addrs.grpc_port
        ));

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
            .arg("run")
            // Can enable for debugging
            //.arg("-vv")
            .env("INFLUXDB_IOX_BIND_ADDR", &addrs.http_bind_addr)
            .env("INFLUXDB_IOX_GRPC_BIND_ADDR", &addrs.grpc_bind_addr)
            // redirect output to log file
            .stdout(stdout_log_file)
            .stderr(stderr_log_file)
            .spawn()
            .unwrap();

        Ok(Self {
            ready,
            dir,
            addrs,
            server_process,
        })
    }

    #[allow(dead_code)]
    fn restart(&mut self) -> Result<()> {
        self.server_process.kill().unwrap();
        self.server_process.wait().unwrap();
        self.server_process = Command::cargo_bin("influxdb_iox")
            .unwrap()
            .arg("run")
            // Can enable for debugging
            //.arg("-vv")
            .env("INFLUXDB_IOX_DB_DIR", self.dir.path())
            .spawn()
            .unwrap();
        Ok(())
    }

    async fn wait_until_ready(&self, initial_config: InitialConfig) {
        let mut ready = self.ready.lock().await;
        match *ready {
            ServerState::Started => {} // first time, need to try and start it
            ServerState::Ready => {
                return;
            }
            ServerState::Error => {
                panic!("Server was previously found to be in Error, aborting");
            }
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
            let url = format!("{}/health", self.addrs().http_base);
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
            Ok(_) => {
                println!("Successfully started {}", self);
                *ready = ServerState::Ready;
            }
            Err(e) => {
                // tell others that this server had some problem
                *ready = ServerState::Error;
                std::mem::drop(ready);
                panic!("Server was not ready in required time: {}", e);
            }
        }

        // Set the writer id, if requested; otherwise default to the default writer id:
        // 1.
        let id = match initial_config {
            InitialConfig::SetWriterId => {
                NonZeroU32::new(42).expect("42 is non zero, among its other properties")
            }
            InitialConfig::None => {
                NonZeroU32::new(1).expect("1 is non zero; the first one to be so, moreover")
            }
        };

        let channel = self.grpc_channel().await.expect("gRPC should be running");
        let mut management_client = influxdb_iox_client::management::Client::new(channel);

        if let Ok(id) = management_client.get_writer_id().await {
            // tell others that this server had some problem
            *ready = ServerState::Error;
            std::mem::drop(ready);
            panic!("Server already has an ID ({}); possibly a stray/orphan server from another test run.", id);
        }

        management_client
            .update_writer_id(id)
            .await
            .expect("set ID failed");
        println!("Set writer_id to {:?}", id);
    }

    /// Create a connection channel for the gRPR endpoing
    async fn grpc_channel(
        &self,
    ) -> influxdb_iox_client::connection::Result<tonic::transport::Channel> {
        influxdb_iox_client::connection::Builder::default()
            .build(&self.addrs().grpc_base)
            .await
    }

    fn addrs(&self) -> &BindAddresses {
        &self.addrs
    }
}

impl std::fmt::Display for TestServer {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::result::Result<(), std::fmt::Error> {
        write!(
            f,
            "TestServer (grpc {}, http {})",
            self.addrs().grpc_base,
            self.addrs().http_base
        )
    }
}

impl Drop for TestServer {
    fn drop(&mut self) {
        self.server_process
            .kill()
            .expect("Should have been able to kill the test server");
    }
}
