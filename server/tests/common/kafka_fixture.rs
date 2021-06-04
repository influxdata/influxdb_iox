use once_cell::sync::OnceCell;
use std::{
    fs::File,
    process::{Child, Command, Stdio},
    sync::{Arc, Weak},
};

static KAFKA_PORT: &str = "9093";

#[macro_export]
/// If `TEST_INTEGRATION` is set and Kafka is available via `docker`, set up the Kafka service as
/// requested and return it to the caller.
///
/// If `TEST_INTEGRATION` is not set, skip the calling test by returning early.
macro_rules! maybe_skip_integration {
    ($server_fixture:expr) => {{
        let command = "docker";

        match (
            std::process::Command::new("which")
                .arg(command)
                .stdout(std::process::Stdio::null())
                .status()
                .expect("should be able to run `which`")
                .success(),
            std::env::var("TEST_INTEGRATION").is_ok(),
        ) {
            (true, true) => $server_fixture,
            (false, true) => {
                panic!(
                    "TEST_INTEGRATION is set which requires running integration tests, but \
                     `{}` is not available",
                    command
                )
            }
            _ => {
                eprintln!(
                    "skipping integration test - set the TEST_INTEGRATION environment variable \
                     and install `{}` to run",
                    command
                );
                return;
            }
        }
    }};
}

/// Represents a Kafka server that has been started and is available for testing.
pub struct KafkaFixture {
    server: Arc<KafkaServer>,
}

impl KafkaFixture {
    /// Create a new Kafka fixture and wait for it to be ready. This
    /// is called "create" rather than new because it is async and
    /// waits. The shared Kafka instance can be used immediately.
    ///
    /// This is currently implemented as a singleton so all tests *must*
    /// use a new IOx database to not interfere with the existing database.
    pub async fn create_shared() -> Self {
        // Try and reuse the same shared server, if there is already
        // one present
        static SHARED_SERVER: OnceCell<parking_lot::Mutex<Weak<KafkaServer>>> = OnceCell::new();

        let shared_server = SHARED_SERVER.get_or_init(|| parking_lot::Mutex::new(Weak::new()));

        let mut shared_server = shared_server.lock();

        // is a shared server already present?
        let server = match shared_server.upgrade() {
            Some(server) => server,
            None => {
                // if not, create one
                // TODO: ensure the server is ready, I can't find a simple way to
                // do this with Kafka?
                let server = Arc::new(KafkaServer::new());

                // save a reference for other threads that may want to
                // use this server, but don't prevent it from being
                // destroyed when going out of scope
                *shared_server = Arc::downgrade(&server);
                server
            }
        };
        std::mem::drop(shared_server);

        Self { server }
    }

    /// Return the connection string for the "bootstrap.servers" Kafka configuration
    pub fn connection(&self) -> &str {
        &self.server.connection
    }
}

struct KafkaServer {
    /// How to connect to this instance
    connection: String,
    /// Handle to the server process being controlled
    server_process: Child,
}

impl KafkaServer {
    fn new() -> Self {
        let temp_dir = test_helpers::tmp_dir().unwrap();

        let mut log_path = temp_dir.path().to_path_buf();
        log_path.push("influxdb_kafka_test_server.log");

        println!("****************");
        println!("Server Logging to {:?}", log_path);
        println!("****************");
        let log_file = File::create(log_path).expect("Opening log file");

        let stdout_log_file = log_file
            .try_clone()
            .expect("cloning file handle for stdout");
        let stderr_log_file = log_file;

        // start up kafka
        Command::new("docker")
            .arg("compose")
            .arg("-f")
            // TODO: this needs to work no matter where you happen to run `cargo test` from
            // and the path needs to live one place
            .arg("../docker/ci-kafka-docker-compose.yml")
            .arg("up")
            .arg("-d")
            .spawn()
            .expect("starting of docker kafka/zookeeper process");

        let server_process = Command::new("docker")
            .arg("compose")
            .arg("-f")
            // TODO: this needs to work no matter where you happen to run `cargo test` from
            // and the path needs to live one place
            .arg("../docker/ci-kafka-docker-compose.yml")
            .arg("logs")
            // redirect output to log file
            .stdout(stdout_log_file)
            .stderr(stderr_log_file)
            .spawn()
            .expect("starting of docker logs process");

        Self {
            connection: format!("127.0.0.1:{}", KAFKA_PORT),
            server_process,
        }
    }
}

impl std::fmt::Display for KafkaServer {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::result::Result<(), std::fmt::Error> {
        write!(f, "KafkaServer (connection: {})", self.connection)
    }
}

impl Drop for KafkaServer {
    fn drop(&mut self) {
        // stop logging
        self.server_process
            .kill()
            .expect("Should have been able to kill the test server");

        // stop kafka
        Command::new("docker")
            .arg("compose")
            .arg("-f")
            // TODO: this needs to work no matter where you happen to run `cargo test` from
            // and the path needs to live one place
            .arg("../docker/ci-kafka-docker-compose.yml")
            .arg("down")
            .stdout(Stdio::null())
            .status()
            .expect("stopping of docker kafka/zookeeper process");
    }
}
