//! This module contains code that defines how Delorean servers talk to each other.
//! This includes read rules, write rules, and traits that abstract these methods
//! away for testing purposes.

#![deny(rust_2018_idioms)]

use std::{
    sync::Arc,
    collections::BTreeMap,
};

use delorean_storage::{DatabaseStore, Database};
use delorean_line_parser::ParsedLine;

use async_trait::async_trait;
use snafu::{OptionExt, ResultExt, Snafu};
use serde::{Deserialize, Serialize};
use serde_json::Result as SerdeResult;

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("Server error: {}", source))]
    ServerError {
        source: std::io::Error,
    },
    #[snafu(display("database not found: {}", db))]
    DatabaseNotFound{
        db: String,
    }
}

pub type Result<T, E = Error> = std::result::Result<T, E>;


/// Server is the container struct for how Delorean servers store data internally
/// as well as how they communicate with other Delorean servers. Each server
/// server will have one of these structs, which keeps track of all read and
/// write rules, hosts, and host groups.
pub struct Server<A: HostPool, B: DatabaseStore> {
    database_rules: BTreeMap<String, DatabaseRules>,
    local_store: B,
    host_pool: A,
}

/// The Server will ask the HostPool for connections to specific host pool ids.
/// These connections can be used to communicate with other Delorean servers.
/// This is implemented as a trait for dependency injection in testing.
#[async_trait]
pub trait HostPool {
    type Error: std::error::Error + Send + Sync + 'static;

    type RemoteServer: RemoteServer;

    async fn host(&self, connect: &str) -> Result<Arc<Self::RemoteServer>, Self::Error>;
}

/// The RemoteServer represents the API for communicating with other Delorean servers.
#[async_trait]
pub trait RemoteServer {
    type Error: std::error::Error + Send + Sync + 'static;

    async fn write(&self, db: &str, lines: &[ParsedLine<'_>]) -> Result<(), Self::Error>;
}

impl<A: HostPool, B: DatabaseStore> Server<A, B> {
    pub fn new(host_pool: A, local_store: B) -> Server<A, B> {
        Server{
            database_rules: BTreeMap::new(),
            local_store,
            host_pool,
        }
    }

    pub async fn write(&self, db: &str, lines: &[ParsedLine<'_>]) -> Result<()> {
        let local = self.local_store
            .db_or_create(db)
            .await
            .unwrap();

        // TODO: how to actually deal with this error?
        local.write_lines(lines).await.unwrap();
        Ok(())
    }
}

/// DatabaseRules contain the read and write rules for a single database
#[derive(Debug, Serialize, Deserialize)]
pub struct DatabaseRules {
    read: Vec<ReadRule>,
    write: Vec<WriteRule>,
}

/// The ReadRule specifies what other Delorean servers should be queried for
/// a given read. This could be a proxy to the other server, or it could contain
/// information about what partitions a remote server has, which the planner
/// can use to determine if the remote server should be included in the query.
#[derive(Debug, Serialize, Deserialize)]
pub struct ReadRule {
}

/// The WriteRule specifies how a write should be split across Delorean servers.
/// This could be a proxy to send all writes to a server, or it could split
/// writes up based on partitioning each individual row (line). Write rules can
/// also be used to shadow production servers or for replication.
#[derive(Debug, Serialize, Deserialize)]
pub struct WriteRule {
    #[serde(rename = "match")]
    matcher: WriteMatcher,
    // Use an ID here so we can update the hosts behind
    // a host group without updating the read or write rules
    target: Target,
}

/// The WriteMatcher specifies what rows in a write should be matched for the
/// WriteRule.
#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
enum WriteMatcher {
    #[serde(rename = "*")]
    All,
    Subset(Box<Matcher>),
}

/// Matcher specifies the rule against the table name and/or a predicate
/// against the row to determine if it matches the write rule.
#[derive(Debug, Serialize, Deserialize)]
struct Matcher {
    #[serde(flatten)]
    against: MatchAgainst,
    // TODO: make this work with delorean_storage::Predicate
    #[serde(skip_serializing_if = "Option::is_none")]
    predicate: Option<String>,
}

/// MatchAgainst looks at the table name of a row to determine if it should
/// match the write rule.
#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
enum MatchAgainst {
    #[serde(rename = "*")]
    All,
    Table(String),
    Regex(String),
}

/// The Target for a write rule can be either the local database or a host group
#[derive(Debug, Serialize, Deserialize)]
enum Target {
    Local,
    HostGroup(HostGroupId),
}

type HostGroupId = String;

#[derive(Debug, Serialize, Deserialize)]
pub struct HostGroup {
    id: HostGroupId,
    hosts: Vec<Host>,
    mode: Mode,
}

/// Mode indicates how writes should be sent within this host group.
#[derive(Debug, Serialize, Deserialize)]
enum Mode {
    /// Send to the primary first, or if failed, then to secondary
    Primary,
    /// Each write request goes to the next host in the group round robin style
    RoundRobin,
    /// Send the write to all hosts in the group, but return success after a quorum responds
    Quorum,
    /// Send the write to all hosts in the group and don't send a response until all have come back
    All,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct Host {
    name: String,
    role: Role,
}

#[derive(Debug, Serialize, Deserialize)]
enum Role {
    Primary,
    Secondary,
    Multi,
}

#[cfg(test)]
mod tests {
    use super::*;
    use delorean_storage::test::TestDatabaseStore;
    use delorean_line_parser::parse_lines;
    use snafu::{OptionExt, Snafu};
    use async_trait::async_trait;

//    type TestError = Box<dyn std::error::Error + Send + Sync + 'static>;
    type Result<T = (), E = TestError> = std::result::Result<T, E>;

    #[tokio::test(threaded_scheduler)]
    async fn write_rule_match_all_local() -> Result {
        let store = TestDatabaseStore::new();
        let pool = TestHostPool::new();
        let server = Server::new(pool, store);

        let lines: Vec<_> = parse_lines("cpu foo=1 10").map(|l| l.unwrap()).collect();
        server.write("foo", &lines).await.unwrap();
        Ok(())
    }

    fn write_rule_target_host_group() {

    }

    fn write_rule_match_table() {

    }

    fn write_rule_match_regex() {

    }

    fn write_rule_match_predicate() {

    }

    #[derive(Snafu, Debug, Clone)]
    enum TestError {
        #[snafu(display("Test database error:  {}", message))]
        General { message: String },
    }

    struct TestHostPool {
        remote: Arc<TestRemoteServer>
    }

    impl TestHostPool {
        fn new() -> TestHostPool {
            TestHostPool{
                remote: Arc::new(TestRemoteServer{})
            }
        }
    }

    #[async_trait]
    impl HostPool for TestHostPool {
        type Error = TestError;
        type RemoteServer = TestRemoteServer;

        async fn host(&self, id: &str) -> Result<Arc<TestRemoteServer>, Self::Error> {
            Ok(self.remote.clone())
        }
    }

    struct TestRemoteServer {}
    #[async_trait]
    impl RemoteServer for TestRemoteServer {
        type Error = TestError;

        async fn write(&self, db: &str, lines: &[ParsedLine<'_>]) -> Result<(), Self::Error> {
            Ok(())
        }
    }
}