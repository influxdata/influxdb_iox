//! `DeloreanWal` manages a Write-Ahead Log for each organization and bucket.

use tokio::sync::RwLock;
use wal_rs::{Config, WAL};

use std::collections::HashMap;
use std::convert::TryInto;
use std::ffi::OsStr;
use std::io::Result;
use std::path::{Path, PathBuf};

use crate::id::Id;

type BucketNameToId = HashMap<String, Id>;
type OrgsToBuckets = HashMap<Id, BucketNameToId>;

pub struct DeloreanWal {
    base_dir: PathBuf,
    config: Config,
    orgs_and_buckets: RwLock<WAL>,
}

impl DeloreanWal {
    /// Create a new `DeloreanWal` that stores all its data in `base_dir`.
    pub fn new<S: AsRef<OsStr> + ?Sized>(base_dir: &S) -> Result<DeloreanWal> {
        let base_dir = Path::new(base_dir).to_path_buf();

        // TODO: Decide on the default configuration and whether it should be:
        // - static
        // - change based on some information
        // - exposed as externally configurable
        let config = Config {
            entry_per_segment: 1000,
            check_crc32: true,
        };

        let orgs_and_buckets = WAL::open(&base_dir.join("orgs_and_buckets"), config)?;

        Ok(DeloreanWal {
            base_dir,
            config,
            orgs_and_buckets: RwLock::new(orgs_and_buckets),
        })
    }

    pub async fn create_bucket(&self, org_id: Id, bucket_name: &str, bucket_id: Id) -> Result<()> {
        let bucket_entry = format!("{},{},{}", org_id, bucket_name, bucket_id);
        self.orgs_and_buckets
            .write()
            .await
            .write(&bucket_entry.as_bytes())
    }

    /// Add the given `data` to the WAL for this organization and bucket.
    /// This method is synchronous to notify the caller whether the data has been successfully
    /// saved in the WAL or whether there was an error.
    pub fn write(&self, org_id: Id, bucket_name: &str, bucket_id: Id, data: &[u8]) -> Result<()> {
        self.wal(org_id, bucket_name, bucket_id)?.write(data)
    }

    /// Read a given number of entries from the WAL for this organization and bucket.
    pub fn read(
        &self,
        org_id: Id,
        bucket_name: &str,
        bucket_id: Id,
        n: usize,
    ) -> Result<Vec<Vec<u8>>> {
        self.wal(org_id, bucket_name, bucket_id)?.read(n)
    }

    pub fn read_all(&self, org_id: Id, bucket_name: &str, bucket_id: Id) -> Result<Vec<Vec<u8>>> {
        let mut wal = self.wal(org_id, bucket_name, bucket_id)?;
        let num_entries = wal.len();
        wal.read(num_entries)
    }

    /// Get the total number of entries in the WAL for this organization and bucket.
    pub fn num_entries(&self, org_id: Id, bucket_name: &str, bucket_id: Id) -> Result<usize> {
        self.wal(org_id, bucket_name, bucket_id)
            .map(|wal| wal.len())
    }

    pub async fn orgs_and_buckets(&self) -> Result<OrgsToBuckets> {
        let mut orgs_and_buckets = self.orgs_and_buckets.write().await;

        let num_entries = orgs_and_buckets.len();
        let org_bucket_list = orgs_and_buckets.read(num_entries)?;

        let mut orgs_to_buckets = HashMap::new();
        for org_bucket_entry in org_bucket_list {
            let org_bucket_entry = std::str::from_utf8(&org_bucket_entry).unwrap();
            let parts: Vec<_> = org_bucket_entry.splitn(3, ',').collect();
            let org_id: Id = parts[0].try_into().unwrap();
            let bucket_name = parts[1].to_string();
            let bucket_id: Id = parts[2].try_into().unwrap();

            let org = orgs_to_buckets.entry(org_id).or_insert_with(HashMap::new);
            org.insert(bucket_name, bucket_id);
        }

        Ok(orgs_to_buckets)
    }

    fn wal(&self, org_id: Id, bucket_name: &str, bucket_id: Id) -> Result<WAL> {
        WAL::open(
            &self
                .base_dir
                .join(org_id.to_string())
                .join(format!("{}-{}", bucket_name, bucket_id)),
            self.config,
        )
    }
}

#[cfg(test)]
mod tests {
    use std::convert::TryInto;
    use std::env;

    use super::*;

    #[tokio::test]
    async fn round_trip() {
        // Data representing line protocol received in /write that contains multiple lines
        // but was sent in one batch.
        // TODO: Decide how to chunk data into WAL entries
        let data = "prometheus,env=toolsus1,hostname=host1,nodename=node1,role=gateway-internal circuitbreaker_redis_consecutive_successes=0 1578431517778522000
prometheus,endpoint=/api/v2/query,env=toolsus1,hostname=host1,nodename=node1,org_id=332e4ccb1c0d7943,role=gateway-internal,status=500 http_query_request_bytes=19098 1578431517778528000";
        let org_id = 5u64.try_into().unwrap();
        let bucket_id: Id = 6u64.try_into().unwrap();
        let bucket_name = bucket_id.to_string();

        let _ = dotenv::dotenv(); // load .env file if present

        let root = env::var_os("TEST_DELOREAN_DB_DIR").unwrap_or_else(|| env::temp_dir().into());

        let dir = tempfile::Builder::new()
            .prefix("delorean")
            .tempdir_in(root)
            .unwrap();

        // Write data to the WAL then drop it
        {
            let dw = DeloreanWal::new(&dir.path()).unwrap();

            // Create a bucket
            dw.create_bucket(org_id, &bucket_name, bucket_id)
                .await
                .unwrap();

            let orgs_and_buckets = dw.orgs_and_buckets().await.unwrap();
            assert_eq!(orgs_and_buckets.len(), 1);
            let buckets = orgs_and_buckets.get(&org_id).unwrap();
            assert_eq!(buckets.len(), 1);
            assert_eq!(*buckets.get(&bucket_name).unwrap(), bucket_id);

            // Write 3 entries to test entry limit
            dw.write(org_id, &bucket_name, bucket_id, data.as_bytes())
                .unwrap();
            dw.write(org_id, &bucket_name, bucket_id, data.as_bytes())
                .unwrap();
            dw.write(org_id, &bucket_name, bucket_id, data.as_bytes())
                .unwrap();

            assert_eq!(dw.num_entries(org_id, &bucket_name, bucket_id).unwrap(), 3);
        }

        // Read data from the WAL files
        {
            let dw = DeloreanWal::new(&dir.path()).unwrap();

            let num_entries = dw.num_entries(org_id, &bucket_name, bucket_id).unwrap();
            assert_eq!(num_entries, 3);

            let all_data = dw
                .read(org_id, &bucket_name, bucket_id, num_entries)
                .unwrap();

            assert_eq!(all_data.len(), 3);
            for entry in all_data {
                assert_eq!(entry, data.as_bytes());
            }
        }
    }
}
