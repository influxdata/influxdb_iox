use crate::delorean::Bucket;
use crate::id::Id;
use crate::storage::StorageError;
use std::sync::Arc;

pub trait ConfigStore: Sync + Send {
    fn create_bucket_if_not_exists(&self, org_id: Id, bucket: &Bucket) -> Result<Id, StorageError>;

    fn get_bucket_by_name(
        &self,
        org_id: Id,
        bucket_name: &str,
    ) -> Result<Option<Arc<Bucket>>, StorageError>;

    fn get_bucket_by_id(&self, bucket_id: Id) -> Result<Option<Arc<Bucket>>, StorageError>;
}
