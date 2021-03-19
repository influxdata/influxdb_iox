//! The catalog representation of a Partition

use std::sync::Arc;

#[derive(Debug, Default)]
pub struct Partition {
    /// The partition key
    key: Arc<String>,
}

impl Partition {
    /// Create a new partition catalog object.
    pub(crate) fn new(key: impl Into<String>) -> Self {
        let key = key.into();

        let key = Arc::new(key);
        Self { key }
    }

    pub fn key(&self) -> &Arc<String> {
        &self.key
    }
}
