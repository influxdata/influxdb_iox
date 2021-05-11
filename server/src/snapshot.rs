use data_types::partition_metadata::{PartitionSummary, TableSummary};
use internal_types::selection::Selection;
use object_store::{path::ObjectStorePath, ObjectStore, ObjectStoreApi};
use query::{predicate::EMPTY_PREDICATE, PartitionChunk};

use std::sync::Arc;

use bytes::Bytes;
use observability_deps::tracing::{error, info};
use parking_lot::Mutex;
use snafu::{ResultExt, Snafu};
use tokio::sync::oneshot;
use uuid::Uuid;

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("Partition error creating snapshot: {}", source))]
    PartitionError {
        source: Box<dyn std::error::Error + Send + Sync>,
    },

    #[snafu(display("Table position out of bounds: {}", position))]
    TablePositionOutOfBounds { position: usize },

    #[snafu(display("Error generating json response: {}", source))]
    JsonGenerationError { source: serde_json::Error },

    #[snafu(display("Error opening Parquet Writer: {}", source))]
    ParquetStreamToByte {
        source: parquet_file::storage::Error,
    },

    #[snafu(display("Error writing to object store: {}", source))]
    WritingToObjectStore { source: object_store::Error },

    #[snafu(display("Error reading batches while writing to '{}': {}", file_name, source))]
    ReadingBatches {
        file_name: String,
        source: arrow::error::ArrowError,
    },

    #[snafu(display("Stopped early"))]
    StoppedEarly,
}

pub type Result<T, E = Error> = std::result::Result<T, E>;

/// Code for snapshotting a database chunk to a Parquet
/// file in object storage.
#[derive(Debug)]
pub struct Snapshot<T>
where
    T: Send + Sync + 'static + PartitionChunk,
{
    pub id: Uuid,
    pub partition_summary: PartitionSummary,
    pub metadata_path: object_store::path::Path,
    pub data_path: object_store::path::Path,
    store: Arc<ObjectStore>,
    chunk: Arc<T>,
    status: Mutex<Status>,
}

impl<T> Snapshot<T>
where
    T: Send + Sync + 'static + PartitionChunk,
{
    fn new(
        partition_key: impl Into<String>,
        metadata_path: object_store::path::Path,
        data_path: object_store::path::Path,
        store: Arc<ObjectStore>,
        partition: Arc<T>,
        table: TableSummary,
    ) -> Self {
        let status = Status::new(TableState::NotStarted);

        Self {
            id: Uuid::new_v4(),
            partition_summary: PartitionSummary {
                key: partition_key.into(),
                tables: vec![table],
            },
            metadata_path,
            data_path,
            store,
            chunk: partition,
            status: Mutex::new(status),
        }
    }

    fn mark_table_running(&self) {
        let mut status = self.status.lock();
        if status.table_state == TableState::NotStarted {
            status.table_state = TableState::Running;
        }
    }

    fn mark_table_finished(&self) {
        let mut status = self.status.lock();
        status.table_state = TableState::Finished;
    }

    fn mark_meta_written(&self) {
        let mut status = self.status.lock();
        status.meta_written = true;
    }

    pub fn finished(&self) -> bool {
        let status = self.status.lock();

        matches!(status.table_state, TableState::Finished)
    }

    fn should_stop(&self) -> bool {
        let status = self.status.lock();
        status.stop_on_next_update
    }

    async fn run(&self, notify: Option<oneshot::Sender<()>>) -> Result<()> {
        self.mark_table_running();

        // get all the data in this chunk:
        let table_name = self.partition_summary.tables[0].name.as_ref();

        let stream = self
            .chunk
            .read_filter(table_name, &EMPTY_PREDICATE, Selection::All)
            .map_err(|e| Box::new(e) as _)
            .context(PartitionError)?;

        let schema = stream.schema();

        let mut location = self.data_path.clone();
        let file_name = format!("{}.parquet", table_name);
        location.set_file_name(&file_name);
        let data = parquet_file::storage::Storage::parquet_stream_to_bytes(stream, schema)
            .await
            .context(ParquetStreamToByte)?;
        self.write_to_object_store(data, &location).await?;
        self.mark_table_finished();

        if self.should_stop() {
            return StoppedEarly.fail();
        }

        let mut partition_meta_path = self.metadata_path.clone();
        let key = format!("{}.json", &self.partition_summary.key);
        partition_meta_path.set_file_name(&key);
        let json_data = serde_json::to_vec(&self.partition_summary).context(JsonGenerationError)?;
        let data = Bytes::from(json_data);
        let len = data.len();
        let stream_data = std::io::Result::Ok(data);
        self.store
            .put(
                &partition_meta_path,
                futures::stream::once(async move { stream_data }),
                Some(len),
            )
            .await
            .context(WritingToObjectStore)?;

        self.mark_meta_written();

        if let Some(notify) = notify {
            if let Err(e) = notify.send(()) {
                error!("error sending notify: {:?}", e);
            }
        }

        Ok(())
    }
    async fn write_to_object_store(
        &self,
        data: Vec<u8>,
        file_name: &object_store::path::Path,
    ) -> Result<()> {
        let len = data.len();
        let data = Bytes::from(data);
        let stream_data = Result::Ok(data);

        self.store
            .put(
                &file_name,
                futures::stream::once(async move { stream_data }),
                Some(len),
            )
            .await
            .context(WritingToObjectStore)
    }

    fn set_error(&self, e: Error) {
        let mut status = self.status.lock();
        status.error = Some(e);
    }
}

#[derive(Debug, PartialEq, Clone)]
pub enum TableState {
    NotStarted,
    Running,
    Finished,
}

#[derive(Debug)]
pub struct Status {
    table_state: TableState,
    meta_written: bool,
    stop_on_next_update: bool,
    error: Option<Error>,
}

impl Status {
    fn new(table_state: TableState) -> Self {
        Self {
            table_state,
            meta_written: false,
            stop_on_next_update: false,
            error: None,
        }
    }
}

pub fn snapshot_chunk<T>(
    metadata_path: object_store::path::Path,
    data_path: object_store::path::Path,
    store: Arc<ObjectStore>,
    partition_key: &str,
    chunk: Arc<T>,
    table_stats: TableSummary,
    notify: Option<oneshot::Sender<()>>,
) -> Result<Arc<Snapshot<T>>>
where
    T: Send + Sync + 'static + PartitionChunk,
{
    let snapshot = Snapshot::new(
        partition_key.to_string(),
        metadata_path,
        data_path,
        store,
        chunk,
        table_stats,
    );
    let snapshot = Arc::new(snapshot);

    let return_snapshot = Arc::clone(&snapshot);

    tokio::spawn(async move {
        info!(
            "starting snapshot of {} to {}",
            &snapshot.partition_summary.key,
            &snapshot.data_path.display()
        );
        if let Err(e) = snapshot.run(notify).await {
            error!("error running snapshot: {:?}", e);
            snapshot.set_error(e);
        }
    });

    Ok(return_snapshot)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{db::test_helpers::write_lp, query_tests::utils::TestDb};
    use futures::TryStreamExt;
    use object_store::memory::InMemory;
    use query::{predicate::Predicate, Database};

    #[tokio::test]
    async fn snapshot() {
        let lp = r#"
cpu,host=A,region=west user=23.2,system=55.1 1
cpu,host=A,region=west user=3.2,system=50.1 10
cpu,host=B,region=east user=10.0,system=74.1 1
        "#;

        let db = TestDb::builder()
            .object_store(Arc::new(ObjectStore::new_in_memory(InMemory::new())))
            .build()
            .db;
        write_lp(&db, &lp);

        let store = Arc::new(ObjectStore::new_in_memory(InMemory::new()));
        let (tx, rx) = tokio::sync::oneshot::channel();
        let mut metadata_path = store.new_path();
        metadata_path.push_dir("meta");

        let mut data_path = store.new_path();
        data_path.push_dir("data");

        let chunk = Arc::clone(&db.chunks(&Predicate::default())[0]);
        let table_summary = db
            .table_summary("1970-01-01T00", "cpu", chunk.id())
            .unwrap();

        let snapshot = snapshot_chunk(
            metadata_path.clone(),
            data_path,
            Arc::clone(&store),
            "testaroo",
            chunk,
            table_summary,
            Some(tx),
        )
        .unwrap();

        rx.await.unwrap();

        let mut location = metadata_path;
        location.set_file_name("testaroo.json");

        let summary = store
            .get(&location)
            .await
            .unwrap()
            .map_ok(|b| bytes::BytesMut::from(&b[..]))
            .try_concat()
            .await
            .unwrap();

        let meta: PartitionSummary = serde_json::from_slice(&*summary).unwrap();
        assert_eq!(meta, snapshot.partition_summary);
        assert!(snapshot.finished());
    }
}
