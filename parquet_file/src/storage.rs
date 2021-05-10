/// This module responsible to write given data to specify object store and
/// read them back
use crate::utils::read_data_from_parquet_data;
use arrow::{datatypes::{Schema as ArrowSchema, SchemaRef}, error::{ArrowError, Result as ArrowResult}, record_batch::RecordBatch};
use datafusion::{error::DataFusionError, physical_plan::{
    common::SizedRecordBatchStream, parquet::RowGroupPredicateBuilder, RecordBatchStream,
    SendableRecordBatchStream,
}};
use internal_types::{schema::Schema, selection::Selection};
use object_store::{
    path::{ObjectStorePath, Path},
    ObjectStore, ObjectStoreApi, ObjectStoreIntegration,
};
use parquet::{
    self,
    arrow::{
        arrow_reader::ParquetFileArrowReader, parquet_to_arrow_schema, ArrowReader, ArrowWriter,
    },
    file::{
        metadata::ParquetMetaData, reader::FileReader, serialized_reader::{SerializedFileReader, SliceableCursor},
        writer::TryClone,
    },
};
use query::predicate::Predicate;

use bytes::Bytes;
use data_types::server_id::ServerId;
use futures::{Stream, StreamExt, TryStreamExt};
use parking_lot::Mutex;
use snafu::{OptionExt, ResultExt, Snafu};
use std::{convert::TryInto, fs::File, io::{Cursor, Seek, SeekFrom, Write}, sync::Arc, task::{Context, Poll}};
use tokio::{
    sync::mpsc::{channel, Receiver, Sender},
    task,
};
use tokio_stream::wrappers::ReceiverStream;

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("Error opening Parquet Writer: {}", source))]
    OpeningParquetWriter {
        source: parquet::errors::ParquetError,
    },

    #[snafu(display("Error reading stream while creating snapshot: {}", source))]
    ReadingStream { source: arrow::error::ArrowError },

    #[snafu(display("Error writing Parquet to memory: {}", source))]
    WritingParquetToMemory {
        source: parquet::errors::ParquetError,
    },

    #[snafu(display("Error closing Parquet Writer: {}", source))]
    ClosingParquetWriter {
        source: parquet::errors::ParquetError,
    },

    #[snafu(display("Error writing to object store: {}", source))]
    WritingToObjectStore { source: object_store::Error },

    #[snafu(display("Error converting to vec[u8]: Nothing else should have a reference here"))]
    WritingToMemWriter {},

    #[snafu(display("Non local file not supported"))]
    NonLocalFile {},

    #[snafu(display("Error opening file: {}", source))]
    OpenFile { source: std::io::Error },

    #[snafu(display("Error at serialized file reader: {}", source))]
    SerializedFileReaderError {
        source: parquet::errors::ParquetError,
    },

    #[snafu(display("Error at parquet arrow reader: {}", source))]
    ParquetArrowReaderError {
        source: parquet::errors::ParquetError,
    },

    #[snafu(display("Error reading data from parquet file: {}", source))]
    ReadingFile { source: arrow::error::ArrowError },

    #[snafu(display("Error reading data from object store: {}", source))]
    ReadingObjectStore { source: object_store::Error },

    #[snafu(display("Error sending results: {}", source))]
    SendResult {
        source: datafusion::error::DataFusionError,
    },

    #[snafu(display("Cannot read arrow schema from parquet: {}", source))]
    ArrowFromParquetFailure {
        source: parquet::errors::ParquetError,
    },

    #[snafu(display("Cannot read IOx schema from arrow: {}", source))]
    IoxFromArrowFailure {
        source: internal_types::schema::Error,
    },
}
pub type Result<T, E = Error> = std::result::Result<T, E>;

#[derive(Debug)]
pub struct ParquetStream {
    schema: SchemaRef,
    inner: ReceiverStream<ArrowResult<RecordBatch>>,
}

impl Stream for ParquetStream {
    type Item = ArrowResult<RecordBatch>;

    fn poll_next(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Self::Item>> {
        self.inner.poll_next_unpin(cx)
    }
}

impl RecordBatchStream for ParquetStream {
    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }
}

#[derive(Debug, Clone)]
pub struct Storage {
    object_store: Arc<ObjectStore>,
    server_id: ServerId,
    db_name: String,
}

impl Storage {
    pub fn new(object_store: Arc<ObjectStore>, server_id: ServerId, db_name: String) -> Self {
        Self {
            object_store,
            server_id,
            db_name,
        }
    }

    /// Return full path including filename in the object store to save a chunk
    /// table file
    pub fn location(
        &self,
        partition_key: String,
        chunk_id: u32,
        table_name: String,
    ) -> object_store::path::Path {
        // Full path of the file in object store
        //    <writer id>/<database>/data/<partition key>/<chunk id>/<table
        // name>.parquet

        let mut path = self.object_store.new_path();
        path.push_dir(self.server_id.to_string());
        path.push_dir(self.db_name.clone());
        path.push_dir("data");
        path.push_dir(partition_key);
        path.push_dir(chunk_id.to_string());
        let file_name = format!("{}.parquet", table_name);
        path.set_file_name(file_name);

        path
    }

    /// Write the given stream of data of a specified table of
    // a specified partitioned chunk to a parquet file of this storage
    pub async fn write_to_object_store(
        &self,
        partition_key: String,
        chunk_id: u32,
        table_name: String,
        stream: SendableRecordBatchStream,
    ) -> Result<Path> {
        // Create full path location of this file in object store
        let path = self.location(partition_key, chunk_id, table_name);

        let schema = stream.schema();
        let data = Self::parquet_stream_to_bytes(stream, schema).await?;
        self.to_object_store(data, &path).await?;

        Ok(path.clone())
    }

    /// Convert the given stream of RecordBatches to bytes

    pub async fn parquet_stream_to_bytes(
        mut stream: SendableRecordBatchStream,
        schema: SchemaRef,
    ) -> Result<Vec<u8>> {
        let mem_writer = MemWriter::default();
        {
            let mut writer = ArrowWriter::try_new(mem_writer.clone(), schema, None)
                .context(OpeningParquetWriter)?;
            while let Some(batch) = stream.next().await {
                let batch = batch.context(ReadingStream)?;
                writer.write(&batch).context(WritingParquetToMemory)?;
            }
            writer.close().context(ClosingParquetWriter)?;
        } // drop the reference to the MemWriter that the SerializedFileWriter has

        mem_writer.into_inner().context(WritingToMemWriter)
    }

    /// Put the given vector of bytes to the specified location
    pub async fn to_object_store(
        &self,
        data: Vec<u8>,
        file_name: &object_store::path::Path,
    ) -> Result<()> {
        let len = data.len();
        let data = Bytes::from(data);
        let stream_data = Result::Ok(data);

        self.object_store
            .put(
                &file_name,
                futures::stream::once(async move { stream_data }),
                Some(len),
            )
            .await
            .context(WritingToObjectStore)
    }

    /// Make a datafusion predicate builder for the given predicate and schema
    pub fn predicate_builder(
        predicate: &Predicate,
        schema: ArrowSchema,
    ) -> Option<RowGroupPredicateBuilder> {
        if predicate.exprs.is_empty() {
            None
        } else {
            // Convert to datafusion's predicate
            let predicate = predicate.filter_expr()?;
            Some(RowGroupPredicateBuilder::try_new(&predicate, schema).ok()?)
        }
    }

    /// Return indices of the schema's fields of the selection columns
    pub fn column_indices(selection: Selection<'_>, schema: SchemaRef) -> Vec<usize> {
        let fields = schema.fields().iter();

        match selection {
            Selection::Some(cols) => fields
                .enumerate()
                .filter_map(|(p, x)| {
                    if cols.contains(&x.name().as_str()) {
                        Some(p)
                    } else {
                        None
                    }
                })
                .collect(),
            Selection::All => fields.enumerate().map(|(p, _)| p).collect(),
        }
    }

    pub fn read_filter(
        predicate: &Predicate,
        selection: Selection<'_>,
        schema: SchemaRef,
        path: Path,
        store: Arc<ObjectStore>,
    ) -> Result<SendableRecordBatchStream> {
        // The below code is based on
        // datafusion::physical_plan::parquet::ParquetExec::execute
        // Will be improved as we go

        // let (response_tx, response_rx): (
        //     Sender<ArrowResult<RecordBatch>>,
        //     Receiver<ArrowResult<RecordBatchThis>>,
        // ) = channel(2);

        // Indices of columns in the schema needed to read
        let projection: Vec<usize> = Self::column_indices(selection, Arc::clone(&schema));

        // Filter needed predicates
        let builder_schema = ArrowSchema::new(schema.fields().clone());
        let predicate_builder = Self::predicate_builder(predicate, builder_schema);

        // Size of each batch
        let batch_size = 1024; // Todo: make a constant or policy for this

        // Limit of total rows to read
        let limit: Option<usize> = None; // Todo: this should be a parameter of the function

        // TODO: These commented-out code lines will either be used or deleted when #1082 done
        // TODO: Until this read_filter is an async, we cannot make this multi-threaded yet
        //       because it returns wrong results if other thread rerun before full results are returned
        // task::spawn_blocking(move || {
        //     if let Err(e) = Self::read_file(
        //         path,
        //         Arc::clone(&store),
        //         projection.as_slice(),
        //         predicate_builder.as_ref(),
        //         batch_size,
        //         response_tx,
        //         limit,
        //     ) {
        //         println!("Parquet reader thread terminated due to error: {:?}", e);
        //     }
        // });

        // Ok(Box::pin(ParquetStream {
        //     schema,  // TODO: This is not the right schem. Need to get schema of the batch
        //     inner: ReceiverStream::new(response_rx),
        // }))

        let mut batches: Vec<Arc<RecordBatch>> = vec![];
        if let Err(e) = Self::read_file(
            path,
            Arc::clone(&store),
            projection.as_slice(),
            predicate_builder.as_ref(),
            batch_size,
            &mut batches,
            limit,
        ) {
            return Err(e);
        }

        // Schema of all record batches must be the same, Get the first one
        // to build record batch stream
        let batch_schema = if batches.is_empty() {
            schema
        } else {
            batches[0].schema()
        };

        Ok(Box::pin(SizedRecordBatchStream::new(batch_schema, batches)))
    }

    // TODO notes: implemented this for #1082 but i turns out might not be able to use
    // because needs to finish #1342 before #1082 is fully tested. Thi function will
    // be either used or removed when #1082 is done
    //
    // fn send_result(
    //     response_tx: &Sender<ArrowResult<RecordBatch>>,
    //     result: ArrowResult<RecordBatch>,
    // ) -> Result<()> {
    //     // Note this function is running on its own blocking tokio thread so blocking
    //     // here is ok.
    //     response_tx
    //         .blocking_send(result)
    //         .map_err(|e| DataFusionError::Execution(e.to_string()))
    //         .context(SendResult)?;
    //     Ok(())
    // }

    // TODO: see the notes for send_result above
    // fn read_file(
    //     path: Path,
    //     store: Arc<ObjectStore>,
    //     projection: &[usize],
    //     predicate_builder: Option<&RowGroupPredicateBuilder>,
    //     batch_size: usize,
    //     response_tx: Sender<ArrowResult<RecordBatch>>,
    //     limit: Option<usize>,
    // ) -> Result<()> {

    //     // TODO: support non local file object store. Ticket #1342
    //     let full_path = match (&store.0, path) {
    //         (ObjectStoreIntegration::File(file_root), Path::File(location)) => {
    //             file_root.path(&location)
    //         }
    //         (_, _) => {
    //             panic!("Non local file object store not supported")
    //         }
    //     };
    
    //     println!("Full path filename: {:#?}", full_path);  // TOTO: to be removed after both #1082 and #1342 done

    //     let mut total_rows = 0;

    //     let file = File::open(&full_path).context(OpenFile)?;
    //     let mut file_reader = SerializedFileReader::new(file).context(SerializedFileReaderError)?;

    //     // TODO: remove these line after https://github.com/apache/arrow-rs/issues/252 is done
    //     // Get file level metadata to set it to the record batch's metadata below
    //     let metadata = file_reader.metadata();
    //     let schema = read_schema_from_parquet_metadata(metadata)?;
    //     println!("---------- schema: {:#?}", schema);

    //     if let Some(predicate_builder) = predicate_builder {
    //         let row_group_predicate =
    //             predicate_builder.build_row_group_predicate(file_reader.metadata().row_groups());
    //         file_reader.filter_row_groups(&row_group_predicate); //filter out
    //                                                              // row group based
    //                                                              // on the predicate
    //     }
    //     let mut arrow_reader = ParquetFileArrowReader::new(Arc::new(file_reader));
    //     let mut batch_reader = arrow_reader
    //         .get_record_reader_by_columns(projection.to_owned(), batch_size)
    //         .context(ParquetArrowReaderError)?;
    //     loop {
    //         match batch_reader.next() {
    //             Some(Ok(batch)) => {
    //                 //println!("ParquetExec got new batch from {}", filename);
    //                 total_rows += batch.num_rows();

    //                 // TODO: remove these lines when arow-rs' ticket https://github.com/apache/arrow-rs/issues/252 is done
    //                 // Since arrow's parquet reading does not return the row group level's metadata, the
    //                 // work around here is to get it from the file level which is the same
    //                 let columns = batch.columns().to_vec();
    //                 let fields = batch.schema().fields().clone();
    //                 let arrow_column_schema = ArrowSchema::new_with_metadata(
    //                     fields,
    //                     schema.as_arrow().metadata().clone(),
    //                 );
    //                 let new_batch = RecordBatch::try_new(Arc::new(arrow_column_schema.clone()), columns)
    //                     .context(ReadingFile)?;

    //                 println!("---------------- New batch: {:#?}", new_batch);

            
    //                 Self::send_result(&response_tx, Ok(new_batch))?;
    //                 if limit.map(|l| total_rows >= l).unwrap_or(false) {
    //                     break;
    //                 }
    //             }
    //             None => {
    //                 break;
    //             }
    //             Some(Err(e)) => {
    //                 let err_msg =
    //                     //format!("Error reading batch from {}: {}", filename, e.to_string());
    //                     format!("Error reading batch: {}", e.to_string());
    //                 // send error to operator
    //                 Self::send_result(&response_tx, Err(ArrowError::ParquetError(err_msg)))?;
    //                 // terminate thread with error
    //                 return Err(e).context(ReadingFile);
    //             }
    //         }
    //     }

    //     // finished reading files (dropping response_tx will close
    //     // channel)
    //     Ok(())
    // }

    // Read the given path of the parquet file and return record batches satisfied
    // the given predicate_builder
    fn read_file(
        path: Path,
        store: Arc<ObjectStore>,
        projection: &[usize],
        predicate_builder: Option<&RowGroupPredicateBuilder>,
        batch_size: usize,
        batches: &mut Vec<Arc<RecordBatch>>,
        limit: Option<usize>,
    ) -> Result<()> {

        use tokio::runtime::Builder;
        let tokio_runtime = Builder::new_current_thread().enable_all().build().unwrap();
        let handle = tokio_runtime.handle();


        let parquet_data = handle.block_on(async move {
            Self::load_parquet_data_from_object_store(path, store).await.unwrap()
        });

        let mut total_rows = 0;

        let cursor = SliceableCursor::new(parquet_data.clone());
        let mut reader = SerializedFileReader::new(cursor).unwrap();

        // TODO: remove these line after https://github.com/apache/arrow-rs/issues/252 is done
        // Get file level metadata to set it to the record batch's metadata below
        let metadata = reader.metadata();
        let schema = read_schema_from_parquet_metadata(metadata)?;
        println!("------ Schema: {:#?}", schema);


        let actual_record_batches =
            read_data_from_parquet_data(Arc::clone(&schema.as_arrow()), parquet_data);
        println!("----- Actual record batches: {:#?}", actual_record_batches);

        if let Some(predicate_builder) = predicate_builder {
            let row_group_predicate =
                predicate_builder.build_row_group_predicate(metadata.row_groups());
            reader.filter_row_groups(&row_group_predicate); //filter out
                                                                 // row group based
                                                                 // on the predicate
        }

        let mut arrow_reader = ParquetFileArrowReader::new(Arc::new(reader) as Arc<dyn FileReader>);

        let mut batch_reader = arrow_reader
            .get_record_reader_by_columns(projection.to_owned(), batch_size)
            .context(ParquetArrowReaderError)?;

        loop {
            match batch_reader.next() {
                Some(Ok(batch)) => {
                    total_rows += batch.num_rows();

                    println!("------------- batch: {:#?}", batch);

                    // TODO: remove these lines when arow-rs' ticket https://github.com/apache/arrow-rs/issues/252 is done
                    // Since arrow's parquet reading does not return the row group level's metadata, the
                    // work around here is to get it from the file level which is the same
                    let columns = batch.columns().to_vec();
                    let fields = batch.schema().fields().clone();
                    let arrow_column_schema = ArrowSchema::new_with_metadata(
                        fields,
                        schema.as_arrow().metadata().clone(),
                    );
                    let new_batch = RecordBatch::try_new(Arc::new(arrow_column_schema), columns)
                        .context(ReadingFile)?;

                    println!("------------- new_batch: {:#?}", new_batch);

                    batches.push(Arc::new(new_batch));
                    if limit.map(|l| total_rows >= l).unwrap_or(false) {
                        break;
                    }
                }
                None => {
                    break;
                }
                Some(Err(e)) => {
                    return Err(e).context(ReadingFile);
                }
            }
        }
        println!("------------ total rows: {}", total_rows);

        Ok(())
    }

    pub async fn load_parquet_data_from_object_store(
        path: Path,
        store: Arc<ObjectStore>,
    ) -> Result<Vec<u8>> {
        store
            .get(&path)
            .await
            .context(ReadingObjectStore)?
            .map_ok(|bytes| bytes.to_vec())
            .try_concat()
            .await
            .context(ReadingObjectStore)
    }
}

#[derive(Debug, Default, Clone)]
pub struct MemWriter {
    mem: Arc<Mutex<Cursor<Vec<u8>>>>,
}

impl MemWriter {
    /// Returns the inner buffer as long as there are no other references to the
    /// Arc.
    pub fn into_inner(self) -> Option<Vec<u8>> {
        Arc::try_unwrap(self.mem)
            .ok()
            .map(|mutex| mutex.into_inner().into_inner())
    }
}

impl Write for MemWriter {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        let mut inner = self.mem.lock();
        inner.write(buf)
    }

    fn flush(&mut self) -> std::io::Result<()> {
        let mut inner = self.mem.lock();
        inner.flush()
    }
}

impl Seek for MemWriter {
    fn seek(&mut self, pos: SeekFrom) -> std::io::Result<u64> {
        let mut inner = self.mem.lock();
        inner.seek(pos)
    }
}

impl TryClone for MemWriter {
    fn try_clone(&self) -> std::io::Result<Self> {
        Ok(Self {
            mem: Arc::clone(&self.mem),
        })
    }
}

/// Read IOx schema from parquet metadata.
pub fn read_schema_from_parquet_metadata(parquet_md: &ParquetMetaData) -> Result<Schema> {
    let file_metadata = parquet_md.file_metadata();

    let arrow_schema = parquet_to_arrow_schema(
        file_metadata.schema_descr(),
        file_metadata.key_value_metadata(),
    )
    .context(ArrowFromParquetFailure {})?;

    let arrow_schema_ref = Arc::new(arrow_schema);

    let schema: Schema = arrow_schema_ref
        .try_into()
        .context(IoxFromArrowFailure {})?;
    Ok(schema)
}

#[cfg(test)]
mod tests {}
