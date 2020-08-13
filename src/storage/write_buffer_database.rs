use crate::storage::partitioned_store::{
    Error as PartitionedStoreError,
    WalDetails,
    start_wal_sync_task
};
use delorean_wal::{Wal, Append, WalBuilder};
use delorean_line_parser::{ParsedLine, FieldValue};
use crate::generated_types::wal as wb;
use wb::{
    WriteBufferBatch,
    WriteBufferEntry,
    PartitionOpen,
    PartitionSnapshotStarted,
    DictionaryAdd,
    SchemaAppend,
    Row,
};

use std::sync::Arc;
use std::collections::HashMap;
use std::sync::atomic::{Ordering, AtomicU32};
use std::path::PathBuf;
use std::io::{Write, ErrorKind};
use std::fmt;
use std::convert::TryFrom;

use tokio::sync::RwLock;
use snafu::{ResultExt, Snafu, OptionExt, ensure};
use arrow::{
    array::{StringBuilder, ArrayRef},
    datatypes::{
        Field as ArrowField,
        Schema as ArrowSchema,
        DataType as ArrowDataType,
    },
    record_batch::RecordBatch,
};
use chrono::{DateTime, TimeZone, NaiveDateTime, Utc};
use string_interner::{backend::StringBackend, StringInterner, DefaultSymbol, DefaultHashBuilder, Symbol};
use futures::{
    channel::mpsc,
    stream::{BoxStream, Stream},
    FutureExt, SinkExt, StreamExt,
};
use flatbuffers::WIPOffset;
use arrow::array::{UInt32Builder, Float64Builder, Int64Builder, BooleanBuilder};

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("Partition error writing to WAL: {}", source))]
    WritingToWal { source: std::io::Error },

    #[snafu(display("Error opening WAL for database {}: {}", database, source))]
    OpeningWal { database: String, source: PartitionedStoreError},

    #[snafu(display("Error opening WAL for database {}: {}", database, source))]
    LoadingWal { database: String, source: delorean_wal::Error},

    #[snafu(display("Error recovering WAL for database {}: {}", database, error))]
    WalRecoverError { database: String, error: String },

    #[snafu(display("Error recovering WAL for partition {}: {}", partition_id, error))]
    WalPartitionError { partition_id: u32, error: String },

    #[snafu(display("Error recovering write from WAL, column id {} not found", column_id))]
    WalColumnError { column_id: u16 },

    #[snafu(display("Error creating db dir for {}: {}", database, err))]
    CreatingWalDir { database: String, err: String},

    #[snafu(display("Schema mismatch: Write with the following errors: {}", error))]
    SchemaMismatch { error: String },

    #[snafu(display("Database {} doesn't exist", database))]
    DatabaseNotFound { database: String },

    #[snafu(display("Partition {} is full", partition_id))]
    PartitionFull { partition_id: String },

    #[snafu(display("Table {} not found", table))]
    TableNotFound { table: String},

    #[snafu(display("Unexpected insert error"))]
    InsertError {},

    #[snafu(display("arrow conversion error"))]
    ArrowError { source: arrow::error::ArrowError },

    #[snafu(display("dictionary lookup error on id {}", id))]
    DictionaryIdLookupError {id: u32},

    #[snafu(display("dictionary lookup error on name {}", name))]
    DictionaryNameLokupError {name: String},

    #[snafu(display("id conversion error"))]
    IdConversionError {source: std::num::TryFromIntError},
}

pub type Result<T, E = Error> = std::result::Result<T, E>;

pub struct WriteBufferDatabases {
    databases: RwLock<HashMap<String, Arc<Db>>>,
    base_dir: PathBuf,
}

impl WriteBufferDatabases {
    pub async fn write_lines(&self, db_name: &str, lines: &[ParsedLine<'_>]) -> Result<()> {
        let db:Arc<Db>;
        {
            let databases = self.databases.read().await;
            db = databases.get(db_name).context(DatabaseNotFound {database: db_name.to_string()})?.clone();
        }

        db.write_lines(lines).await
    }
}

pub struct Db {
    name: String,
    // TODO: partitions need to b wrapped in an Arc if they're going to be used without this lock
    partitions: RwLock<Vec<Partition>>,
    next_partition_id: AtomicU32,
    wal_details: Option<WalDetails>,
    dir: PathBuf,
}

impl Db {
    pub async fn new_with_wal(name: String, wal_dir: &mut PathBuf) -> Result<Self> {
        wal_dir.push(&name);
        if let Err(e) = std::fs::create_dir(wal_dir.clone()) {
            match e.kind() {
                ErrorKind::AlreadyExists => (),
                _ => return CreatingWalDir{database: name, err: e.to_string()}.fail(),
            }
        }
        let dir = wal_dir.clone();
        let wal_builder = WalBuilder::new(wal_dir.clone());
        let wal_details = start_wal_sync_task(wal_builder).await.context(OpeningWal{database: name.clone()})?;
        wal_details.write_metadata().await.context(OpeningWal{database: name.clone()})?;

        Ok(Self {
            name,
            dir,
            partitions: RwLock::new(vec![]),
            next_partition_id: AtomicU32::new(1),
            wal_details: Some(wal_details),
        })
    }

    pub async fn restore_from_wal(name: String, wal_dir: PathBuf) -> Result<Self> {
        let wal_builder = WalBuilder::new(wal_dir.clone());
        let wal_details = start_wal_sync_task(wal_builder.clone()).await.context(OpeningWal{database: name.clone()})?;

        // TODO: check wal metadata format
        let entries = wal_builder.entries().context(LoadingWal {database: name.clone()})?;
        let mut partitions: HashMap<u32, Partition> = HashMap::new();
        let mut next_partition_id = 0;
        for entry in entries {
            let entry = entry.context(LoadingWal{database: name.clone()})?;
            let bytes = entry.as_data();

            let entry = flatbuffers::get_root::<wb::WriteBufferBatch<'_>>(&bytes);

            if let Some(entries) = entry.entries() {
                for entry in entries {
                    if let Some(po) = entry.partition_open() {
                        println!("opened partition {} {}", po.id(), po.name().unwrap());
                        let id = po.id();
                        let p = Partition::new(id, po.name().unwrap().to_string());
                        partitions.insert(id, p);
                    } else if let Some(_ps) = entry.partition_snapshot_started() {
                        // TODO: handle partition snapshot
                    } else if let Some(_pf) = entry.partition_snapshot_finished() {
                        // TODO: handle partition snapshot finished
                    } else if let Some(da) = entry.dictionary_add() {
                        println!("dict add {} - {}:{}", da.partition_id(), da.id(), da.value().unwrap());
                        let p = partitions.get_mut(&da.partition_id()).context(WalRecoverError{database: name.clone(), error: format!("couldn't add dictionary item to partition {} with id {} and value {}", da.partition_id(), da.id(), da.value().unwrap())})?;
                        p.intern_new_dict_entry(da.value().unwrap());
                    } else if let Some(sa) = entry.schema_append() {
                        println!("schema append partition:{} - table:{} - column:{}, type:{:?}", sa.partition_id(), sa.table_id(), sa.column_id(), sa.column_type());
                        let p = partitions.get_mut(&sa.partition_id()).context(WalRecoverError{database: name.clone(), error: format!("couldn't append schema to partition {} in table id {} and column id {}", sa.partition_id(), sa.table_id(), sa.column_id())})?;
                        p.append_schema(sa.table_id(), sa.column_id(), sa.column_type())?;
                    } else if let Some(row) = entry.write() {
                        println!("adding row! {:?}", row);
                        let p = partitions.get_mut(&row.partition_id()).context(WalRecoverError{database: name.clone(), error: format!("couldn't add row because partition {} wasn't found", row.partition_id())})?;
                        p.add_row(row.partition_id(), &row.values().unwrap())?;
                    }
                }
            }
        }

        let partitions = partitions.drain().map(|(_, p)| p).collect();
        Ok(Self{
            name,
            dir: wal_dir,
            partitions: RwLock::new(partitions),
            next_partition_id: AtomicU32::new(next_partition_id + 1),
            wal_details: Some(wal_details),
        })
    }

    pub async fn write_lines(&self, lines: &[ParsedLine<'_>]) -> Result<()> {
        let partition_keys: Vec<_> = lines.into_iter().map(|l| (l, self.partition_key(l))).collect();
        let mut partitions = self.partitions.write().await;

        let mut builder = match &self.wal_details {
            Some(_) => Some((flatbuffers::FlatBufferBuilder::new_with_capacity(1024), vec![])),
            None => None,
        };

        // TODO: rollback writes to partitions on validation failures
        for (line, key) in partition_keys {
            match partitions.iter_mut().find(|p| p.should_write(&key)) {
                Some(p) => p.write_line(line, &mut builder)?,
                None => {
                    let id = self.next_partition_id.fetch_add(1, Ordering::Relaxed);

                    if let Some((fb, entries)) = &mut builder {
                        let partition_name = fb.create_string(&key);
                        let partition_open = wb::PartitionOpen::create(fb, &wb::PartitionOpenArgs{
                            id,
                            name: Some(partition_name),
                        });
                        let entry = wb::WriteBufferEntry::create(fb, &wb::WriteBufferEntryArgs{
                            partition_open: Some(partition_open),
                            ..Default::default()
                        });
                        entries.push(entry);
                    }

                    let mut p = Partition::new(id, key);
                    p.write_line(line, &mut builder)?;
                    partitions.push(p)
                }
            }
        }

        if let Some(WalDetails { wal, .. }) = &self.wal_details {
            let (fb, entries) = &mut builder.unwrap();

            let (ingest_done_tx, mut ingest_done_rx) = mpsc::channel(1);

            let mut w = wal.append();

            let entry_vec = fb.create_vector(&entries);
            let batch = wb::WriteBufferBatch::create(fb, &wb::WriteBufferBatchArgs{
                entries: Some(entry_vec)
            });
            fb.finish(batch, None);

            w.write_all(fb.finished_data())
                .context(WritingToWal)?;
            w.finalize(ingest_done_tx).expect("TODO handle errors");

            ingest_done_rx
                .next()
                .await
                .expect("TODO handle errors")
                .expect("TODO handle errors");
        }

        Ok(())
    }

    pub async fn table_to_arrow(&self, table_name: &str, columns: &[&str]) -> Result<RecordBatch> {
        // TODO: have this work with multiple partitions
        let partitions = self.partitions.read().await;
        let partition = partitions.first().context(TableNotFound {table: table_name.to_string()})?;
        partition.table_to_arrow(table_name)
    }

    // partition_key returns the partition key for the given line. The key will be the prefix of a
    // partition name (multiple partitions can exist for each key). It uses the user defined
    // partitioning rules to construct this key
    fn partition_key(&self, line: &ParsedLine<'_>) -> String {
        // TODO - wire this up to use partitioning rules, for now just partition by day
        let ts = line.timestamp.unwrap();
        let secs = ts / 1_000_000_000;
        let nsecs = (ts % 1_000_000_000) as u32;
        let dt = DateTime::<Utc>::from_utc(NaiveDateTime::from_timestamp(secs, nsecs), Utc);
        dt.format("%Y/%m/%d").to_string()
    }
}

struct Partition {
    name: String,
    id: u32,
    dictionary: StringInterner<DefaultSymbol, StringBackend<DefaultSymbol>, DefaultHashBuilder>,
    // tables is a map of the dictionary ID for the table name to the table
    tables: HashMap<u32, Table>,
    is_open: bool,
}

impl Partition {
    fn new(id: u32, name: String) -> Partition {
        Partition{
            name,
            id,
            dictionary: StringInterner::new(),
            tables: HashMap::new(),
            is_open: true,
        }
    }

    fn intern_new_dict_entry(&mut self, name: &str) {
        self.dictionary.get_or_intern(name);
    }

    fn append_schema(&mut self, table_id: u32, column_id: u32, column_type: wb::ColumnType) -> Result<()> {
        let t = self.tables.get_mut(&table_id).context(WalPartitionError{partition_id: self.id, error: format!("error addding schema to table id {} with column id {}", table_id, column_id)})?;
        t.append_schema(column_id, column_type);
        Ok(())
    }

    fn add_row(&mut self, table_id: u32, values: &flatbuffers::Vector<'_, flatbuffers::ForwardsUOffset<wb::Value<'_>>>) -> Result<()> {
        let t = self.tables.get_mut(&table_id).context(WalPartitionError{partition_id: self.id, error: format!("error: table id {} not found to add row", table_id)})?;
        t.add_wal_row(values)
    }

    fn write_line<'a>(&mut self, line: &ParsedLine<'_>, builder: &mut Option<(flatbuffers::FlatBufferBuilder<'a>, Vec<flatbuffers::WIPOffset<wb::WriteBufferEntry<'a>>>)>) -> Result<()> {
        let measurement = line.series.measurement.as_str();
        let table_id = self.get_or_insert_dict(measurement, builder);
        let partition_id = self.id;

        let column_count = 1 + line.field_set.len() + line.series.tag_set.as_ref().map(|t| t.len()).unwrap_or(0);
        let mut values: Vec<ColumnValue<'_>> = Vec::with_capacity(column_count);

        // Make sure the time, tag and field names exist in the dictionary
        if let Some(tags) = &line.series.tag_set {
            for (k, v) in tags {
                let tag_column_id = self.get_or_insert_dict(k.as_str(), builder);
                let tag_value_id = self.get_or_insert_dict(v.as_str(), builder);
                values.push(ColumnValue{id: tag_column_id, value: Value::TagValueId(tag_value_id)});
            }
        }
        for (field_name, value) in &line.field_set {
            let field_id = self.get_or_insert_dict(field_name.as_str(), builder);
            values.push(ColumnValue{id: field_id, value: Value::FieldValue(value)});
        }
        let time_id = self.get_or_insert_dict("time", builder);
        let time = line.timestamp.unwrap_or(0);
        let time_value = FieldValue::I64(time);
        values.push(ColumnValue{id: time_id, value: Value::FieldValue(&time_value)});

        let mut table = self.tables.entry(table_id).or_insert_with(|| Table::new(table_id, partition_id));
        table.add_row(&values, builder)?;

        Ok(())
    }

    fn get_or_insert_dict<'a>(&mut self, value: &str, builder: &mut Option<(flatbuffers::FlatBufferBuilder<'a>, Vec<flatbuffers::WIPOffset<wb::WriteBufferEntry<'a>>>)>) -> u32 {
        match self.dictionary.get(value) {
            Some(id) => symbol_to_u32(id),
            None => {
                let id = symbol_to_u32(self.dictionary.get_or_intern(value));

                if let Some((fbb, entries)) = builder {
                    add_dictionary_entry(self.id, value, id, fbb, entries);
                }

                id
            }
        }
    }

    fn should_write(&self, key: &str) -> bool {
        self.name.starts_with(key) && self.is_open
    }

    fn table_to_arrow(&self, table_name: &str) -> Result<RecordBatch> {
        let table_id = self.dictionary.get(table_name).context( TableNotFound{table: table_name.to_string()} )?;
        let table_id = u32::try_from(table_id.to_usize()).context(IdConversionError {})?;

        let table = self.tables.get(&table_id).context(TableNotFound {table: format!("id: {}", table_id)})?;
        table.to_arrow(&self.dictionary)
    }
}

// ColumnValue is a temporary holder of the column ID (name to dict mapping) and its value
#[derive(Debug)]
struct ColumnValue<'a> {
    id: u32,
    value: Value<'a>,
}

#[derive(Debug)]
enum Value<'a> {
    TagValueId(u32),
    FieldValue(&'a FieldValue<'a>)
}


fn add_dictionary_entry<'a>(partition_id: u32, value: &str, id: u32, fbb: &mut flatbuffers::FlatBufferBuilder<'a>, entries: &mut Vec<flatbuffers::WIPOffset<wb::WriteBufferEntry<'a>>>) {
    let value_offset = fbb.create_string(value);
    let dictionary_add = wb::DictionaryAdd::create(fbb, &wb::DictionaryAddArgs {
        id,
        partition_id,
        value: Some(value_offset),
    });
    let entry = wb::WriteBufferEntry::create(fbb, &wb::WriteBufferEntryArgs {
        dictionary_add: Some(dictionary_add),
        ..Default::default()
    });
    entries.push(entry);
}

fn symbol_to_u32(sym: DefaultSymbol) -> u32 {
    sym.to_usize() as u32
}

struct Table {
    id: u32,
    partition_id: u32,
    column_id_to_index: HashMap<u32, usize>,
    columns: Vec<Column>
}

impl Table {
    fn new(id: u32, partition_id: u32) -> Table {
        Self{
            id,
            partition_id,
            column_id_to_index: HashMap::new(),
            columns: Vec::new(),
        }
    }

    fn append_schema(&mut self, column_id: u32, column_type: wb::ColumnType) {
        let column_index = self.columns.len();
        self.column_id_to_index.insert(column_id, column_index);
        let row_count = self.row_count();

        match column_type {
            wb::ColumnType::Tag => {
                let mut v= Vec::with_capacity(row_count + 1);
                v.resize_with(row_count, || None);
                self.columns.push(Column::Tag(v));
            },
            wb::ColumnType::I64 => {
                let mut v= Vec::with_capacity(row_count + 1);
                v.resize_with(row_count, || None);
                self.columns.push(Column::I64(v));
            },
            wb::ColumnType::F64 => {
                let mut v= Vec::with_capacity(row_count + 1);
                v.resize_with(row_count, || None);
                self.columns.push(Column::F64(v));
            },
            wb::ColumnType::U64 => {
                // TODO: handle this (write it out, in mem, and recover)
            },
            wb::ColumnType::String => {
                let mut v= Vec::with_capacity(row_count + 1);
                v.resize_with(row_count, || None);
                self.columns.push(Column::String(v));
            },
            wb::ColumnType::Bool => {
                let mut v= Vec::with_capacity(row_count + 1);
                v.resize_with(row_count, || None);
                self.columns.push(Column::Bool(v));
            },
        }
    }

    fn add_wal_row(&mut self, values: &flatbuffers::Vector<'_, flatbuffers::ForwardsUOffset<wb::Value<'_>>>) -> Result<()> {
        for value in values {
            let col = self.columns.get_mut(value.column_index() as usize).context(WalColumnError{column_id: value.column_index()})?;
            match (col, value.value_type()) {
                (Column::Tag(vals), wb::ColumnValue::TagValue) => {
                    let v = value.value_as_tag_value().context(WalColumnError {column_id: value.column_index()})?;
                    vals.push(Some(v.value()));
                },
                (Column::Bool(vals), wb::ColumnValue::BoolValue) => {
                    let v = value.value_as_bool_value().context(WalColumnError {column_id: value.column_index()})?;
                    vals.push(Some(v.value()));
                },
                (Column::String(vals), wb::ColumnValue::StringValue) => {
                    let v = value.value_as_string_value().context(WalColumnError {column_id: value.column_index()})?;
                    vals.push(Some(v.value().unwrap().to_string()));
                },
                (Column::I64(vals), wb::ColumnValue::I64Value) => {
                    let v = value.value_as_i64value().context(WalColumnError {column_id: value.column_index()})?;
                    vals.push(Some(v.value()));
                },
                (Column::F64(vals), wb::ColumnValue::F64Value) => {
                    let v = value.value_as_f64value().context(WalColumnError {column_id: value.column_index()})?;
                    vals.push(Some(v.value()));
                },
                _ => return SchemaMismatch {error: "column type mismatch recovering from WAL"}.fail(),
            }
        }
        Ok(())
    }

    fn row_count(&self) -> usize {
        match self.columns.first() {
            Some(v) => v.len(),
            None => 0,
        }
    }

    fn add_row<'a>(&mut self, values: &[ColumnValue<'_>], builder: &mut Option<(flatbuffers::FlatBufferBuilder<'a>, Vec<flatbuffers::WIPOffset<wb::WriteBufferEntry<'a>>>)>) -> Result<()> {
        let row_count = self.row_count();

        // insert new columns and validate existing ones
        for val in values {
            let mut column = match self.column_id_to_index.get(&val.id) {
                Some(idx) => &mut self.columns[*idx],
                None => {
                    // Add the column and make all values for existing rows None
                    let index = self.columns.len();
                    self.column_id_to_index.insert(val.id, index);

                    let (col, wal_type) = match val.value {
                        Value::TagValueId(_) => {
                            let mut v = Vec::with_capacity(row_count + 1);
                            v.resize_with(row_count, || None);
                            (Column::Tag(v), wb::ColumnType::Tag)
                        },
                        Value::FieldValue(FieldValue::I64(_)) => {
                            let mut v= Vec::with_capacity(row_count + 1);
                            v.resize_with(row_count, || None);
                            (Column::I64(v), wb::ColumnType::I64)
                        },
                        Value::FieldValue(FieldValue::F64(_)) => {
                            let mut v= Vec::with_capacity(row_count + 1);
                            v.resize_with(row_count, || None);
                            (Column::F64(v), wb::ColumnType::F64)
                        },
                        Value::FieldValue(FieldValue::Boolean(_)) => {
                            let mut v= Vec::with_capacity(row_count + 1);
                            v.resize_with(row_count, || None);
                            (Column::Bool(v), wb::ColumnType::Bool)
                        },
                        Value::FieldValue(FieldValue::String(_)) => {
                            let mut v= Vec::with_capacity(row_count + 1);
                            v.resize_with(row_count, || None);
                            (Column::String(v), wb::ColumnType::String)
                        },
                    };
                    self.columns.push(col);

                    if let Some((fbb, entries)) = builder {
                        let schema_append = wb::SchemaAppend::create(fbb, &wb::SchemaAppendArgs{
                            partition_id: self.partition_id,
                            table_id: self.id,
                            column_id: val.id,
                            column_type: wal_type,
                        });

                        let entry = wb::WriteBufferEntry::create(fbb, &wb::WriteBufferEntryArgs {
                            schema_append: Some(schema_append),
                            ..Default::default()
                        });
                        entries.push(entry);
                    }

                    &mut self.columns[index]
                }
            };

            ensure!(column.matches_type(&val), SchemaMismatch { error: format!("new column type {:?} doesn't match existing type", val)});
        }

        // insert the actual values
        for val in values {
            let idx = self.column_id_to_index.get(&val.id).context(InsertError {})?;
            let column = self.columns.get_mut(*idx).context(InsertError {})?;
            column.push_value(val)?;
        }

        // make sure all columns are of the same length
        for col in &mut self.columns {
            col.push_none_if_len_equal(row_count);
        }

        Ok(())
    }

    fn to_arrow(&self, dictionary: &StringInterner<DefaultSymbol, StringBackend<DefaultSymbol>, DefaultHashBuilder>) -> Result<RecordBatch> {
        let mut index: Vec<_> = self.column_id_to_index.iter().collect();
        index.sort_by(|a, b| a.1.cmp(b.1));
        let ids: Vec<_> = index.iter().map(|(a, b)| **a).collect();

        let mut fields = Vec::with_capacity(self.columns.len());
        let mut columns: Vec<ArrayRef> = Vec::with_capacity(self.columns.len());

        for (col, id) in self.columns.iter().zip(ids) {
            let symbol = Symbol::try_from_usize(id as usize).unwrap();
            let column_name = dictionary.resolve(symbol).context(DictionaryIdLookupError {id})?;

            let arrow_col: ArrayRef = match col {
                Column::String(vals) => {
                    fields.push(ArrowField::new(column_name, ArrowDataType::Utf8, true));
                    let mut builder = StringBuilder::with_capacity(vals.len(), vals.len() * 10);

                    for v in vals {
                        match v {
                            None =>  builder.append_null(),
                            Some(s) => builder.append_value(s),
                        }.context(ArrowError {})?;
                    }

                    Arc::new(builder.finish())
                },
                Column::Tag(vals) => {
                    fields.push(ArrowField::new(column_name, ArrowDataType::Utf8, true));
                    let mut builder = StringBuilder::with_capacity(vals.len(), vals.len() * 10);

                    for v in vals {
                        match v {
                            None => builder.append_null(),
                            Some(id) => {
                                let symbol = Symbol::try_from_usize(*id as usize).unwrap();
                                let tag_value = dictionary.resolve(symbol).context(DictionaryIdLookupError {id: *id})?;
                                builder.append_value(tag_value)
                            },
                        }.context(ArrowError {})?;
                    }

                    Arc::new(builder.finish())
                },
                Column::F64(vals) => {
                    fields.push(ArrowField::new(column_name, ArrowDataType::Float64, true));
                    let mut builder = Float64Builder::new(vals.len());

                    for v in vals {
                        builder.append_option(*v).context(ArrowError {})?;
                    }

                    Arc::new(builder.finish())
                },
                Column::I64(vals) => {
                    fields.push(ArrowField::new(column_name, ArrowDataType::Int64, true));
                    let mut builder = Int64Builder::new(vals.len());

                    for v in vals {
                        builder.append_option(*v).context(ArrowError {})?;
                    }

                    Arc::new(builder.finish())
                },
                Column::Bool(vals) => {
                    fields.push(ArrowField::new(column_name, ArrowDataType::Boolean, true));
                    let mut builder = BooleanBuilder::new(vals.len());

                    for v in vals {
                        builder.append_option(*v).context(ArrowError {})?;
                    }

                    Arc::new(builder.finish())
                },
            };

            columns.push(arrow_col);
        }

        let schema = ArrowSchema::new(fields);

        RecordBatch::try_new(Arc::new(schema), columns).context(ArrowError {})
    }
}

enum Column {
    F64(Vec<Option<f64>>),
    I64(Vec<Option<i64>>),
    String(Vec<Option<String>>),
    Bool(Vec<Option<bool>>),
    Tag(Vec<Option<u32>>),
}

impl Column {
    fn len(&self) -> usize {
        match self {
            Self::F64(v) => v.len(),
            Self::I64(v) => v.len(),
            Self::String(v) => v.len(),
            Self::Bool(v) => v.len(),
            Self::Tag(v) => v.len(),
        }
    }

    // TODO: have type mismatches return helpful error
    fn matches_type(&self, val: &ColumnValue<'_>) -> bool {
        match (self, &val.value) {
            (Column::Tag(_), Value::TagValueId(_)) => true,
            (col, Value::FieldValue(field)) => {
                match (col, field) {
                    (Column::F64(_), FieldValue::F64(_)) => true,
                    (Column::I64(_), FieldValue::I64(_)) => true,
                    (Column::Bool(_), FieldValue::Boolean(_)) => true,
                    (Column::String(_), FieldValue::String(_)) => true,
                    _ => false,
                }
            },
            _ => false,
        }
    }

    fn push_value(&mut self, val: &ColumnValue<'_>) -> Result<()> {
        match &val.value {
            Value::TagValueId(val) => {
                match self {
                    Column::Tag(vals) => vals.push(Some(*val)),
                    _ => return SchemaMismatch { error: "passed value is a tag and existing column is not".to_string()}.fail(),
                }
            },
            Value::FieldValue(field) => {
                match (self, field) {
                    (Column::Tag(_), _ ) => return SchemaMismatch { error: "existing column is a tag and passed value is not".to_string()}.fail(),
                    (Column::String(vals), FieldValue::String(val)) => vals.push(Some(val.as_str().to_string())),
                    (Column::Bool(vals), FieldValue::Boolean(val)) => vals.push(Some(*val)),
                    (Column::I64(vals), FieldValue::I64(val)) => vals.push(Some(*val)),
                    (Column::F64(vals), FieldValue::F64(val)) => vals.push(Some(*val)),
                    _ => panic!("yarrr the field didn't match"),
                }
            },
        }

        Ok(())
    }

    // push_none_if_len_equal will add a None value to the end of the Vec of values if the
    // length is equal to the passed in value.
    fn push_none_if_len_equal(&mut self, len: usize) {
        match self {
            Self::F64(v) => if v.len() == len {
                v.push(None);
            },
            Self::I64(v) => if v.len() == len {
                v.push(None);
            },
            Self::String(v) => if v.len() == len {
                v.push(None);
            },
            Self::Bool(v) => if v.len() == len {
                v.push(None);
            },
            Self::Tag(v) => if v.len() == len {
                v.push(None);
            },
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use delorean_line_parser::parse_lines;

    type TestError = Box<dyn std::error::Error + Send + Sync + 'static>;
    type Result<T = (), E = TestError> = std::result::Result<T, E>;

    #[tokio::test(threaded_scheduler)]
    async fn write_data_and_recover() -> Result {
        let mut dir = delorean_test_helpers::tmp_dir()?.into_path();

        {
            let mut db = Db::new_with_wal("mydb".to_string(), &mut dir.clone()).await?;
            let lines: Vec<_> = parse_lines("cpu,region=west,host=A user=23.2,other=1i,str=\"some string\",b=true 10").map(|l| l.unwrap()).collect();
            db.write_lines(&lines).await?;
            let lines: Vec<_> = parse_lines("cpu,region=west,host=B user=23.1 15").map(|l| l.unwrap()).collect();
            db.write_lines(&lines).await?;
            let lines: Vec<_> = parse_lines("cpu,host=A,new_tag=foo new_field=15.1 20").map(|l| l.unwrap()).collect();
            db.write_lines(&lines).await?;
            let lines: Vec<_> = parse_lines("mem,region=east,host=C val=23432 10").map(|l| l.unwrap()).collect();
            db.write_lines(&lines).await?;

            let table = db.table_to_arrow("mem", &vec!["region","host"]).await?;
            std::thread::sleep(std::time::Duration::from_secs(1));
            for f in std::fs::read_dir(db.dir).unwrap() {
                let f = f.unwrap();
                println!("wal path: {:?}", f.path());
                if f.path().is_dir() {
                    println!("dir inside: {:?}", std::fs::read_dir(f.path()).unwrap());
                }
            }
//            panic!(format!("OMG teh table!\n{:?}", table));
        }

        // check that it recovers from the wal
        {
            let db = Db::restore_from_wal("mydb".to_string(), &mut dir).await?;
            let table = db.table_to_arrow("cpu", &vec![]).await?;
            panic!(format!("table restored! {:?}", table));
        }


        Ok(())
    }
}