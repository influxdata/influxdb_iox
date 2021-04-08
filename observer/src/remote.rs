use async_trait::async_trait;
use std::{sync::Arc, time::Instant};

use arrow_deps::{
    arrow::{
        array::{Array, ArrayRef, StringArray},
        datatypes::{Field, Schema},
        record_batch::RecordBatch,
    },
    datafusion::error::Result,
    datafusion::{datasource::MemTable, prelude::ExecutionContext},
};

use crate::context::Context;

#[derive(Debug, Default)]
pub struct RemoteLoad {
    // current list of remote database names
    databases: Vec<String>,

    // currently active database, if any
    current_database: Option<String>,
}

#[async_trait]
impl crate::command::Command for RemoteLoad {
    async fn matches(
        &mut self,
        line: &str,
        context: &mut Context,
    ) -> std::result::Result<bool, String> {
        let commands = line.split(' ').map(|c| c.trim()).collect::<Vec<_>>();
        if commands.is_empty() {
            return Ok(false);
        }

        if !commands[0].eq_ignore_ascii_case("remote") {
            // If we have a currently selected database, route the query here
            if let Some(current_database) = self.current_database.as_ref() {
                println!(
                    "NOTE: Running query on remote database {}",
                    current_database
                );

                let batches = scrape_query(&mut context.flight_client, current_database, line)
                    .await
                    .map_err(|e| format!("Error running remote query: {}", e))?;

                let format = influxdb_iox_client::format::QueryOutputFormat::Pretty;
                let formatted_results = format
                    .format(&batches)
                    .map_err(|e| format!("Error formatting results from remote server: {}", e))?;
                println!("{}", formatted_results);
                Ok(true)
            } else {
                return Ok(false);
            }
        } else if commands.len() == 1 {
            println!(
                r#"Unknown REMOTE command. Perhaps you meant:

REMOTE LOAD: Load databases and system tables from remote server

REMOTE LIST DATABASES: List remote known databases

REMOTE SHOW : show the currently active remote database, if any

REMOTE SET: clear remote database; All subsequent queries are sent to the local aggregated views

REMOTE SET <DATABASE>: Set a remote database; All subsquent queries are sent to that database

"#
            );
            Ok(false)
            // REMOTE LOAD
        } else if commands.len() == 2 && eq(commands[1], "load") {
            self.refresh_database_list(context).await?;
            self.refresh_remote_system_tables(context).await?;
            Ok(true)
            // REMOTE LIST DATABASES
        } else if commands.len() == 3 && eq(commands[1], "list") && eq(commands[2], "databases") {
            if self.databases.is_empty() {
                println!("No databases. Perhaps you need to run REMOTE LOAD?");
            } else {
                println!("{}", self.databases.join("\n"));
            }
            Ok(true)
            // REMOTE SHOW
        } else if commands.len() == 2 && eq(commands[1], "show") {
            match self.current_database.as_ref() {
                Some(current_database) => println!("Routing requests to: {}", current_database),
                None => println!("No remote database set. Use REMOTE SET DATABASE to do so"),
            };
            Ok(true)
            // REMOTE SET
        } else if commands.len() == 2 && eq(commands[1], "set") {
            self.current_database = None;
            println!("Using local aggregated views");
            Ok(true)
            // REMOTE SET <DATABASE>
        } else if commands.len() == 3 && eq(commands[1], "set") {
            let current_database = commands[2].to_string();
            println!("Setting remote database to {}", current_database);
            self.current_database = Some(current_database);
            Ok(true)
        } else {
            Ok(false)
        }
    }
}

fn eq(part: &str, command: &str) -> bool {
    part.eq_ignore_ascii_case(command)
}

impl RemoteLoad {
    // Loads the list of databases
    async fn refresh_database_list(
        &mut self,
        context: &mut Context,
    ) -> std::result::Result<bool, String> {
        self.databases = context
            .management_client
            .list_databases()
            .await
            .map_err(|e| e.to_string())?;
        Ok(true)
    }

    // Loads all system tables
    async fn refresh_remote_system_tables(
        &self,
        context: &mut Context,
    ) -> std::result::Result<bool, String> {
        let start = Instant::now();

        println!(
            "Aggregating system tables from {} databases",
            self.databases.len()
        );

        // The basic idea is to find all databases, and create a synthetic
        // system tables that have the information from all of them

        let tasks = self
            .databases
            .iter()
            .map(|db_name| db_name.to_string())
            .map(|db_name| {
                let connection = context.connection.clone();
                tokio::task::spawn(async move {
                    let mut client = influxdb_iox_client::flight::Client::new(connection);
                    let batches =
                        scrape_query(&mut client, &db_name, "select * from system.chunks")
                            .await
                            .expect("selecting from system.chunks");

                    let chunks_table = RemoteSystemTable::Chunks {
                        db_name: db_name.clone(),
                        batches,
                    };

                    let batches =
                        scrape_query(&mut client, &db_name, "select * from system.columns")
                            .await
                            .expect("selecting from system.columns");

                    let columns_table = RemoteSystemTable::Columns { db_name, batches };

                    let result: Result<Vec<RemoteSystemTable>> =
                        Ok(vec![chunks_table, columns_table]);
                    result
                })
            });

        // now, get the results and combine them
        let mut builder = AggregatedTableBuilder::new();
        for task in tasks {
            match task.await {
                Ok(Ok(tables)) => {
                    for t in tables {
                        builder.append(t);
                    }
                }
                // This is not a fatal error so log it and keep going
                Ok(Err(e)) => {
                    println!("WARNING: Error running query: {}", e);
                }
                // This is not a fatal error so log it and keep going
                Err(e) => {
                    println!("WARNING: Error running task: {}", e);
                }
            }
        }

        println!(
            "Completed loading remote system tables in {:?}",
            Instant::now() - start
        );

        builder.build(&mut context.ctx);

        println!(
            r#"Some interesting queries:

-- Total estimated storage size by database
select database_name, storage, count(*) as num_chunks, sum(estimated_bytes)/1024/1024 as estimated_mb
from chunks
group by database_name, storage
order by estimated_mb desc;

-- Total row count by partition and table
select database_name, partition_key, table_name, max(count) as total_rows
from columns
group by database_name, partition_key, table_name
order by database_name, partition_key, table_name;

-- Total rows by partition
select database_name, partition_key, sum(total_rows) as total_rows
from (
  select database_name, partition_key, table_name, max(count) as total_rows
  from columns
  group by database_name, partition_key, table_name
)
group by database_name, partition_key
order by database_name, partition_key;

"#
        );

        Ok(true)
    }
}

#[derive(Debug)]
/// Contains the results from a system table query for a specific database
enum RemoteSystemTable {
    /// `select * from system.chunks`
    Chunks {
        db_name: String,
        batches: Vec<RecordBatch>,
    },

    /// `select * from system.columns`
    Columns {
        db_name: String,
        batches: Vec<RecordBatch>,
    },
}

#[derive(Debug)]
/// Aggregates several table responses into a unified view
struct AggregatedTableBuilder {
    chunks: VirtualTableBuilder,
    columns: VirtualTableBuilder,
}

impl AggregatedTableBuilder {
    fn new() -> Self {
        Self {
            chunks: VirtualTableBuilder::new("chunks"),
            columns: VirtualTableBuilder::new("columns"),
        }
    }

    /// Appends a table response to the aggregated tables being built
    fn append(&mut self, t: RemoteSystemTable) {
        match t {
            RemoteSystemTable::Chunks { db_name, batches } => {
                let num_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
                println!(
                    "Aggregating results from system.chunks @ {} ({} batches, {} rows)",
                    db_name,
                    batches.len(),
                    num_rows
                );
                self.chunks.append_batches(&db_name, batches);
            }
            RemoteSystemTable::Columns { db_name, batches } => {
                let num_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
                println!(
                    "Aggregating results from system.columns @ {} ({} batches, {} rows)",
                    db_name,
                    batches.len(),
                    num_rows
                );
                self.columns.append_batches(&db_name, batches);
            }
        };
    }
    /// register a table provider  for this sytem table
    fn build(self, ctx: &mut ExecutionContext) {
        let Self { chunks, columns } = self;

        println!("registering system table: chunks");
        chunks.build(ctx);
        println!("registering system table: columns");
        columns.build(ctx);
    }
}

/// Creates a "virtual" version of  `select * from system.chunks`
/// which has a "database_name" column pre-pended
///
/// The resulting schema
#[derive(Debug)]
struct VirtualTableBuilder {
    table_name: String,
    batches: Vec<RecordBatch>,
}

impl VirtualTableBuilder {
    pub fn new(table_name: impl Into<String>) -> Self {
        let table_name = table_name.into();
        Self {
            table_name,
            batches: Vec::new(),
        }
    }

    /// Append batches from `select * from system.chunks` to the
    /// results being created
    fn append_batches(&mut self, db_name: &str, new_batches: Vec<RecordBatch>) {
        self.batches.extend(new_batches.into_iter().map(|batch| {
            use std::iter::once;

            let array =
                StringArray::from_iter_values(std::iter::repeat(db_name).take(batch.num_rows()));
            let data_type = array.data_type().clone();
            let array = Arc::new(array) as ArrayRef;

            let new_columns = once(array)
                .chain(batch.columns().iter().cloned())
                .collect::<Vec<ArrayRef>>();

            let new_fields = once(Field::new("database_name", data_type, false))
                .chain(batch.schema().fields().iter().cloned())
                .collect::<Vec<Field>>();
            let new_schema = Arc::new(Schema::new(new_fields));

            RecordBatch::try_new(new_schema, new_columns).expect("Creating new record batch")
        }))
    }

    /// register a table provider  for this sytem table
    fn build(self, ctx: &mut ExecutionContext) {
        let Self {
            table_name,
            batches,
        } = self;

        let schema = if batches.is_empty() {
            panic!("No batches for ChunksTableBuilder");
        } else {
            batches[0].schema()
        };

        let partitions = batches
            .into_iter()
            .map(|batch| vec![batch])
            .collect::<Vec<_>>();

        let memtable = MemTable::try_new(schema, partitions).expect("creating memtable");

        ctx.register_table(table_name.as_str(), Arc::new(memtable))
            .ok();
    }
}

/// Runs the specified `query` and returns the record batches of the result
async fn scrape_query(
    client: &mut influxdb_iox_client::flight::Client,
    db_name: &str,
    query: &str,
) -> std::result::Result<Vec<RecordBatch>, influxdb_iox_client::flight::Error> {
    let mut query_results = client.perform_query(db_name, query).await?;

    let mut batches = vec![];

    while let Some(data) = query_results.next().await? {
        batches.push(data);
    }

    Ok(batches)
}
