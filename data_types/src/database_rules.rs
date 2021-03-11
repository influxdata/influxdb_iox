use std::convert::{TryFrom, TryInto};

use chrono::{DateTime, TimeZone, Utc};
use serde::{Deserialize, Serialize};
use snafu::Snafu;

use generated_types::google::protobuf::Empty;
use generated_types::{
    google::{FieldViolation, FieldViolationExt},
    influxdata::iox::management::v1 as management,
};
use influxdb_line_protocol::ParsedLine;

use crate::field_validation::{FromField, FromFieldOpt, FromFieldString, FromFieldVec};
use crate::DatabaseName;

use std::time::Duration;

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("Error in {}: {}", source_module, source))]
    PassThrough {
        source_module: &'static str,
        source: Box<dyn std::error::Error + Send + Sync + 'static>,
    },
}

pub type Result<T, E = Error> = std::result::Result<T, E>;

const DEFAULT_CHUNK_MOVER_CHECK_DURATION: Duration = Duration::from_secs(10);

/// DatabaseRules contains the rules for replicating data, sending data to
/// subscribers, and querying data for a single database.
#[derive(Debug, Serialize, Deserialize, Default, Eq, PartialEq, Clone)]
pub struct DatabaseRules {
    /// The unencoded name of the database. This gets put in by the create
    /// database call, so an empty default is fine.
    #[serde(default)]
    pub name: String, // TODO: Use DatabaseName here
    /// Template that generates a partition key for each row inserted into the
    /// db
    #[serde(default)]
    pub partition_template: PartitionTemplate,
    /// The set of host groups that data should be replicated to. Which host a
    /// write goes to within a host group is determined by consistent hashing of
    /// the partition key. We'd use this to create a host group per
    /// availability zone, so you might have 5 availability zones with 2
    /// hosts in each. Replication will ensure that N of those zones get a
    /// write. For each zone, only a single host needs to get the write.
    /// Replication is for ensuring a write exists across multiple hosts
    /// before returning success. Its purpose is to ensure write durability,
    /// rather than write availability for query (this is covered by
    /// subscriptions).
    #[serde(default)]
    pub replication: Vec<HostGroupId>,
    /// The minimum number of host groups to replicate a write to before success
    /// is returned. This can be overridden on a per request basis.
    /// Replication will continue to write to the other host groups in the
    /// background.
    #[serde(default)]
    pub replication_count: u8,
    /// How long the replication queue can get before either rejecting writes or
    /// dropping missed writes. The queue is kept in memory on a
    /// per-database basis. A queue size of zero means it will only try to
    /// replicate synchronously and drop any failures.
    #[serde(default)]
    pub replication_queue_max_size: usize,
    /// `subscriptions` are used for query servers to get data via either push
    /// or pull as it arrives. They are separate from replication as they
    /// have a different purpose. They're for query servers or other clients
    /// that want to subscribe to some subset of data being written in. This
    /// could either be specific partitions, ranges of partitions, tables, or
    /// rows matching some predicate. This is step #3 from the diagram.
    #[serde(default)]
    pub subscriptions: Vec<Subscription>,

    /// If set to `true`, this server should answer queries from one or more of
    /// of its local write buffer and any read-only partitions that it knows
    /// about. In this case, results will be merged with any others from the
    /// remote goups or read-only partitions.
    #[serde(default)]
    pub query_local: bool,
    /// Set `primary_query_group` to a host group if remote servers should be
    /// issued queries for this database. All hosts in the group should be
    /// queried with this server acting as the coordinator that merges
    /// results together. If a specific host in the group is unavailable,
    /// another host in the same position from a secondary group should be
    /// queried. For example, imagine we've partitioned the data in this DB into
    /// 4 partitions and we are replicating the data across 3 availability
    /// zones. We have 4 hosts in each of those AZs, thus they each have 1
    /// partition. We'd set the primary group to be the 4 hosts in the same
    /// AZ as this one, and the secondary groups as the hosts in the other 2
    /// AZs.
    #[serde(default)]
    pub primary_query_group: Option<HostGroupId>,
    #[serde(default)]
    pub secondary_query_groups: Vec<HostGroupId>,

    /// Use `read_only_partitions` when a server should answer queries for
    /// partitions that come from object storage. This can be used to start
    /// up a new query server to handle queries by pointing it at a
    /// collection of partitions and then telling it to also pull
    /// data from the replication servers (writes that haven't been snapshotted
    /// into a partition).
    #[serde(default)]
    pub read_only_partitions: Vec<PartitionId>,

    /// When set this will buffer WAL writes in memory based on the
    /// configuration.
    #[serde(default)]
    pub wal_buffer_config: Option<WalBufferConfig>,

    /// Unless explicitly disabled by setting this to None (or null in JSON),
    /// writes will go into a queryable in-memory database
    /// called the Mutable Buffer. It is optimized to receive writes so they
    /// can be batched together later to the Read Buffer or to Parquet files
    /// in object storage.
    #[serde(default = "MutableBufferConfig::default_option")]
    pub mutable_buffer_config: Option<MutableBufferConfig>,

    /// Duration for chunk movers to wake up and do move & drop chunks
    #[serde(default = "DatabaseRules::chunk_mover_duration_default")]
    pub chunk_mover_duration: std::time::Duration,
}

impl DatabaseRules {
    pub fn partition_key(
        &self,
        line: &ParsedLine<'_>,
        default_time: &DateTime<Utc>,
    ) -> Result<String> {
        self.partition_template.partition_key(line, default_time)
    }

    pub fn new() -> Self {
        Self {
            // TODO: Add tests before removing these lines
            //mutable_buffer_config: MutableBufferConfig::default_option(),
            //chunk_mover_duration: DEFAULT_CHUNK_MOVER_CHECK_DURATION,
            ..Default::default()
        }
    }

    pub fn chunk_mover_duration_default() -> Duration {
        DEFAULT_CHUNK_MOVER_CHECK_DURATION
    }
}

/// Generates a partition key based on the line and the default time.
pub trait Partitioner {
    fn partition_key(
        &self,
        _line: &ParsedLine<'_>,
        _default_time: &DateTime<Utc>,
    ) -> Result<String>;
}

impl Partitioner for DatabaseRules {
    fn partition_key(&self, line: &ParsedLine<'_>, default_time: &DateTime<Utc>) -> Result<String> {
        self.partition_key(&line, &default_time)
    }
}

impl From<DatabaseRules> for management::DatabaseRules {
    fn from(rules: DatabaseRules) -> Self {
        let subscriptions: Vec<management::subscription_config::Subscription> =
            rules.subscriptions.into_iter().map(Into::into).collect();

        let replication_config = management::ReplicationConfig {
            replications: rules.replication,
            replication_count: rules.replication_count as _,
            replication_queue_max_size: rules.replication_queue_max_size as _,
        };

        let query_config = management::QueryConfig {
            query_local: rules.query_local,
            primary: rules.primary_query_group.unwrap_or_default(),
            secondaries: rules.secondary_query_groups,
            read_only_partitions: rules.read_only_partitions,
        };

        Self {
            name: rules.name,
            partition_template: Some(rules.partition_template.into()),
            replication_config: Some(replication_config),
            subscription_config: Some(management::SubscriptionConfig { subscriptions }),
            query_config: Some(query_config),
            wal_buffer_config: rules.wal_buffer_config.map(Into::into),
            mutable_buffer_config: rules.mutable_buffer_config.map(Into::into),
        }
    }
}

impl TryFrom<management::DatabaseRules> for DatabaseRules {
    type Error = FieldViolation;

    fn try_from(proto: management::DatabaseRules) -> Result<Self, Self::Error> {
        DatabaseName::new(&proto.name).field("name")?;

        let subscriptions = proto
            .subscription_config
            .map(|s| {
                s.subscriptions
                    .vec_field("subscription_config.subscriptions")
            })
            .transpose()?
            .unwrap_or_default();

        let wal_buffer_config = proto.wal_buffer_config.optional("wal_buffer_config")?;

        let mutable_buffer_config = proto
            .mutable_buffer_config
            .optional("mutable_buffer_config")?;

        let partition_template = proto
            .partition_template
            .optional("partition_template")?
            .unwrap_or_default();

        let query = proto.query_config.unwrap_or_default();
        let replication = proto.replication_config.unwrap_or_default();

        // TODO: need to have chunk_mover_duration in the proto
        let chunk_mover_duration = DEFAULT_CHUNK_MOVER_CHECK_DURATION; //proto.chunk_mover_duration;

        Ok(Self {
            name: proto.name,
            partition_template,
            replication: replication.replications,
            replication_count: replication.replication_count as _,
            replication_queue_max_size: replication.replication_queue_max_size as _,
            subscriptions,
            query_local: query.query_local,
            primary_query_group: query.primary.optional(),
            secondary_query_groups: query.secondaries,
            read_only_partitions: query.read_only_partitions,
            wal_buffer_config,
            mutable_buffer_config,
            chunk_mover_duration,
        })
    }
}

/// MutableBufferConfig defines the configuration for the in-memory database
/// that is hot for writes as they arrive. Operators can define rules for
/// evicting data once the mutable buffer passes a set memory threshold.
#[derive(Debug, Serialize, Deserialize, Eq, PartialEq, Clone)]
pub struct MutableBufferConfig {
    /// The size the mutable buffer should be limited to. Once the buffer gets
    /// to this size it will drop partitions in the given order. If unable
    /// to drop partitions (because of later rules in this config) it will
    /// reject writes until it is able to drop partitions.
    pub buffer_size: usize,
    /// If set, the mutable buffer will not drop partitions that have chunks
    /// that have not yet been persisted. Thus it will reject writes if it
    /// is over size and is unable to drop partitions. The default is to
    /// drop partitions in the sort order, regardless of whether they have
    /// unpersisted chunks or not. The WAL Buffer can be used to ensure
    /// persistence, but this may cause longer recovery times.
    pub reject_if_not_persisted: bool,
    /// Drop partitions to free up space in this order. Can be by the oldest
    /// created at time, the longest since the last write, or the min or max of
    /// some column.
    pub partition_drop_order: PartitionSortRules,
    /// Attempt to persist partitions after they haven't received a write for
    /// this number of seconds. If not set, partitions won't be
    /// automatically persisted.
    pub persist_after_cold_seconds: Option<u32>,
}

const DEFAULT_MUTABLE_BUFFER_SIZE: usize = 2_147_483_648; // 2 GB
const DEFAULT_PERSIST_AFTER_COLD_SECONDS: u32 = 900; // 15 minutes

impl MutableBufferConfig {
    fn default_option() -> Option<Self> {
        Some(Self::default())
    }
}

// TODO: Remove this when deprecating HTTP API - cannot be used in gRPC as no
// explicit NULL support
impl Default for MutableBufferConfig {
    fn default() -> Self {
        Self {
            buffer_size: DEFAULT_MUTABLE_BUFFER_SIZE,
            // keep taking writes and drop partitions on the floor
            reject_if_not_persisted: false,
            partition_drop_order: PartitionSortRules {
                order: Order::Desc,
                sort: PartitionSort::CreatedAtTime,
            },
            // rollover the chunk and persist it after the partition has been cold for
            // 15 minutes
            persist_after_cold_seconds: Some(DEFAULT_PERSIST_AFTER_COLD_SECONDS),
        }
    }
}

impl From<MutableBufferConfig> for management::MutableBufferConfig {
    fn from(config: MutableBufferConfig) -> Self {
        Self {
            buffer_size: config.buffer_size as _,
            reject_if_not_persisted: config.reject_if_not_persisted,
            partition_drop_order: Some(config.partition_drop_order.into()),
            persist_after_cold_seconds: config.persist_after_cold_seconds.unwrap_or_default(),
        }
    }
}

impl TryFrom<management::MutableBufferConfig> for MutableBufferConfig {
    type Error = FieldViolation;

    fn try_from(proto: management::MutableBufferConfig) -> Result<Self, Self::Error> {
        let partition_drop_order = proto
            .partition_drop_order
            .optional("partition_drop_order")?
            .unwrap_or_default();

        let buffer_size = if proto.buffer_size == 0 {
            DEFAULT_MUTABLE_BUFFER_SIZE
        } else {
            proto.buffer_size as usize
        };

        let persist_after_cold_seconds = if proto.persist_after_cold_seconds == 0 {
            None
        } else {
            Some(proto.persist_after_cold_seconds)
        };

        Ok(Self {
            buffer_size,
            reject_if_not_persisted: proto.reject_if_not_persisted,
            partition_drop_order,
            persist_after_cold_seconds,
        })
    }
}

/// This struct specifies the rules for the order to sort partitions
/// from the mutable buffer. This is used to determine which order to drop them
/// in. The last partition in the list will be dropped, until enough space has
/// been freed up to be below the max size.
///
/// For example, to drop the partition that has been open longest:
/// ```
/// use data_types::database_rules::{PartitionSortRules, Order, PartitionSort};
///
/// let rules = PartitionSortRules{
///     order: Order::Desc,
///     sort: PartitionSort::CreatedAtTime,
/// };
/// ```
#[derive(Debug, Default, Serialize, Deserialize, Eq, PartialEq, Clone)]
pub struct PartitionSortRules {
    /// Sort partitions by this order. Last will be dropped.
    pub order: Order,
    /// Sort by either a column value, or when the partition was opened, or when
    /// it last received a write.
    pub sort: PartitionSort,
}

impl From<PartitionSortRules> for management::mutable_buffer_config::PartitionDropOrder {
    fn from(ps: PartitionSortRules) -> Self {
        let order: management::Order = ps.order.into();

        Self {
            order: order as _,
            sort: Some(ps.sort.into()),
        }
    }
}

impl TryFrom<management::mutable_buffer_config::PartitionDropOrder> for PartitionSortRules {
    type Error = FieldViolation;

    fn try_from(
        proto: management::mutable_buffer_config::PartitionDropOrder,
    ) -> Result<Self, Self::Error> {
        Ok(Self {
            order: proto.order().scope("order")?,
            sort: proto.sort.optional("sort")?.unwrap_or_default(),
        })
    }
}

/// What to sort the partition by.
#[derive(Debug, Serialize, Deserialize, Eq, PartialEq, Clone)]
pub enum PartitionSort {
    /// The last time the partition received a write.
    LastWriteTime,
    /// When the partition was opened in the mutable buffer.
    CreatedAtTime,
    /// A column name, its expected type, and whether to use the min or max
    /// value. The ColumnType is necessary because the column can appear in
    /// any number of tables and be of a different type. This specifies that
    /// when sorting partitions, only columns with the given name and type
    /// should be used for the purposes of determining the partition order. If a
    /// partition doesn't have the given column in any way, the partition will
    /// appear at the beginning of the list with a null value where all
    /// partitions having null for that value will then be
    /// sorted by created_at_time desc. So if none of the partitions in the
    /// mutable buffer had this column with this type, then the partition
    /// that was created first would appear last in the list and thus be the
    /// first up to be dropped.
    Column(String, ColumnType, ColumnValue),
}

impl Default for PartitionSort {
    fn default() -> Self {
        Self::CreatedAtTime
    }
}

impl From<PartitionSort> for management::mutable_buffer_config::partition_drop_order::Sort {
    fn from(ps: PartitionSort) -> Self {
        use management::mutable_buffer_config::partition_drop_order::ColumnSort;

        match ps {
            PartitionSort::LastWriteTime => Self::LastWriteTime(Empty {}),
            PartitionSort::CreatedAtTime => Self::CreatedAtTime(Empty {}),
            PartitionSort::Column(column_name, column_type, column_value) => {
                let column_type: management::ColumnType = column_type.into();
                let column_value: management::Aggregate = column_value.into();

                Self::Column(ColumnSort {
                    column_name,
                    column_type: column_type as _,
                    column_value: column_value as _,
                })
            }
        }
    }
}

impl TryFrom<management::mutable_buffer_config::partition_drop_order::Sort> for PartitionSort {
    type Error = FieldViolation;

    fn try_from(
        proto: management::mutable_buffer_config::partition_drop_order::Sort,
    ) -> Result<Self, Self::Error> {
        use management::mutable_buffer_config::partition_drop_order::Sort;

        Ok(match proto {
            Sort::LastWriteTime(_) => Self::LastWriteTime,
            Sort::CreatedAtTime(_) => Self::CreatedAtTime,
            Sort::Column(column_sort) => {
                let column_type = column_sort.column_type().scope("column.column_type")?;
                let column_value = column_sort.column_value().scope("column.column_value")?;
                Self::Column(
                    column_sort.column_name.required("column.column_name")?,
                    column_type,
                    column_value,
                )
            }
        })
    }
}

/// The sort order.
#[derive(Debug, Serialize, Deserialize, Eq, PartialEq, Clone)]
pub enum Order {
    Asc,
    Desc,
}

impl Default for Order {
    fn default() -> Self {
        Self::Desc
    }
}

impl From<Order> for management::Order {
    fn from(o: Order) -> Self {
        match o {
            Order::Asc => Self::Asc,
            Order::Desc => Self::Desc,
        }
    }
}

impl TryFrom<management::Order> for Order {
    type Error = FieldViolation;

    fn try_from(proto: management::Order) -> Result<Self, Self::Error> {
        Ok(match proto {
            management::Order::Unspecified => Self::default(),
            management::Order::Asc => Self::Asc,
            management::Order::Desc => Self::Desc,
        })
    }
}

/// Use columns of this type.
#[derive(Debug, Serialize, Deserialize, Eq, PartialEq, Clone)]
pub enum ColumnType {
    I64,
    U64,
    F64,
    String,
    Bool,
}

impl From<ColumnType> for management::ColumnType {
    fn from(t: ColumnType) -> Self {
        match t {
            ColumnType::I64 => Self::I64,
            ColumnType::U64 => Self::U64,
            ColumnType::F64 => Self::F64,
            ColumnType::String => Self::String,
            ColumnType::Bool => Self::Bool,
        }
    }
}

impl TryFrom<management::ColumnType> for ColumnType {
    type Error = FieldViolation;

    fn try_from(proto: management::ColumnType) -> Result<Self, Self::Error> {
        Ok(match proto {
            management::ColumnType::Unspecified => return Err(FieldViolation::required("")),
            management::ColumnType::I64 => Self::I64,
            management::ColumnType::U64 => Self::U64,
            management::ColumnType::F64 => Self::F64,
            management::ColumnType::String => Self::String,
            management::ColumnType::Bool => Self::Bool,
        })
    }
}

/// Use either the min or max summary statistic.
#[derive(Debug, Serialize, Deserialize, Eq, PartialEq, Clone)]
pub enum ColumnValue {
    Min,
    Max,
}

impl From<ColumnValue> for management::Aggregate {
    fn from(v: ColumnValue) -> Self {
        match v {
            ColumnValue::Min => Self::Min,
            ColumnValue::Max => Self::Max,
        }
    }
}

impl TryFrom<management::Aggregate> for ColumnValue {
    type Error = FieldViolation;

    fn try_from(proto: management::Aggregate) -> Result<Self, Self::Error> {
        use management::Aggregate;

        Ok(match proto {
            Aggregate::Unspecified => return Err(FieldViolation::required("")),
            Aggregate::Min => Self::Min,
            Aggregate::Max => Self::Max,
        })
    }
}

/// WalBufferConfig defines the configuration for buffering data from the WAL in
/// memory. This buffer is used for asynchronous replication and to collect
/// segments before sending them to object storage.
#[derive(Debug, Serialize, Deserialize, Eq, PartialEq, Clone)]
pub struct WalBufferConfig {
    /// The size the WAL buffer should be limited to. Once the buffer gets to
    /// this size it will drop old segments to remain below this size, but
    /// still try to hold as much in memory as possible while remaining
    /// below this threshold
    pub buffer_size: u64,
    /// WAL segments become read-only after crossing over this size. Which means
    /// that segments will always be >= this size. When old segments are
    /// dropped from of memory, at least this much space will be freed from
    /// the buffer.
    pub segment_size: u64,
    /// What should happen if a write comes in that would exceed the WAL buffer
    /// size and the oldest segment that could be dropped hasn't yet been
    /// persisted to object storage. If the oldest segment has been
    /// persisted, then it will be dropped from the buffer so that new writes
    /// can be accepted. This option is only for defining the behavior of what
    /// happens if that segment hasn't been persisted. If set to return an
    /// error, new writes will be rejected until the oldest segment has been
    /// persisted so that it can be cleared from memory. Alternatively, this
    /// can be set so that old segments are dropped even if they haven't been
    /// persisted. This setting is also useful for cases where persistence
    /// isn't being used and this is only for in-memory buffering.
    pub buffer_rollover: WalBufferRollover,
    /// If set to true, buffer segments will be written to object storage.
    pub store_segments: bool,
    /// If set, segments will be rolled over after this period of time even
    /// if they haven't hit the size threshold. This allows them to be written
    /// out to object storage as they must be immutable first.
    pub close_segment_after: Option<std::time::Duration>,
}

impl From<WalBufferConfig> for management::WalBufferConfig {
    fn from(rollover: WalBufferConfig) -> Self {
        let buffer_rollover: management::wal_buffer_config::Rollover =
            rollover.buffer_rollover.into();

        Self {
            buffer_size: rollover.buffer_size,
            segment_size: rollover.segment_size,
            buffer_rollover: buffer_rollover as _,
            persist_segments: rollover.store_segments,
            close_segment_after: rollover.close_segment_after.map(Into::into),
        }
    }
}

impl TryFrom<management::WalBufferConfig> for WalBufferConfig {
    type Error = FieldViolation;

    fn try_from(proto: management::WalBufferConfig) -> Result<Self, Self::Error> {
        let buffer_rollover = proto.buffer_rollover().scope("buffer_rollover")?;
        let close_segment_after = proto
            .close_segment_after
            .map(TryInto::try_into)
            .transpose()
            .map_err(|_| FieldViolation {
                field: "closeSegmentAfter".to_string(),
                description: "Duration must be positive".to_string(),
            })?;

        Ok(Self {
            buffer_size: proto.buffer_size,
            segment_size: proto.segment_size,
            buffer_rollover,
            store_segments: proto.persist_segments,
            close_segment_after,
        })
    }
}

/// WalBufferRollover defines the behavior of what should happen if a write
/// comes in that would cause the buffer to exceed its max size AND the oldest
/// segment can't be dropped because it has not yet been persisted.
#[derive(Debug, Clone, Serialize, Deserialize, Eq, PartialEq, Copy)]
pub enum WalBufferRollover {
    /// Drop the old segment even though it hasn't been persisted. This part of
    /// the WAL will be lost on this server.
    DropOldSegment,
    /// Drop the incoming write and fail silently. This favors making sure that
    /// older WAL data will be backed up.
    DropIncoming,
    /// Reject the incoming write and return an error. The client may retry the
    /// request, which will succeed once the oldest segment has been
    /// persisted to object storage.
    ReturnError,
}

impl From<WalBufferRollover> for management::wal_buffer_config::Rollover {
    fn from(rollover: WalBufferRollover) -> Self {
        match rollover {
            WalBufferRollover::DropOldSegment => Self::DropOldSegment,
            WalBufferRollover::DropIncoming => Self::DropIncoming,
            WalBufferRollover::ReturnError => Self::ReturnError,
        }
    }
}

impl TryFrom<management::wal_buffer_config::Rollover> for WalBufferRollover {
    type Error = FieldViolation;

    fn try_from(proto: management::wal_buffer_config::Rollover) -> Result<Self, Self::Error> {
        use management::wal_buffer_config::Rollover;
        Ok(match proto {
            Rollover::Unspecified => return Err(FieldViolation::required("")),
            Rollover::DropOldSegment => Self::DropOldSegment,
            Rollover::DropIncoming => Self::DropIncoming,
            Rollover::ReturnError => Self::ReturnError,
        })
    }
}

/// `PartitionTemplate` is used to compute the partition key of each row that
/// gets written. It can consist of the table name, a column name and its value,
/// a formatted time, or a string column and regex captures of its value. For
/// columns that do not appear in the input row, a blank value is output.
///
/// The key is constructed in order of the template parts; thus ordering changes
/// what partition key is generated.
#[derive(Debug, Serialize, Deserialize, Default, Eq, PartialEq, Clone)]
pub struct PartitionTemplate {
    pub parts: Vec<TemplatePart>,
}

impl PartitionTemplate {
    pub fn partition_key(
        &self,
        line: &ParsedLine<'_>,
        default_time: &DateTime<Utc>,
    ) -> Result<String> {
        let parts: Vec<_> = self
            .parts
            .iter()
            .map(|p| match p {
                TemplatePart::Table => line.series.measurement.to_string(),
                TemplatePart::Column(column) => match line.tag_value(&column) {
                    Some(v) => format!("{}_{}", column, v),
                    None => match line.field_value(&column) {
                        Some(v) => format!("{}_{}", column, v),
                        None => "".to_string(),
                    },
                },
                TemplatePart::TimeFormat(format) => match line.timestamp {
                    Some(t) => Utc.timestamp_nanos(t).format(&format).to_string(),
                    None => default_time.format(&format).to_string(),
                },
                _ => unimplemented!(),
            })
            .collect();

        Ok(parts.join("-"))
    }
}

impl From<PartitionTemplate> for management::PartitionTemplate {
    fn from(pt: PartitionTemplate) -> Self {
        Self {
            parts: pt.parts.into_iter().map(Into::into).collect(),
        }
    }
}

impl TryFrom<management::PartitionTemplate> for PartitionTemplate {
    type Error = FieldViolation;

    fn try_from(proto: management::PartitionTemplate) -> Result<Self, Self::Error> {
        let parts = proto.parts.vec_field("parts")?;
        Ok(Self { parts })
    }
}

/// `TemplatePart` specifies what part of a row should be used to compute this
/// part of a partition key.
#[derive(Debug, Serialize, Deserialize, Eq, PartialEq, Clone)]
pub enum TemplatePart {
    Table,
    Column(String),
    TimeFormat(String),
    RegexCapture(RegexCapture),
    StrftimeColumn(StrftimeColumn),
}

/// `RegexCapture` is for pulling parts of a string column into the partition
/// key.
#[derive(Debug, Serialize, Deserialize, Eq, PartialEq, Clone)]
pub struct RegexCapture {
    column: String,
    regex: String,
}

/// `StrftimeColumn` can be used to create a time based partition key off some
/// column other than the builtin `time` column.
#[derive(Debug, Serialize, Deserialize, Eq, PartialEq, Clone)]
pub struct StrftimeColumn {
    column: String,
    format: String,
}

impl From<TemplatePart> for management::partition_template::part::Part {
    fn from(part: TemplatePart) -> Self {
        use management::partition_template::part::ColumnFormat;

        match part {
            TemplatePart::Table => Self::Table(Empty {}),
            TemplatePart::Column(column) => Self::Column(column),
            TemplatePart::RegexCapture(RegexCapture { column, regex }) => {
                Self::Regex(ColumnFormat {
                    column,
                    format: regex,
                })
            }
            TemplatePart::StrftimeColumn(StrftimeColumn { column, format }) => {
                Self::StrfTime(ColumnFormat { column, format })
            }
            TemplatePart::TimeFormat(format) => Self::Time(format),
        }
    }
}

impl TryFrom<management::partition_template::part::Part> for TemplatePart {
    type Error = FieldViolation;

    fn try_from(proto: management::partition_template::part::Part) -> Result<Self, Self::Error> {
        use management::partition_template::part::{ColumnFormat, Part};

        Ok(match proto {
            Part::Table(_) => Self::Table,
            Part::Column(column) => Self::Column(column.required("column")?),
            Part::Regex(ColumnFormat { column, format }) => Self::RegexCapture(RegexCapture {
                column: column.required("regex.column")?,
                regex: format.required("regex.format")?,
            }),
            Part::StrfTime(ColumnFormat { column, format }) => {
                Self::StrftimeColumn(StrftimeColumn {
                    column: column.required("strf_time.column")?,
                    format: format.required("strf_time.format")?,
                })
            }
            Part::Time(format) => Self::TimeFormat(format.required("time")?),
        })
    }
}

impl From<TemplatePart> for management::partition_template::Part {
    fn from(part: TemplatePart) -> Self {
        Self {
            part: Some(part.into()),
        }
    }
}

impl TryFrom<management::partition_template::Part> for TemplatePart {
    type Error = FieldViolation;

    fn try_from(proto: management::partition_template::Part) -> Result<Self, Self::Error> {
        proto.part.required("part")
    }
}

/// `PartitionId` is the object storage identifier for a specific partition. It
/// should be a path that can be used against an object store to locate all the
/// files and subdirectories for a partition. It takes the form of `/<writer
/// ID>/<database>/<partition key>/`.
pub type PartitionId = String;
pub type WriterId = u32;

/// `Subscription` represents a group of hosts that want to receive data as it
/// arrives. The subscription has a matcher that is used to determine what data
/// will match it, and an optional queue for storing matched writes. Subscribers
/// that recieve some subeset of an individual replicated write will get a new
/// replicated write, but with the same originating writer ID and sequence
/// number for the consuming subscriber's tracking purposes.
///
/// For pull based subscriptions, the requester will send a matcher, which the
/// receiver will execute against its in-memory WAL.
#[derive(Debug, Serialize, Deserialize, Eq, PartialEq, Clone)]
pub struct Subscription {
    pub name: String,
    pub host_group_id: HostGroupId,
    pub matcher: Matcher,
}

impl From<Subscription> for management::subscription_config::Subscription {
    fn from(s: Subscription) -> Self {
        Self {
            name: s.name,
            host_group_id: s.host_group_id,
            matcher: Some(s.matcher.into()),
        }
    }
}

impl TryFrom<management::subscription_config::Subscription> for Subscription {
    type Error = FieldViolation;

    fn try_from(proto: management::subscription_config::Subscription) -> Result<Self, Self::Error> {
        Ok(Self {
            name: proto.name.required("name")?,
            host_group_id: proto.host_group_id.required("host_group_id")?,
            matcher: proto.matcher.optional("matcher")?.unwrap_or_default(),
        })
    }
}

/// `Matcher` specifies the rule against the table name and/or a predicate
/// against the row to determine if it matches the write rule.
#[derive(Debug, Default, Serialize, Deserialize, Eq, PartialEq, Clone)]
pub struct Matcher {
    pub tables: MatchTables,
    // TODO: make this work with query::Predicate
    #[serde(skip_serializing_if = "Option::is_none")]
    pub predicate: Option<String>,
}

impl From<Matcher> for management::Matcher {
    fn from(m: Matcher) -> Self {
        Self {
            predicate: m.predicate.unwrap_or_default(),
            table_matcher: Some(m.tables.into()),
        }
    }
}

impl TryFrom<management::Matcher> for Matcher {
    type Error = FieldViolation;

    fn try_from(proto: management::Matcher) -> Result<Self, Self::Error> {
        Ok(Self {
            tables: proto.table_matcher.required("table_matcher")?,
            predicate: proto.predicate.optional(),
        })
    }
}

/// `MatchTables` looks at the table name of a row to determine if it should
/// match the rule.
#[derive(Debug, Serialize, Deserialize, Eq, PartialEq, Clone)]
#[serde(rename_all = "camelCase")]
pub enum MatchTables {
    #[serde(rename = "*")]
    All,
    Table(String),
    Regex(String),
}

impl Default for MatchTables {
    fn default() -> Self {
        Self::All
    }
}

impl From<MatchTables> for management::matcher::TableMatcher {
    fn from(m: MatchTables) -> Self {
        match m {
            MatchTables::All => Self::All(Empty {}),
            MatchTables::Table(table) => Self::Table(table),
            MatchTables::Regex(regex) => Self::Regex(regex),
        }
    }
}

impl TryFrom<management::matcher::TableMatcher> for MatchTables {
    type Error = FieldViolation;

    fn try_from(proto: management::matcher::TableMatcher) -> Result<Self, Self::Error> {
        use management::matcher::TableMatcher;
        Ok(match proto {
            TableMatcher::All(_) => Self::All,
            TableMatcher::Table(table) => Self::Table(table.required("table_matcher.table")?),
            TableMatcher::Regex(regex) => Self::Regex(regex.required("table_matcher.regex")?),
        })
    }
}

pub type HostGroupId = String;

#[derive(Debug, Serialize, Deserialize, Eq, PartialEq, Clone)]
pub struct HostGroup {
    pub id: HostGroupId,
    /// `hosts` is a vector of connection strings for remote hosts.
    pub hosts: Vec<String>,
}

#[cfg(test)]
mod tests {
    use influxdb_line_protocol::parse_lines;

    use super::*;

    type TestError = Box<dyn std::error::Error + Send + Sync + 'static>;
    type Result<T = (), E = TestError> = std::result::Result<T, E>;

    #[test]
    fn partition_key_with_table() -> Result {
        let template = PartitionTemplate {
            parts: vec![TemplatePart::Table],
        };

        let line = parse_line("cpu foo=1 10");
        assert_eq!("cpu", template.partition_key(&line, &Utc::now()).unwrap());

        Ok(())
    }

    #[test]
    fn partition_key_with_int_field() -> Result {
        let template = PartitionTemplate {
            parts: vec![TemplatePart::Column("foo".to_string())],
        };

        let line = parse_line("cpu foo=1 10");
        assert_eq!("foo_1", template.partition_key(&line, &Utc::now()).unwrap());

        Ok(())
    }

    #[test]
    fn partition_key_with_float_field() -> Result {
        let template = PartitionTemplate {
            parts: vec![TemplatePart::Column("foo".to_string())],
        };

        let line = parse_line("cpu foo=1.1 10");
        assert_eq!(
            "foo_1.1",
            template.partition_key(&line, &Utc::now()).unwrap()
        );

        Ok(())
    }

    #[test]
    fn partition_key_with_string_field() -> Result {
        let template = PartitionTemplate {
            parts: vec![TemplatePart::Column("foo".to_string())],
        };

        let line = parse_line("cpu foo=\"asdf\" 10");
        assert_eq!(
            "foo_asdf",
            template.partition_key(&line, &Utc::now()).unwrap()
        );

        Ok(())
    }

    #[test]
    fn partition_key_with_bool_field() -> Result {
        let template = PartitionTemplate {
            parts: vec![TemplatePart::Column("bar".to_string())],
        };

        let line = parse_line("cpu bar=true 10");
        assert_eq!(
            "bar_true",
            template.partition_key(&line, &Utc::now()).unwrap()
        );

        Ok(())
    }

    #[test]
    fn partition_key_with_tag_column() -> Result {
        let template = PartitionTemplate {
            parts: vec![TemplatePart::Column("region".to_string())],
        };

        let line = parse_line("cpu,region=west usage_user=23.2 10");
        assert_eq!(
            "region_west",
            template.partition_key(&line, &Utc::now()).unwrap()
        );

        Ok(())
    }

    #[test]
    fn partition_key_with_missing_column() -> Result {
        let template = PartitionTemplate {
            parts: vec![TemplatePart::Column("not_here".to_string())],
        };

        let line = parse_line("cpu,foo=asdf bar=true 10");
        assert_eq!("", template.partition_key(&line, &Utc::now()).unwrap());

        Ok(())
    }

    #[test]
    fn partition_key_with_time() -> Result {
        let template = PartitionTemplate {
            parts: vec![TemplatePart::TimeFormat("%Y-%m-%d %H:%M:%S".to_string())],
        };

        let line = parse_line("cpu,foo=asdf bar=true 1602338097000000000");
        assert_eq!(
            "2020-10-10 13:54:57",
            template.partition_key(&line, &Utc::now()).unwrap()
        );

        Ok(())
    }

    #[test]
    fn partition_key_with_default_time() -> Result {
        let format_string = "%Y-%m-%d %H:%M:%S";
        let template = PartitionTemplate {
            parts: vec![TemplatePart::TimeFormat(format_string.to_string())],
        };

        let default_time = Utc::now();
        let line = parse_line("cpu,foo=asdf bar=true");
        assert_eq!(
            default_time.format(format_string).to_string(),
            template.partition_key(&line, &default_time).unwrap()
        );

        Ok(())
    }

    #[test]
    fn partition_key_with_many_parts() -> Result {
        let template = PartitionTemplate {
            parts: vec![
                TemplatePart::Table,
                TemplatePart::Column("region".to_string()),
                TemplatePart::Column("usage_system".to_string()),
                TemplatePart::TimeFormat("%Y-%m-%d %H:%M:%S".to_string()),
            ],
        };

        let line = parse_line(
            "cpu,host=a,region=west usage_user=22.1,usage_system=53.1 1602338097000000000",
        );
        assert_eq!(
            "cpu-region_west-usage_system_53.1-2020-10-10 13:54:57",
            template.partition_key(&line, &Utc::now()).unwrap()
        );

        Ok(())
    }

    fn parsed_lines(lp: &str) -> Vec<ParsedLine<'_>> {
        parse_lines(lp).map(|l| l.unwrap()).collect()
    }

    fn parse_line(line: &str) -> ParsedLine<'_> {
        parsed_lines(line).pop().unwrap()
    }

    #[test]
    fn test_database_rules_defaults() {
        let protobuf = management::DatabaseRules {
            name: "database".to_string(),
            ..Default::default()
        };

        let rules: DatabaseRules = protobuf.clone().try_into().unwrap();
        let back: management::DatabaseRules = rules.clone().into();

        assert_eq!(rules.name, protobuf.name);
        assert_eq!(protobuf.name, back.name);

        assert_eq!(rules.partition_template.parts.len(), 0);
        assert_eq!(rules.subscriptions.len(), 0);
        assert!(rules.primary_query_group.is_none());
        assert_eq!(rules.read_only_partitions.len(), 0);
        assert_eq!(rules.secondary_query_groups.len(), 0);

        // These will be defaulted as optionality not preserved on non-protobuf
        // DatabaseRules
        assert_eq!(back.replication_config, Some(Default::default()));
        assert_eq!(back.subscription_config, Some(Default::default()));
        assert_eq!(back.query_config, Some(Default::default()));
        assert_eq!(back.partition_template, Some(Default::default()));

        // These should be none as preserved on non-protobuf DatabaseRules
        assert!(back.wal_buffer_config.is_none());
        assert!(back.mutable_buffer_config.is_none());
    }

    #[test]
    fn test_database_rules_query() {
        let readonly = vec!["readonly1".to_string(), "readonly2".to_string()];
        let secondaries = vec!["secondary1".to_string(), "secondary2".to_string()];

        let protobuf = management::DatabaseRules {
            name: "database".to_string(),
            query_config: Some(management::QueryConfig {
                query_local: true,
                primary: "primary".to_string(),
                secondaries: secondaries.clone(),
                read_only_partitions: readonly.clone(),
            }),
            ..Default::default()
        };

        let rules: DatabaseRules = protobuf.clone().try_into().unwrap();
        let back: management::DatabaseRules = rules.clone().into();

        assert_eq!(rules.name, protobuf.name);
        assert_eq!(protobuf.name, back.name);

        assert_eq!(rules.read_only_partitions, readonly);
        assert_eq!(rules.primary_query_group, Some("primary".to_string()));
        assert_eq!(rules.secondary_query_groups, secondaries);
        assert_eq!(rules.subscriptions.len(), 0);
        assert_eq!(rules.partition_template.parts.len(), 0);

        // Should be the same as was specified
        assert_eq!(back.query_config, protobuf.query_config);
        assert!(back.wal_buffer_config.is_none());
        assert!(back.mutable_buffer_config.is_none());

        // These will be defaulted as optionality not preserved on non-protobuf
        // DatabaseRules
        assert_eq!(back.replication_config, Some(Default::default()));
        assert_eq!(back.subscription_config, Some(Default::default()));
        assert_eq!(back.partition_template, Some(Default::default()));
    }

    #[test]
    fn test_query_config_default() {
        let protobuf = management::DatabaseRules {
            name: "database".to_string(),
            query_config: Some(Default::default()),
            ..Default::default()
        };

        let rules: DatabaseRules = protobuf.clone().try_into().unwrap();
        let back: management::DatabaseRules = rules.clone().into();

        assert!(rules.primary_query_group.is_none());
        assert_eq!(rules.secondary_query_groups.len(), 0);
        assert_eq!(rules.read_only_partitions.len(), 0);
        assert_eq!(rules.query_local, false);

        assert_eq!(protobuf.query_config, back.query_config);
    }

    #[test]
    fn test_partition_template_default() {
        let protobuf = management::DatabaseRules {
            name: "database".to_string(),
            partition_template: Some(management::PartitionTemplate { parts: vec![] }),
            ..Default::default()
        };

        let rules: DatabaseRules = protobuf.clone().try_into().unwrap();
        let back: management::DatabaseRules = rules.clone().into();

        assert_eq!(rules.partition_template.parts.len(), 0);
        assert_eq!(protobuf.partition_template, back.partition_template);
    }

    #[test]
    fn test_partition_template_no_part() {
        let protobuf = management::DatabaseRules {
            name: "database".to_string(),
            partition_template: Some(management::PartitionTemplate {
                parts: vec![Default::default()],
            }),
            ..Default::default()
        };

        let res: Result<DatabaseRules, _> = protobuf.try_into();
        let err = res.expect_err("expected failure");

        assert_eq!(&err.field, "partition_template.parts.0.part");
        assert_eq!(&err.description, "Field is required");
    }

    #[test]
    fn test_partition_template() {
        use management::partition_template::part::{ColumnFormat, Part};

        let protobuf = management::PartitionTemplate {
            parts: vec![
                management::partition_template::Part {
                    part: Some(Part::Time("time".to_string())),
                },
                management::partition_template::Part {
                    part: Some(Part::Table(Empty {})),
                },
                management::partition_template::Part {
                    part: Some(Part::Regex(ColumnFormat {
                        column: "column".to_string(),
                        format: "format".to_string(),
                    })),
                },
            ],
        };

        let pt: PartitionTemplate = protobuf.clone().try_into().unwrap();
        let back: management::PartitionTemplate = pt.clone().into();

        assert_eq!(
            pt.parts,
            vec![
                TemplatePart::TimeFormat("time".to_string()),
                TemplatePart::Table,
                TemplatePart::RegexCapture(RegexCapture {
                    column: "column".to_string(),
                    regex: "format".to_string()
                })
            ]
        );
        assert_eq!(protobuf, back);
    }

    #[test]
    fn test_partition_template_empty() {
        use management::partition_template::part::{ColumnFormat, Part};

        let protobuf = management::PartitionTemplate {
            parts: vec![management::partition_template::Part {
                part: Some(Part::Regex(ColumnFormat {
                    ..Default::default()
                })),
            }],
        };

        let res: Result<PartitionTemplate, _> = protobuf.try_into();
        let err = res.expect_err("expected failure");

        assert_eq!(&err.field, "parts.0.part.regex.column");
        assert_eq!(&err.description, "Field is required");
    }

    #[test]
    fn test_wal_buffer_config_default() {
        let protobuf: management::WalBufferConfig = Default::default();

        let res: Result<WalBufferConfig, _> = protobuf.try_into();
        let err = res.expect_err("expected failure");

        assert_eq!(&err.field, "buffer_rollover");
        assert_eq!(&err.description, "Field is required");
    }

    #[test]
    fn test_wal_buffer_config_rollover() {
        let protobuf = management::WalBufferConfig {
            buffer_rollover: management::wal_buffer_config::Rollover::DropIncoming as _,
            ..Default::default()
        };

        let config: WalBufferConfig = protobuf.clone().try_into().unwrap();
        let back: management::WalBufferConfig = config.clone().into();

        assert_eq!(config.buffer_rollover, WalBufferRollover::DropIncoming);
        assert_eq!(protobuf, back);
    }

    #[test]
    fn test_wal_buffer_config_negative_duration() {
        use generated_types::google::protobuf::Duration;

        let protobuf = management::WalBufferConfig {
            buffer_rollover: management::wal_buffer_config::Rollover::DropOldSegment as _,
            close_segment_after: Some(Duration {
                seconds: -1,
                nanos: -40,
            }),
            ..Default::default()
        };

        let res: Result<WalBufferConfig, _> = protobuf.try_into();
        let err = res.expect_err("expected failure");

        assert_eq!(&err.field, "closeSegmentAfter");
        assert_eq!(&err.description, "Duration must be positive");
    }

    #[test]
    fn test_matcher_default() {
        let protobuf: management::Matcher = Default::default();

        let res: Result<Matcher, _> = protobuf.try_into();
        let err = res.expect_err("expected failure");

        assert_eq!(&err.field, "table_matcher");
        assert_eq!(&err.description, "Field is required");
    }

    #[test]
    fn test_matcher() {
        let protobuf = management::Matcher {
            predicate: Default::default(),
            table_matcher: Some(management::matcher::TableMatcher::Regex(
                "regex".to_string(),
            )),
        };
        let matcher: Matcher = protobuf.try_into().unwrap();

        assert_eq!(matcher.tables, MatchTables::Regex("regex".to_string()));
        assert!(matcher.predicate.is_none());
    }

    #[test]
    fn test_subscription_default() {
        let pb_matcher = Some(management::Matcher {
            predicate: "predicate1".to_string(),
            table_matcher: Some(management::matcher::TableMatcher::Table(
                "table".to_string(),
            )),
        });

        let matcher = Matcher {
            tables: MatchTables::Table("table".to_string()),
            predicate: Some("predicate1".to_string()),
        };

        let subscription_config = management::SubscriptionConfig {
            subscriptions: vec![
                management::subscription_config::Subscription {
                    name: "subscription1".to_string(),
                    host_group_id: "host group".to_string(),
                    matcher: pb_matcher.clone(),
                },
                management::subscription_config::Subscription {
                    name: "subscription2".to_string(),
                    host_group_id: "host group".to_string(),
                    matcher: pb_matcher,
                },
            ],
        };

        let protobuf = management::DatabaseRules {
            name: "database".to_string(),
            subscription_config: Some(subscription_config),
            ..Default::default()
        };

        let rules: DatabaseRules = protobuf.clone().try_into().unwrap();
        let back: management::DatabaseRules = rules.clone().into();

        assert_eq!(protobuf.subscription_config, back.subscription_config);
        assert_eq!(
            rules.subscriptions,
            vec![
                Subscription {
                    name: "subscription1".to_string(),
                    host_group_id: "host group".to_string(),
                    matcher: matcher.clone()
                },
                Subscription {
                    name: "subscription2".to_string(),
                    host_group_id: "host group".to_string(),
                    matcher
                }
            ]
        )
    }

    #[test]
    fn mutable_buffer_config_default() {
        let protobuf: management::MutableBufferConfig = Default::default();

        let config: MutableBufferConfig = protobuf.try_into().unwrap();
        let back: management::MutableBufferConfig = config.clone().into();

        assert_eq!(config.buffer_size, DEFAULT_MUTABLE_BUFFER_SIZE);
        assert_eq!(config.persist_after_cold_seconds, None);
        assert_eq!(config.partition_drop_order, PartitionSortRules::default());
        assert!(!config.reject_if_not_persisted);

        assert_eq!(back.reject_if_not_persisted, config.reject_if_not_persisted);
        assert_eq!(back.buffer_size as usize, config.buffer_size);
        assert_eq!(
            back.partition_drop_order,
            Some(PartitionSortRules::default().into())
        );
        assert_eq!(back.persist_after_cold_seconds, 0);
    }

    #[test]
    fn mutable_buffer_config() {
        let protobuf = management::MutableBufferConfig {
            buffer_size: 32,
            reject_if_not_persisted: true,
            partition_drop_order: Some(management::mutable_buffer_config::PartitionDropOrder {
                order: management::Order::Desc as _,
                sort: None,
            }),
            persist_after_cold_seconds: 439,
        };

        let config: MutableBufferConfig = protobuf.clone().try_into().unwrap();
        let back: management::MutableBufferConfig = config.clone().into();

        assert_eq!(config.buffer_size, protobuf.buffer_size as usize);
        assert_eq!(
            config.persist_after_cold_seconds,
            Some(protobuf.persist_after_cold_seconds)
        );
        assert_eq!(config.partition_drop_order.order, Order::Desc);
        assert!(config.reject_if_not_persisted);

        assert_eq!(back.reject_if_not_persisted, config.reject_if_not_persisted);
        assert_eq!(back.buffer_size as usize, config.buffer_size);
        assert_eq!(
            back.persist_after_cold_seconds,
            protobuf.persist_after_cold_seconds
        );
    }

    #[test]
    fn partition_drop_order_default() {
        let protobuf: management::mutable_buffer_config::PartitionDropOrder = Default::default();
        let config: PartitionSortRules = protobuf.try_into().unwrap();

        assert_eq!(config, PartitionSortRules::default());
        assert_eq!(config.order, Order::default());
        assert_eq!(config.sort, PartitionSort::default());
    }

    #[test]
    fn partition_drop_order() {
        use management::mutable_buffer_config::{partition_drop_order::Sort, PartitionDropOrder};
        let protobuf = PartitionDropOrder {
            order: management::Order::Asc as _,
            sort: Some(Sort::CreatedAtTime(Empty {})),
        };
        let config: PartitionSortRules = protobuf.clone().try_into().unwrap();
        let back: PartitionDropOrder = config.clone().into();

        assert_eq!(protobuf, back);
        assert_eq!(config.order, Order::Asc);
        assert_eq!(config.sort, PartitionSort::CreatedAtTime);
    }

    #[test]
    fn partition_sort() {
        use management::mutable_buffer_config::partition_drop_order::{ColumnSort, Sort};

        let created_at: PartitionSort = Sort::CreatedAtTime(Empty {}).try_into().unwrap();
        let last_write: PartitionSort = Sort::LastWriteTime(Empty {}).try_into().unwrap();
        let column: PartitionSort = Sort::Column(ColumnSort {
            column_name: "column".to_string(),
            column_type: management::ColumnType::Bool as _,
            column_value: management::Aggregate::Min as _,
        })
        .try_into()
        .unwrap();

        assert_eq!(created_at, PartitionSort::CreatedAtTime);
        assert_eq!(last_write, PartitionSort::LastWriteTime);
        assert_eq!(
            column,
            PartitionSort::Column("column".to_string(), ColumnType::Bool, ColumnValue::Min)
        );
    }

    #[test]
    fn partition_sort_column_sort() {
        use management::mutable_buffer_config::partition_drop_order::{ColumnSort, Sort};

        let res: Result<PartitionSort, _> = Sort::Column(Default::default()).try_into();
        let err1 = res.expect_err("expected failure");

        let res: Result<PartitionSort, _> = Sort::Column(ColumnSort {
            column_type: management::ColumnType::F64 as _,
            ..Default::default()
        })
        .try_into();
        let err2 = res.expect_err("expected failure");

        let res: Result<PartitionSort, _> = Sort::Column(ColumnSort {
            column_type: management::ColumnType::F64 as _,
            column_value: management::Aggregate::Max as _,
            ..Default::default()
        })
        .try_into();
        let err3 = res.expect_err("expected failure");

        assert_eq!(err1.field, "column.column_type");
        assert_eq!(err1.description, "Field is required");

        assert_eq!(err2.field, "column.column_value");
        assert_eq!(err2.description, "Field is required");

        assert_eq!(err3.field, "column.column_name");
        assert_eq!(err3.description, "Field is required");
    }
}
