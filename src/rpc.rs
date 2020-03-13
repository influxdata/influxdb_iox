use delorean::delorean::Bucket;
use delorean::delorean::{
    delorean_server::Delorean,
    read_response::{
        frame::Data, DataType, FloatPointsFrame, Frame, GroupFrame, IntegerPointsFrame, SeriesFrame,
    },
    storage_server::Storage,
    CapabilitiesResponse, CreateBucketRequest, CreateBucketResponse, DeleteBucketRequest,
    DeleteBucketResponse, GetBucketsResponse, Organization, Predicate, ReadFilterRequest,
    ReadGroupRequest, ReadResponse, ReadSource, StringValuesResponse, TagKeysRequest,
    TagValuesRequest, TimestampRange,
};
use delorean::storage::database::Database;
use delorean::storage::inverted_index::SeriesFilter;
use delorean::storage::SeriesDataType;

use std::cmp::Ordering;
use std::collections::{BTreeMap, BTreeSet};
use std::convert::TryInto;
use std::sync::Arc;

use tokio::sync::mpsc;
use tonic::Status;

use crate::App;

pub struct GrpcServer {
    pub app: Arc<App>,
}

const MEASUREMENT_KEY: &str = "_measurement";
const FIELD_KEY: &str = "_field";

const MEASUREMENT_KEY_BYTES: &[u8] = MEASUREMENT_KEY.as_bytes();
const FIELD_KEY_BYTES: &[u8] = FIELD_KEY.as_bytes();

#[tonic::async_trait]
impl Delorean for GrpcServer {
    async fn create_bucket(
        &self,
        _req: tonic::Request<CreateBucketRequest>,
    ) -> Result<tonic::Response<CreateBucketResponse>, Status> {
        Ok(tonic::Response::new(CreateBucketResponse {}))
    }

    async fn delete_bucket(
        &self,
        _req: tonic::Request<DeleteBucketRequest>,
    ) -> Result<tonic::Response<DeleteBucketResponse>, Status> {
        Ok(tonic::Response::new(DeleteBucketResponse {}))
    }

    async fn get_buckets(
        &self,
        _req: tonic::Request<Organization>,
    ) -> Result<tonic::Response<GetBucketsResponse>, Status> {
        Ok(tonic::Response::new(GetBucketsResponse { buckets: vec![] }))
    }
}

/// This trait implements extraction of information from all storage gRPC requests. The only method
/// required to implement is `read_source_field` because for some requests the field is named
/// `read_source` and for others it is `tags_source`.
trait GrpcInputs {
    fn read_source_field(&self) -> Option<&prost_types::Any>;

    fn read_source_raw(&self) -> Result<&prost_types::Any, Status> {
        Ok(self
            .read_source_field()
            .ok_or_else(|| Status::invalid_argument("missing read_source"))?)
    }

    fn read_source(&self) -> Result<ReadSource, Status> {
        let raw = self.read_source_raw()?;
        let val = &raw.value[..];
        Ok(prost::Message::decode(val).map_err(|_| {
            Status::invalid_argument("value could not be parsed as a ReadSource message")
        })?)
    }

    fn org_id(&self) -> Result<u32, Status> {
        Ok(self
            .read_source()?
            .org_id
            .try_into()
            .map_err(|_| Status::invalid_argument("org_id did not fit in a u32"))?)
    }

    fn bucket(&self, db: &Database) -> Result<Arc<Bucket>, Status> {
        let bucket_id = self
            .read_source()?
            .bucket_id
            .try_into()
            .map_err(|_| Status::invalid_argument("bucket_id did not fit in a u32"))?;

        let maybe_bucket = db
            .get_bucket_by_id(bucket_id)
            .map_err(|_| Status::internal("could not query for bucket"))?;

        Ok(maybe_bucket
            .ok_or_else(|| Status::not_found(&format!("bucket {} not found", bucket_id)))?)
    }
}

impl GrpcInputs for ReadFilterRequest {
    fn read_source_field(&self) -> Option<&prost_types::Any> {
        self.read_source.as_ref()
    }
}

impl GrpcInputs for ReadGroupRequest {
    fn read_source_field(&self) -> Option<&prost_types::Any> {
        self.read_source.as_ref()
    }
}

impl GrpcInputs for TagKeysRequest {
    fn read_source_field(&self) -> Option<&prost_types::Any> {
        self.tags_source.as_ref()
    }
}

impl GrpcInputs for TagValuesRequest {
    fn read_source_field(&self) -> Option<&prost_types::Any> {
        self.tags_source.as_ref()
    }
}

#[tonic::async_trait]
impl Storage for GrpcServer {
    type ReadFilterStream = mpsc::Receiver<Result<ReadResponse, Status>>;

    async fn read_filter(
        &self,
        req: tonic::Request<ReadFilterRequest>,
    ) -> Result<tonic::Response<Self::ReadFilterStream>, Status> {
        let (mut tx, rx) = mpsc::channel(4);

        let read_filter_request = req.into_inner();

        let _org_id = read_filter_request.org_id()?;
        let bucket = read_filter_request.bucket(&self.app.db)?;
        let predicate = read_filter_request.predicate;
        let range = read_filter_request.range;

        let app = Arc::clone(&self.app);

        // TODO: is this blocking because of the blocking calls to the database...?
        tokio::spawn(async move {
            let predicate = predicate.as_ref();
            // TODO: The call to read_series_matching_predicate_and_range takes an optional range,
            // but read_f64_range requires a range-- should this route require a range or use a
            // default or something else?
            let range = range.as_ref().expect("TODO: Must have a range?");

            if let Err(e) = send_series_filters(tx.clone(), app, &bucket, predicate, &range).await {
                tx.send(Err(e)).await.unwrap();
            }
        });

        Ok(tonic::Response::new(rx))
    }

    type ReadGroupStream = mpsc::Receiver<Result<ReadResponse, Status>>;

    async fn read_group(
        &self,
        req: tonic::Request<ReadGroupRequest>,
    ) -> Result<tonic::Response<Self::ReadGroupStream>, Status> {
        let (mut tx, rx) = mpsc::channel(4);

        let read_group_request = req.into_inner();

        let _org_id = read_group_request.org_id()?;
        let bucket = read_group_request.bucket(&self.app.db)?;
        let predicate = read_group_request.predicate;
        let range = read_group_request.range;
        let group_keys = read_group_request.group_keys;
        // TODO: handle Group::None
        let _group = read_group_request.group;
        // TODO: handle aggregate values, especially whether None is the same as
        // Some(AggregateType::None) or not
        let _aggregate = read_group_request.aggregate;

        let app = Arc::clone(&self.app);

        // TODO: is this blocking because of the blocking calls to the database...?
        tokio::spawn(async move {
            let predicate = predicate.as_ref();
            let range = range.as_ref();

            if let Err(e) =
                send_groups(tx.clone(), app, &bucket, predicate, range, group_keys).await
            {
                tx.send(Err(e)).await.unwrap();
            }
        });

        Ok(tonic::Response::new(rx))
    }

    type TagKeysStream = mpsc::Receiver<Result<StringValuesResponse, Status>>;

    async fn tag_keys(
        &self,
        req: tonic::Request<TagKeysRequest>,
    ) -> Result<tonic::Response<Self::TagKeysStream>, Status> {
        let (mut tx, rx) = mpsc::channel(4);

        let tag_keys_request = req.get_ref();

        let _org_id = tag_keys_request.org_id()?;
        let bucket = tag_keys_request.bucket(&self.app.db)?;
        let predicate = tag_keys_request.predicate.clone();
        let _range = tag_keys_request.range.as_ref();

        let app = self.app.clone();

        tokio::spawn(async move {
            match app.db.get_tag_keys(&bucket, predicate.as_ref()) {
                Err(_) => tx
                    .send(Err(Status::internal("could not query for tag keys")))
                    .await
                    .unwrap(),
                Ok(tag_keys_iter) => {
                    // TODO: Should these be batched? If so, how?
                    let tag_keys: Vec<_> = tag_keys_iter
                        .map(|s| match s.as_ref() {
                            "_m" => MEASUREMENT_KEY_BYTES.to_vec(),
                            "_f" => FIELD_KEY_BYTES.to_vec(),
                            other => other.as_bytes().to_vec(),
                        })
                        .collect();
                    tx.send(Ok(StringValuesResponse { values: tag_keys }))
                        .await
                        .unwrap();
                }
            }
        });

        Ok(tonic::Response::new(rx))
    }

    type TagValuesStream = mpsc::Receiver<Result<StringValuesResponse, Status>>;

    async fn tag_values(
        &self,
        req: tonic::Request<TagValuesRequest>,
    ) -> Result<tonic::Response<Self::TagValuesStream>, Status> {
        let (mut tx, rx) = mpsc::channel(4);

        let tag_values_request = req.get_ref();

        let _org_id = tag_values_request.org_id()?;
        let bucket = tag_values_request.bucket(&self.app.db)?;
        let predicate = tag_values_request.predicate.clone();
        let _range = tag_values_request.range.as_ref();

        let tag_key = tag_values_request.tag_key.clone();

        let app = self.app.clone();

        tokio::spawn(async move {
            match app.db.get_tag_values(&bucket, &tag_key, predicate.as_ref()) {
                Err(_) => tx
                    .send(Err(Status::internal("could not query for tag values")))
                    .await
                    .unwrap(),
                Ok(tag_values_iter) => {
                    // TODO: Should these be batched? If so, how?
                    let tag_values: Vec<_> = tag_values_iter.map(|s| s.into_bytes()).collect();
                    tx.send(Ok(StringValuesResponse { values: tag_values }))
                        .await
                        .unwrap();
                }
            }
        });

        Ok(tonic::Response::new(rx))
    }

    async fn capabilities(
        &self,
        _: tonic::Request<()>,
    ) -> Result<tonic::Response<CapabilitiesResponse>, Status> {
        Err(Status::unimplemented("Not yet implemented"))
    }
}

async fn send_series_filters(
    mut tx: mpsc::Sender<Result<ReadResponse, Status>>,
    app: Arc<App>,
    bucket: &Bucket,
    predicate: Option<&Predicate>,
    range: &TimestampRange,
) -> Result<(), Status> {
    let filter_iter = app
        .db
        .read_series_matching_predicate_and_range(&bucket, predicate, Some(range))
        .map_err(|e| Status::internal(format!("could not query for filters: {}", e)))?;

    for series_filter in filter_iter {
        let tags = series_filter.tags();
        let data_type = match series_filter.series_type {
            SeriesDataType::F64 => DataType::Float,
            SeriesDataType::I64 => DataType::Integer,
        } as _;
        let series_frame_response_header = Ok(ReadResponse {
            frames: vec![Frame {
                data: Some(Data::Series(SeriesFrame { data_type, tags })),
            }],
        });

        tx.send(series_frame_response_header).await.unwrap();

        let app = Arc::clone(&app);
        if let Err(e) = send_points(tx.clone(), app, bucket, range, series_filter).await {
            tx.send(Err(e)).await.unwrap();
        }
    }

    Ok(())
}

async fn send_points(
    mut tx: mpsc::Sender<Result<ReadResponse, Status>>,
    app: Arc<App>,
    bucket: &Bucket,
    range: &TimestampRange,
    series_filter: SeriesFilter,
) -> Result<(), Status> {
    // TODO: Should this match https://github.com/influxdata/influxdb/blob/d96f3dc5abb6bb187374caa9e7c7a876b4799bd2/storage/reads/response_writer.go#L21 ?
    const BATCH_SIZE: usize = 1;

    match series_filter.series_type {
        SeriesDataType::F64 => {
            let iter = app
                .db
                .read_f64_range(&bucket, &series_filter, &range, BATCH_SIZE)
                .map_err(|e| {
                    Status::internal(format!("could not query for SeriesFilter data: {}", e))
                })?;

            let frames = iter
                .map(|batch| {
                    // TODO: Performance hazard; splitting this vector is non-ideal
                    let (timestamps, values) = batch.into_iter().map(|p| (p.time, p.value)).unzip();
                    Frame {
                        data: Some(Data::FloatPoints(FloatPointsFrame { timestamps, values })),
                    }
                })
                .collect();
            let data_frame_response = Ok(ReadResponse { frames });

            tx.send(data_frame_response).await.unwrap();
        }
        SeriesDataType::I64 => {
            let iter = app
                .db
                .read_i64_range(&bucket, &series_filter, &range, BATCH_SIZE)
                .map_err(|e| {
                    Status::internal(format!("could not query for SeriesFilter data: {}", e))
                })?;

            let frames = iter
                .map(|batch| {
                    // TODO: Performance hazard; splitting this vector is non-ideal
                    let (timestamps, values) = batch.into_iter().map(|p| (p.time, p.value)).unzip();
                    Frame {
                        data: Some(Data::IntegerPoints(IntegerPointsFrame {
                            timestamps,
                            values,
                        })),
                    }
                })
                .collect();
            let data_frame_response = Ok(ReadResponse { frames });

            tx.send(data_frame_response).await.unwrap();
        }
    }

    Ok(())
}

async fn send_groups(
    mut tx: mpsc::Sender<Result<ReadResponse, Status>>,
    app: Arc<App>,
    bucket: &Bucket,
    predicate: Option<&Predicate>,
    range: Option<&TimestampRange>,
    group_keys: Vec<String>,
) -> Result<(), Status> {
    // Query for all the SeriesFilters that should be returned.
    let filter_iter = app
        .db
        .read_series_matching_predicate_and_range(&bucket, predicate, range)
        .map_err(|e| Status::internal(format!("could not query for filters: {}", e)))?;

    // Group the SeriesFilters by the values they have for the group_keys.
    let mut series_filters_by_group = BTreeMap::new();
    for series_filter in filter_iter {
        let partition_key_values = PartitionKeyValues::new(&group_keys, &series_filter);

        let entry = series_filters_by_group
            .entry(partition_key_values)
            .or_insert_with(|| vec![]);
        entry.push(series_filter);
    }

    for (partition_key_values, series_filters) in series_filters_by_group {
        // Unify all the tag keys present in all of the SeriesFilters in this group.
        let tag_keys: BTreeSet<_> = series_filters
            .iter()
            .map(|sf| sf.tag_keys())
            .flatten()
            .collect();

        let group_frame = ReadResponse {
            frames: vec![Frame {
                data: Some(Data::Group(GroupFrame {
                    tag_keys: tag_keys.iter().map(|tk| tk.as_bytes().to_vec()).collect(),
                    partition_key_vals: partition_key_values
                        .values
                        .iter()
                        .map(|tv| {
                            tv.as_ref()
                                .map(|opt| opt.as_bytes().to_vec())
                                .unwrap_or_else(|| vec![])
                        })
                        .collect(),
                })),
            }],
        };

        tx.send(Ok(group_frame)).await.unwrap();

        let range = range.as_ref().expect("TODO: Must have a range?");

        for series in series_filters {
            if let Err(e) =
                send_points(tx.clone(), Arc::clone(&app), bucket, range, series.clone()).await
            {
                tx.send(Err(e)).await.unwrap();
            }
        }
    }

    Ok(())
}

#[derive(PartialEq, Eq, Hash, Debug)]
struct PartitionKeyValues {
    values: Vec<Option<String>>,
}

impl PartitionKeyValues {
    fn new(group_keys: &[String], series_filter: &SeriesFilter) -> Self {
        PartitionKeyValues {
            values: group_keys
                .iter()
                .map(|group_key| series_filter.tag_with_key(group_key).map(String::from))
                .collect(),
        }
    }
}

impl Ord for PartitionKeyValues {
    fn cmp(&self, other: &Self) -> Ordering {
        self.values
            .iter()
            .zip(other.values.iter())
            .fold(Ordering::Equal, |acc, (a, b)| {
                acc.then_with(|| match (a, b) {
                    (Some(a), Some(b)) => a.partial_cmp(b).unwrap(),
                    (Some(_), None) => Ordering::Less,
                    (None, Some(_)) => Ordering::Greater,
                    (None, None) => Ordering::Equal,
                })
            })
    }
}

impl PartialOrd for PartitionKeyValues {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

#[cfg(test)]
mod tests {
    use super::PartitionKeyValues;
    use delorean::storage::inverted_index::SeriesFilter;
    use delorean::storage::SeriesDataType;

    #[test]
    fn partition_key_values_creation() {
        let sf = SeriesFilter {
            id: 1,
            key: "cpu,host=b,region=west\tusage_system".to_string(),
            value_predicate: None,
            series_type: SeriesDataType::I64,
        };

        let group_keys = vec![
            String::from("region"),
            String::from("not_present"),
            String::from("host"),
        ];

        let partition_key_values = PartitionKeyValues::new(&group_keys, &sf);

        assert_eq!(
            partition_key_values.values,
            vec![Some(String::from("west")), None, Some(String::from("b"))]
        );
    }

    impl From<&Vec<Option<&str>>> for PartitionKeyValues {
        fn from(other: &Vec<Option<&str>>) -> Self {
            Self {
                values: other.iter().map(|o| o.map(String::from)).collect(),
            }
        }
    }

    #[test]
    fn partition_key_values_ordering() {
        let mut partitions: Vec<PartitionKeyValues> = vec![
            vec![None, Some("val12")],
            vec![None, None],
            vec![Some("val21"), Some("val11")],
            vec![None, Some("val10")],
            vec![Some("val20"), Some("val10")],
            vec![None, Some("val11")],
            vec![Some("val20"), Some("val11")],
        ]
        .iter()
        .map(Into::into)
        .collect();

        partitions.sort();

        let expected_ordering: Vec<PartitionKeyValues> = vec![
            vec![Some("val20"), Some("val10")],
            vec![Some("val20"), Some("val11")],
            vec![Some("val21"), Some("val11")],
            vec![None, Some("val10")],
            vec![None, Some("val11")],
            vec![None, Some("val12")],
            vec![None, None],
        ]
        .iter()
        .map(Into::into)
        .collect();

        assert_eq!(partitions, expected_ordering);
    }
}
