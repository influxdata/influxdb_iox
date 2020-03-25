//! partitioned_store is a trait and set of helper functions and structs to define Partitions
//! that store data. The helper funcs and structs merge results from multiple partitions together.
use crate::delorean::{Predicate, TimestampRange};
use crate::line_parser::PointType;
use crate::storage::series_store::ReadPoint;
use crate::storage::StorageError;

use futures::stream::{BoxStream, Stream};
use std::cmp::Ordering;
use std::mem;
use std::pin::Pin;
use std::task::{Context, Poll};

/// A Partition is a block of data. It has methods for reading the metadata like which measurements,
/// tags, tag values, and fields exist. Along with the raw time series data. It is designed to work
/// as a stream so that it can be used in safely an asynchronous context. A partition is the
/// lowest level organization scheme. Above it you will have a database which keeps track of
/// what organizations and buckets exist. A bucket will have 1-many partitions and a partition
/// will only ever contain data for a single bucket.
pub trait Partition {
    fn id(&self) -> String;

    fn size(&self) -> u64;

    fn write(&self, points: &[PointType]) -> Result<(), StorageError>;

    //    fn measurements(
    //        &self,
    //    ) -> Result<Box<dyn Iterator<Item = Vec<String>>>, StorageError>;
    //
    //    fn tag_keys(
    //        &self,
    //        measurement: &str,
    //    ) -> Result<Box<dyn Iterator<Item = Vec<String>>>, StorageError>;
    //
    //    fn fields(
    //        &self,
    //        measurement: &str,
    //    ) -> Result<Box<dyn Iterator<Item = Vec<&Field>>>, StorageError>;
    //
    //    fn tag_values(
    //        &self,
    //        measurement: &str,
    //        key: &str,
    //    ) -> Result<Box<dyn Iterator<Item = Vec<String>>>, StorageError>;

    fn get_tag_keys(
        &self,
        range: &TimestampRange,
        predicate: &Predicate,
    ) -> Result<BoxStream<'_, String>, StorageError>;

    fn get_tag_values(
        &self,
        tag_key: &str,
        range: &TimestampRange,
        predicate: &Predicate,
    ) -> Result<BoxStream<'_, String>, StorageError>;

    fn read(
        &self,
        batch_size: usize,
        predicate: &Predicate,
        range: &TimestampRange,
    ) -> Result<BoxStream<'_, ReadBatch>, StorageError>;
}

/// StringMergeStream will do a merge sort with deduplication of multiple streams of Strings. This
/// is used for combining results from multiple partitions for calls to get measurements, tag keys,
/// tag values, or field keys. It assumes the incoming streams are in sorted order.
pub struct StringMergeStream<'a> {
    streams: Vec<BoxStream<'a, String>>,
    next_vals: Vec<Poll<Option<String>>>,
    drained: bool,
}

impl StringMergeStream<'_> {
    fn new(streams: Vec<BoxStream<'_, String>>) -> StringMergeStream<'_> {
        let len = streams.len();

        StringMergeStream {
            streams,
            next_vals: vec![Poll::Pending; len],
            drained: false,
        }
    }
}

impl Stream for StringMergeStream<'_> {
    type Item = String;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        if self.drained {
            return Poll::Ready(None);
        }

        let mut one_pending = false;

        for pos in 0..self.next_vals.len() {
            match self.next_vals[pos] {
                Poll::Pending => {
                    let v = self.streams[pos].as_mut().poll_next(cx);
                    if v.is_pending() {
                        one_pending = true;
                    }

                    self.next_vals[pos] = v;
                }
                Poll::Ready(_) => (),
            }
        }

        if one_pending {
            return Poll::Pending;
        }

        let mut next_pos = 0;

        for pos in 1..self.next_vals.len() {
            if let Poll::Ready(Some(s)) = &self.next_vals[pos] {
                match &self.next_vals[next_pos] {
                    Poll::Ready(None) => next_pos = pos,
                    Poll::Ready(Some(next)) => match next.cmp(s) {
                        Ordering::Greater => next_pos = pos,
                        Ordering::Equal => {
                            self.next_vals[pos] = self.streams[pos].as_mut().poll_next(cx)
                        }
                        Ordering::Less => (),
                    },
                    Poll::Pending => return Poll::Pending,
                }
            }
        }

        let val = mem::replace(&mut self.next_vals[next_pos], Poll::Pending);

        if let Poll::Ready(None) = val {
            self.drained = true;
        }

        val
    }
}

/// ReadMergeStream will do a merge sort of the ReadBatches from multiple partitions. When merging
/// it will ensure that batches are sent through in lexographical order by key. In situations
/// where multiple partitions have batches with the same key, they are merged together in time
/// ascending order. For any given key, multiple read batches can come through.
pub struct ReadMergeStream<'a> {
    streams: Vec<BoxStream<'a, ReadBatch>>,
    next_vals: Vec<Poll<Option<ReadBatch>>>,
    drained: bool,
}

impl ReadMergeStream<'_> {
    fn new(streams: Vec<BoxStream<'_, ReadBatch>>) -> ReadMergeStream<'_> {
        let len = streams.len();

        ReadMergeStream {
            streams,
            next_vals: vec![Poll::Pending; len],
            drained: false,
        }
    }
}

impl Stream for ReadMergeStream<'_> {
    type Item = ReadBatch;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        if self.drained {
            return Poll::Ready(None);
        }

        let mut min_key: Option<String> = None;
        let mut min_time = std::i64::MAX;
        let mut min_pos = 0;

        let mut one_pending = false;

        // find the key that we should send next and make sure that things are populated
        for pos in 0..self.next_vals.len() {
            match &self.next_vals[pos] {
                Poll::Pending => match self.streams[pos].as_mut().poll_next(cx) {
                    Poll::Pending => one_pending = true,
                    Poll::Ready(Some(batch)) => {
                        match &min_key {
                            Some(k) => match batch.key.cmp(k) {
                                Ordering::Less => {
                                    min_key = Some(batch.key.clone());
                                    let (_, min) = batch.start_stop_times().unwrap();
                                    min_pos = pos;
                                    min_time = min;
                                }
                                Ordering::Equal => {
                                    let (_, min) = batch.start_stop_times().unwrap();
                                    if min < min_time {
                                        min_pos = pos;
                                        min_time = min;
                                    }
                                }
                                Ordering::Greater => (),
                            },
                            None => {
                                min_key = Some(batch.key.clone());
                                let (_, min) = batch.start_stop_times().unwrap();
                                min_pos = pos;
                                min_time = min;
                            }
                        }
                        self.next_vals[pos] = Poll::Ready(Some(batch));
                    }
                    Poll::Ready(None) => self.next_vals[pos] = Poll::Ready(None),
                },
                Poll::Ready(None) => (),
                Poll::Ready(Some(batch)) => match &min_key {
                    Some(k) => match batch.key.cmp(k) {
                        Ordering::Less => {
                            min_key = Some(batch.key.clone());
                            let (_, min) = batch.start_stop_times().unwrap();
                            min_pos = pos;
                            min_time = min;
                        }
                        Ordering::Equal => {
                            let (_, min) = batch.start_stop_times().unwrap();
                            if min < min_time {
                                min_pos = pos;
                                min_time = min;
                            }
                        }
                        Ordering::Greater => (),
                    },
                    None => {
                        min_key = Some(batch.key.clone());
                        let (_, min) = batch.start_stop_times().unwrap();
                        min_pos = pos;
                        min_time = min;
                    }
                },
            }
        }

        if one_pending {
            return Poll::Pending;
        }

        if min_key.is_none() {
            self.drained = true;
            return Poll::Ready(None);
        }
        let min_key = min_key.unwrap();

        let batch = mem::replace(&mut self.next_vals[min_pos], Poll::Pending);

        let mut batch = match batch {
            Poll::Ready(Some(b)) => b,
            _ => unreachable!(),
        };

        let mut sort_needed = false;

        for pos in 0..self.next_vals.len() {
            if let Poll::Ready(Some(b)) = &mut self.next_vals[pos] {
                if b.key == min_key {
                    if batch.append_below_time(b, min_time) {
                        self.next_vals[pos] = Poll::Pending;
                    }
                    sort_needed = true;
                }
            }
        }

        if sort_needed {
            batch.sort_by_time();
        }

        Poll::Ready(Some(batch))
    }
}

#[derive(Debug, PartialEq, Clone)]
pub enum ReadValues {
    I64(Vec<ReadPoint<i64>>),
    F64(Vec<ReadPoint<f64>>),
}

#[derive(Debug, PartialEq, Clone)]
pub struct ReadBatch {
    pub key: String,
    pub values: ReadValues,
}

impl ReadBatch {
    fn start_stop_times(&self) -> Option<(i64, i64)> {
        match &self.values {
            ReadValues::I64(vals) => {
                if vals.is_empty() {
                    return None;
                }
                Some((vals.first()?.time, vals.last()?.time))
            }
            ReadValues::F64(vals) => {
                if vals.is_empty() {
                    return None;
                }
                Some((vals.first()?.time, vals.last()?.time))
            }
        }
    }

    fn sort_by_time(&mut self) {
        match &mut self.values {
            ReadValues::I64(vals) => vals.sort_by_key(|v| v.time),
            ReadValues::F64(vals) => vals.sort_by_key(|v| v.time),
        }
    }

    // append_below_time will append all values from other that have a time < than the one passed in.
    // it returns true if other has been cleared of all values
    fn append_below_time(&mut self, other: &mut ReadBatch, t: i64) -> bool {
        match (&mut self.values, &mut other.values) {
            (ReadValues::I64(vals), ReadValues::I64(other_vals)) => {
                let pos = other_vals.iter().position(|val| val.time > t);
                match pos {
                    None => {
                        vals.append(other_vals);
                        true
                    }
                    Some(pos) => {
                        let mut rest = other_vals.split_off(pos);
                        vals.append(other_vals);
                        other_vals.append(&mut rest);
                        false
                    }
                }
            }
            (ReadValues::F64(vals), ReadValues::F64(other_vals)) => {
                let pos = other_vals.iter().position(|val| val.time > t);
                match pos {
                    None => {
                        vals.append(other_vals);
                        other_vals.clear();
                        true
                    }
                    Some(pos) => {
                        let mut rest = other_vals.split_off(pos);
                        vals.append(other_vals);
                        other_vals.append(&mut rest);
                        false
                    }
                }
            }
            (_, _) => true, // do nothing here
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use futures::{stream, StreamExt};

    #[test]
    fn string_merge_stream() {
        let one = stream::iter(vec!["a".to_string(), "c".to_string()].into_iter());
        let two = stream::iter(vec!["b".to_string(), "c".to_string(), "d".to_string()].into_iter());
        let three =
            stream::iter(vec!["c".to_string(), "e".to_string(), "f".to_string()].into_iter());
        let four = stream::iter(vec![].into_iter());

        let merger =
            StringMergeStream::new(vec![one.boxed(), two.boxed(), three.boxed(), four.boxed()]);

        let stream = futures::executor::block_on_stream(merger);
        let vals: Vec<String> = stream.collect();

        assert_eq!(
            vals,
            vec![
                "a".to_string(),
                "b".to_string(),
                "c".to_string(),
                "d".to_string(),
                "e".to_string(),
                "f".to_string()
            ],
        );
    }

    #[test]
    fn read_merge_stream() {
        let one = stream::iter(
            vec![
                ReadBatch {
                    key: "foo".to_string(),
                    values: ReadValues::I64(vec![
                        ReadPoint { time: 3, value: 30 },
                        ReadPoint { time: 4, value: 40 },
                    ]),
                },
                ReadBatch {
                    key: "test".to_string(),
                    values: ReadValues::F64(vec![
                        ReadPoint {
                            time: 1,
                            value: 1.1,
                        },
                        ReadPoint {
                            time: 2,
                            value: 2.2,
                        },
                    ]),
                },
            ]
            .into_iter(),
        );

        let two = stream::iter(
            vec![
                ReadBatch {
                    key: "bar".to_string(),
                    values: ReadValues::F64(vec![
                        ReadPoint {
                            time: 5,
                            value: 5.5,
                        },
                        ReadPoint {
                            time: 6,
                            value: 6.6,
                        },
                    ]),
                },
                ReadBatch {
                    key: "foo".to_string(),
                    values: ReadValues::I64(vec![
                        ReadPoint { time: 1, value: 10 },
                        ReadPoint { time: 2, value: 20 },
                        ReadPoint { time: 6, value: 60 },
                        ReadPoint {
                            time: 11,
                            value: 110,
                        },
                    ]),
                },
            ]
            .into_iter(),
        );

        let three = stream::iter(
            vec![ReadBatch {
                key: "foo".to_string(),
                values: ReadValues::I64(vec![
                    ReadPoint { time: 5, value: 50 },
                    ReadPoint {
                        time: 10,
                        value: 100,
                    },
                ]),
            }]
            .into_iter(),
        );

        let four = stream::iter(vec![].into_iter());

        let merger =
            ReadMergeStream::new(vec![one.boxed(), two.boxed(), three.boxed(), four.boxed()]);
        let stream = futures::executor::block_on_stream(merger);
        let vals: Vec<ReadBatch> = stream.collect();

        assert_eq!(
            vals,
            vec![
                ReadBatch {
                    key: "bar".to_string(),
                    values: ReadValues::F64(vec![
                        ReadPoint {
                            time: 5,
                            value: 5.5
                        },
                        ReadPoint {
                            time: 6,
                            value: 6.6
                        },
                    ]),
                },
                ReadBatch {
                    key: "foo".to_string(),
                    values: ReadValues::I64(vec![
                        ReadPoint { time: 1, value: 10 },
                        ReadPoint { time: 2, value: 20 },
                        ReadPoint { time: 3, value: 30 },
                        ReadPoint { time: 4, value: 40 },
                    ]),
                },
                ReadBatch {
                    key: "foo".to_string(),
                    values: ReadValues::I64(vec![
                        ReadPoint { time: 5, value: 50 },
                        ReadPoint { time: 6, value: 60 },
                        ReadPoint {
                            time: 10,
                            value: 100
                        },
                    ]),
                },
                ReadBatch {
                    key: "foo".to_string(),
                    values: ReadValues::I64(vec![ReadPoint {
                        time: 11,
                        value: 110
                    },]),
                },
                ReadBatch {
                    key: "test".to_string(),
                    values: ReadValues::F64(vec![
                        ReadPoint {
                            time: 1,
                            value: 1.1
                        },
                        ReadPoint {
                            time: 2,
                            value: 2.2
                        }
                    ]),
                },
            ],
        )
    }

    use futures::executor;
    use proptest::prelude::*;
    use std::task::Poll;

    /// Given a vector of optional values, put all the `Some` values in sorted order, leaving the
    /// `None` values interspersed as they are.
    fn sort_and_dedup_present_values<T: Ord>(d: &mut Vec<Option<T>>) {
        let mut values = vec![];
        let mut indices = vec![];

        for (i, v) in d.iter_mut().enumerate() {
            if let Some(v) = v.take() {
                values.push(v);
                indices.push(i);
            }
        }

        values.sort();

        // Remove duplicates because this is an invariant upheld by the input streams. If this
        // assumption changes, this code should be updated.
        // Duplicates will become `None` values.
        values.dedup();

        for (v, i) in values.into_iter().zip(indices) {
            d[i] = Some(v);
        }
    }

    /// Adds an arbitrary number of `Poll::Pending` values throughout the `Poll::Ready(Some(T))`
    /// values returned by the stream.
    fn arb_proto_stream<T: Arbitrary + Ord + Clone>() -> impl Strategy<Value = ProtoStream<T>> {
        // Generate a vector of optional values
        any::<Vec<Option<T>>>().prop_map(|mut v| {
            sort_and_dedup_present_values(&mut v);

            // `original` will contain the sorted present values only
            let original = v.clone().into_iter().flatten().collect();
            let poll_values = v
                .into_iter()
                .map(|v| match v {
                    // Convert optional values to `Poll` values
                    Some(v) => Poll::Ready(v),
                    None => Poll::Pending,
                })
                .collect();
            ProtoStream {
                original,
                poll_values,
            }
        })
    }

    #[derive(Debug, Clone)]
    struct ProtoStream<T> {
        original: Vec<T>,
        poll_values: Vec<Poll<T>>,
    }

    impl<T: 'static> ProtoStream<T> {
        fn to_stream(self) -> (Vec<T>, impl Stream<Item = T>) {
            let Self {
                original,
                mut poll_values,
            } = self;

            // Since we pop off the poll values for implementation
            // simplicity, we reverse them first so the original
            // values line up.
            poll_values.reverse();

            let mut exhausted = false;

            (
                original,
                stream::poll_fn(move |ctx| {
                    // Always schedule a wakeup as nothing else will!
                    ctx.waker().wake_by_ref();

                    match poll_values.pop() {
                        Some(v) => v.map(Some),
                        None if !exhausted => {
                            // Always return a `Poll::Ready(None)` when all data has been returned.
                            exhausted = true;
                            Poll::Ready(None)
                        }
                        // A property of `ProtoStream`s is they're never polled after returning
                        // `Poll::Ready(None)`.
                        None => panic!("stream polled after it was exhausted"),
                    }
                }),
            )
        }
    }

    proptest! {
        #[test]
        fn test_add(a in arb_proto_stream::<i32>()) {
            let (values, stream) = a.to_stream();
            let stream_vals: Vec<_> = executor::block_on_stream(stream).collect();
            prop_assert_eq!(&values, &stream_vals);
            prop_assert!(values.is_empty());
        }
    }
}
