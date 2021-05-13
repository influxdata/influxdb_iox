use std::collections::hash_map::Entry;
use std::collections::HashMap;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

use dashmap::DashMap;

use crate::KeyValue;
use observability_deps::opentelemetry::labels::{DefaultLabelEncoder, LabelSet};

/// A `Gauge` allows tracking multiple usize values by label set
///
/// Metrics can be recorded directly on the Gauge or when the labels are
/// known ahead of time a `GaugeValue` can be obtained with `Gauge::gauge_value`
///
/// When a `Gauge` is dropped any contributions it made to any label sets
/// will be deducted
#[derive(Debug, Default)]
pub struct Gauge {
    shared: Arc<GaugeShared>,
    values: HashMap<String, GaugeValue>,
    default_labels: Vec<KeyValue>,
}

impl Clone for Gauge {
    fn clone(&self) -> Self {
        Self {
            shared: Arc::clone(&self.shared),
            values: Default::default(),
            default_labels: self.default_labels.clone(),
        }
    }
}

impl Gauge {
    pub fn new(default_labels: Vec<KeyValue>) -> Self {
        Self {
            shared: Default::default(),
            values: Default::default(),
            default_labels,
        }
    }

    /// Sets the default labels for this Guage
    pub(crate) fn set_labels(&mut self, default_labels: Vec<KeyValue>) {
        self.default_labels = default_labels;
    }

    /// Gets a `GaugeValue` for a given set of labels
    /// This allows fast value updates and retrieval when the tags are known in advance
    pub fn gauge_value(&self, labels: &[KeyValue]) -> GaugeValue {
        let (encoded, keys) = self.encode_labels(labels);
        self.shared.gauge_value(encoded, keys)
    }

    /// Visits the totals for all label sets recorded by this Gauge
    pub fn visit_values(&self, f: impl Fn(usize, &Vec<KeyValue>)) {
        for data in self.shared.values.iter() {
            let (data, labels) = data.value();
            f(data.get_total(), labels)
        }
    }

    pub fn inc(&mut self, delta: usize, labels: &[KeyValue]) {
        self.call(labels, |observer| observer.inc(delta))
    }

    pub fn decr(&mut self, delta: usize, labels: &[KeyValue]) {
        self.call(labels, |observer| observer.decr(delta))
    }

    pub fn set(&mut self, value: usize, labels: &[KeyValue]) {
        self.call(labels, |observer| observer.set(value))
    }

    fn encode_labels(&self, labels: &[KeyValue]) -> (String, LabelSet) {
        self.shared
            .encode_labels(self.default_labels.iter().chain(labels).cloned())
    }

    fn call(&mut self, labels: &[KeyValue], f: impl Fn(&mut GaugeValue)) {
        let (encoded, keys) = self.encode_labels(labels);

        match self.values.entry(encoded.clone()) {
            Entry::Occupied(mut occupied) => f(occupied.get_mut()),
            Entry::Vacant(vacant) => {
                let observer = self.shared.gauge_value(encoded, keys);
                f(vacant.insert(observer))
            }
        }
    }
}

#[derive(Debug, Default)]
struct GaugeShared {
    /// Maps a set of tags to a gauge and its tags
    values: DashMap<String, (GaugeValue, Vec<KeyValue>)>,
}

impl GaugeShared {
    fn encode_labels(&self, labels: impl IntoIterator<Item = KeyValue>) -> (String, LabelSet) {
        let set = LabelSet::from_labels(labels);
        let encoded = set.encoded(Some(&DefaultLabelEncoder));

        (encoded, set)
    }

    fn gauge_value(&self, encoded: String, keys: LabelSet) -> GaugeValue {
        self.values
            .entry(encoded)
            .or_insert_with(|| {
                (
                    Default::default(),
                    keys.iter()
                        .map(|(key, value)| KeyValue::new(key.clone(), value.clone()))
                        .collect(),
                )
            })
            .value()
            .0
            .clone()
    }
}

#[derive(Debug, Default)]
struct GaugeValueShared {
    /// The total bytes across all associated `GaugeValueHandle`s
    bytes: AtomicUsize,
}

/// A `GaugeValue` is a single measurement associated with a `Gauge`
///
/// When the `GaugeValue` is dropped any contribution it made to the total
/// will be deducted from the `GaugeValue` total
#[derive(Debug, Default)]
pub struct GaugeValue {
    shared: Arc<GaugeValueShared>,
    /// The locally observed value
    local: usize,
}

impl Clone for GaugeValue {
    fn clone(&self) -> Self {
        Self {
            shared: Arc::clone(&self.shared),
            local: 0,
        }
    }
}

impl GaugeValue {
    /// Gets the total value from across all `GaugeValue` associated
    /// with this `Gauge`
    pub fn get_total(&self) -> usize {
        self.shared.bytes.load(Ordering::Relaxed)
    }

    /// Gets the local contribution from this instance
    pub fn get_local(&self) -> usize {
        self.local
    }

    /// Increment the local value for this GaugeValue
    pub fn inc(&mut self, delta: usize) {
        self.local += delta;
        self.shared.bytes.fetch_add(delta, Ordering::Relaxed);
    }

    /// Decrement the local value for this GaugeValue
    pub fn decr(&mut self, delta: usize) {
        self.local -= delta;
        self.shared.bytes.fetch_sub(delta, Ordering::Relaxed);
    }

    /// Sets the local value for this GaugeValue
    pub fn set(&mut self, new: usize) {
        if new > self.local {
            self.inc(new - self.local)
        } else {
            self.decr(self.local - new)
        }
    }
}

impl Drop for GaugeValue {
    fn drop(&mut self) {
        self.shared.bytes.fetch_sub(self.local, Ordering::Relaxed);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_tracker() {
        let start = GaugeValue::default();
        let mut t1 = start.clone();
        let mut t2 = start.clone();

        t1.set(200);

        assert_eq!(t1.get_total(), 200);
        assert_eq!(t2.get_total(), 200);
        assert_eq!(start.get_total(), 200);

        t1.set(100);

        assert_eq!(t1.get_total(), 100);
        assert_eq!(t2.get_total(), 100);
        assert_eq!(start.get_total(), 100);

        t2.set(300);
        assert_eq!(t1.get_total(), 400);
        assert_eq!(t2.get_total(), 400);
        assert_eq!(start.get_total(), 400);

        t2.set(400);
        assert_eq!(t1.get_total(), 500);
        assert_eq!(t2.get_total(), 500);
        assert_eq!(start.get_total(), 500);

        std::mem::drop(t2);
        assert_eq!(t1.get_total(), 100);
        assert_eq!(start.get_total(), 100);

        std::mem::drop(t1);
        assert_eq!(start.get_total(), 0);
    }

    #[test]
    fn test_mixed() {
        let mut gauge = Gauge::new(vec![KeyValue::new("foo", "bar")]);
        let mut gauge2 = gauge.clone();

        gauge.set(32, &[KeyValue::new("bingo", "bongo")]);
        gauge2.set(64, &[KeyValue::new("bingo", "bongo")]);

        let mut gauge_value = gauge.gauge_value(&[KeyValue::new("bingo", "bongo")]);
        gauge_value.set(12);

        assert_eq!(gauge_value.get_local(), 12);
        assert_eq!(gauge_value.get_total(), 32 + 64 + 12);

        std::mem::drop(gauge2);
        assert_eq!(gauge_value.get_total(), 32 + 12);

        gauge.inc(5, &[KeyValue::new("no", "match")]);
        assert_eq!(gauge_value.get_total(), 32 + 12);

        gauge.inc(5, &[KeyValue::new("bingo", "bongo")]);
        assert_eq!(gauge_value.get_total(), 32 + 12 + 5);

        std::mem::drop(gauge);

        assert_eq!(gauge_value.get_total(), 12);
        assert_eq!(gauge_value.get_local(), 12);
    }
}
