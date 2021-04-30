use std::sync::Arc;

use snafu::{ensure, OptionExt, Snafu};

use observability_deps::prometheus::proto::{
    Counter as PromCounter, Histogram as PromHistogram, MetricFamily,
};

use crate::MetricRegistry;

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("no metric family with name: {}\n{}", name, metrics))]
    MetricFamilyNotFoundError { name: String, metrics: String },

    #[snafu(display("labels {:?} do not match metric: {}\n{}", labels, name, metrics))]
    NoMatchingLabelsError {
        labels: Vec<(String, String)>,
        name: String,
        metrics: String,
    },

    #[snafu(display("bucket {:?} is not in metric family: {}\n{}", bound, name, metrics))]
    HistogramBucketNotFoundError {
        bound: f64,
        name: String,
        metrics: String,
    },

    #[snafu(display("metric '{}' failed assertion: '{}'\n{}", name, msg, metrics))]
    FailedMetricAssertionError {
        name: String,
        msg: String,
        metrics: String,
    },
}

pub type Result<T, E = Error> = std::result::Result<T, E>;

/// A metric registry with handy helpers for asserting conditions on metrics.
///
/// You can either initialise a default `TestMetricRegistry` and use the
/// `registry()` method to inject a `MetricRegistry` wherever you need it.
/// Alternatively you can provide an existing `MetricRegistry` via `new`.
///
///
/// The main purpose of the `TestRegistry` is to provide a simple API to assert
/// that metrics exist and have certain values.
///
/// Please see the test cases at the top of this crate for example usage.
#[derive(Debug, Default)]
pub struct TestMetricRegistry {
    registry: Arc<MetricRegistry>,
}

impl TestMetricRegistry {
    pub fn new(registry: Arc<MetricRegistry>) -> Self {
        Self { registry }
    }

    pub fn registry(&self) -> Arc<MetricRegistry> {
        Arc::clone(&self.registry)
    }

    /// Returns an assertion builder for the specified metric name or an error
    /// if one doesn't exist.
    ///
    /// Note: Prometheus histograms comprise multiple metrics sharing the same
    /// family. Use the family name, e.g., `http_request_duration_seconds` to
    /// get access to the individual metrics via the `histogram` method on the
    /// returned `AssertionBuilder`.
    pub fn try_has_metric_family<'a>(&'a self, name: &str) -> Result<AssertionBuilder<'a>, Error> {
        let metric_families = self.registry.exporter.registry().gather();
        let family = metric_families
            .into_iter()
            .find(|fam| fam.get_name() == name)
            .context(MetricFamilyNotFoundError {
                name,
                metrics: self.registry.metrics_as_str(),
            })?;
        Ok(AssertionBuilder::new(family, &self.registry))
    }

    /// Returns an assertion builder for the specified metric name.
    ///
    /// # Panics
    ///
    /// Panics if no metric family has `name`. To avoid a panic see
    /// `try_has_metric`.
    pub fn has_metric_family<'a>(&'a self, name: &str) -> AssertionBuilder<'a> {
        self.try_has_metric_family(name).unwrap()
    }
}

#[derive(Debug)]
pub struct AssertionBuilder<'a> {
    family: MetricFamily,
    labels: Vec<(String, String)>,
    registry: &'a MetricRegistry,
}

impl<'a> AssertionBuilder<'a> {
    fn new(family: MetricFamily, registry: &'a MetricRegistry) -> Self {
        Self {
            family,
            labels: vec![],
            registry,
        }
    }

    /// Assert that the metric has the following set of labels.
    pub fn with_labels(mut self, labels: &[(&'static str, &'static str)]) -> Self {
        for (key, value) in labels {
            self.labels.push((key.to_string(), value.to_string()));
        }

        self
    }

    /// Returns the counter metric, allowing assertions to be applied.
    pub fn counter(&mut self) -> Counter<'_> {
        // sort the assertion's labels
        self.labels.sort_by(|a, b| a.0.cmp(&b.0));

        let metric = self.family.get_metric().iter().find(|metric| {
            if metric.get_label().len() != self.labels.len() {
                return false; // this metric can't match
            }

            // sort this metrics labels and compare to assertion labels
            let mut metric_labels = metric.get_label().to_vec();
            metric_labels.sort_by(|a, b| a.get_name().cmp(b.get_name()));

            // metric only matches if all labels are identical.
            metric_labels
                .iter()
                .zip(self.labels.iter())
                .all(|(a, b)| a.get_name() == b.0 && a.get_value() == b.1)
        });

        // Can't find metric matching labels
        if metric.is_none() {
            return Counter {
                c: NoMatchingLabelsError {
                    name: self.family.get_name().to_owned(),
                    labels: self.labels.clone(),
                    metrics: self.registry.metrics_as_str(),
                }
                .fail(),
                family_name: "".to_string(),
                metric_dump: "".to_string(),
            };
        }
        let metric = metric.unwrap();

        if !metric.has_counter() {
            return Counter {
                c: FailedMetricAssertionError {
                    name: self.family.get_name().to_owned(),
                    msg: "metric not a counter".to_owned(),
                    metrics: self.registry.metrics_as_str(),
                }
                .fail(),
                family_name: "".to_string(),
                metric_dump: "".to_string(),
            };
        }

        Counter {
            c: Ok(metric.get_counter()),
            family_name: self.family.get_name().to_owned(),
            metric_dump: self.registry.metrics_as_str(),
        }
    }

    /// Returns the histogram metric, allowing assertions to be applied.
    pub fn histogram(&mut self) -> Histogram<'_> {
        // sort the assertion's labels
        self.labels.sort_by(|a, b| a.0.cmp(&b.0));

        let metric = self.family.get_metric().iter().find(|metric| {
            if metric.get_label().len() != self.labels.len() {
                return false; // this metric can't match
            }

            // sort this metrics labels and compare to assertion labels
            let mut metric_labels = metric.get_label().to_vec();
            metric_labels.sort_by(|a, b| a.get_name().cmp(b.get_name()));

            // metric only matches if all labels are identical.
            metric_labels
                .iter()
                .zip(self.labels.iter())
                .all(|(a, b)| a.get_name() == b.0 && a.get_value() == b.1)
        });

        // Can't find metric matching labels
        let metric = match metric {
            Some(metric) => metric,
            None => {
                return Histogram {
                    c: NoMatchingLabelsError {
                        name: self.family.get_name(),
                        labels: self.labels.clone(), // Maybe `labels: &self.labels`
                        metrics: self.registry.metrics_as_str(),
                    }
                    .fail(),
                    family_name: "".to_string(),
                    metric_dump: "".to_string(),
                };
            }
        };

        if !metric.has_histogram() {
            return Histogram {
                c: FailedMetricAssertionError {
                    name: self.family.get_name().to_owned(),
                    msg: "metric not a counter".to_owned(),
                    metrics: self.registry.metrics_as_str(),
                }
                .fail(),
                family_name: "".to_string(),
                metric_dump: "".to_string(),
            };
        }

        Histogram {
            c: Ok(metric.get_histogram()),
            family_name: self.family.get_name().to_owned(),
            metric_dump: self.registry.metrics_as_str(),
        }
    }
}

#[derive(Debug)]
pub struct Counter<'a> {
    // if there was a problem getting the counter based on labels then the
    // Error will contain details.
    c: Result<&'a PromCounter, Error>,

    family_name: String,
    metric_dump: String,
}

impl<'a> Counter<'a> {
    pub fn eq(self, v: f64) -> Result<(), Error> {
        let c = self.c?; // return previous errors

        ensure!(
            v == c.get_value(),
            FailedMetricAssertionError {
                name: self.family_name,
                msg: format!("{:?} == {:?} failed", c.get_value(), v),
                metrics: self.metric_dump,
            }
        );
        Ok(())
    }

    pub fn gte(self, v: f64) -> Result<(), Error> {
        let c = self.c?; // return previous errors

        ensure!(
            c.get_value() >= v,
            FailedMetricAssertionError {
                name: self.family_name,
                msg: format!("{:?} >= {:?} failed", c.get_value(), v),
                metrics: self.metric_dump,
            }
        );
        Ok(())
    }

    pub fn gt(self, v: f64) -> Result<(), Error> {
        let c = self.c?; // return previous errors

        ensure!(
            c.get_value() > v,
            FailedMetricAssertionError {
                name: self.family_name,
                msg: format!("{:?} > {:?} failed", c.get_value(), v),
                metrics: self.metric_dump,
            }
        );
        Ok(())
    }

    pub fn lte(self, v: f64) -> Result<(), Error> {
        let c = self.c?; // return previous errors

        ensure!(
            c.get_value() <= v,
            FailedMetricAssertionError {
                name: self.family_name,
                msg: format!("{:?} <= {:?} failed", c.get_value(), v),
                metrics: self.metric_dump,
            }
        );
        Ok(())
    }

    pub fn lt(self, v: f64) -> Result<(), Error> {
        let c = self.c?; // return previous errors

        ensure!(
            c.get_value() < v,
            FailedMetricAssertionError {
                name: self.family_name,
                msg: format!("{:?} < {:?} failed", c.get_value(), v),
                metrics: self.metric_dump,
            }
        );
        Ok(())
    }
}

#[derive(Debug)]
pub struct Histogram<'a> {
    // if there was a problem getting the counter based on labels then the
    // Error will contain details.
    c: Result<&'a PromHistogram, Error>,

    family_name: String,
    metric_dump: String,
}

impl<'a> Histogram<'a> {
    pub fn bucket_cumulative_count_eq(self, bound: f64, count: u64) -> Result<(), Error> {
        let c = self.c?; // return previous errors

        let bucket = c
            .get_bucket()
            .iter()
            .find(|bucket| bucket.get_upper_bound() == bound)
            .context(HistogramBucketNotFoundError {
                bound,
                name: &self.family_name,
                metrics: &self.metric_dump,
            })?;

        ensure!(
            count == bucket.get_cumulative_count(),
            FailedMetricAssertionError {
                name: &self.family_name,
                msg: format!("{:?} == {:?} failed", bucket.get_cumulative_count(), count),
                metrics: self.metric_dump,
            }
        );

        Ok(())
    }

    pub fn sample_sum_eq(self, v: f64) -> Result<(), Error> {
        let c = self.c?; // return previous errors

        ensure!(
            v == c.get_sample_sum(),
            FailedMetricAssertionError {
                name: self.family_name,
                msg: format!("{:?} == {:?} failed", c.get_sample_sum(), v),
                metrics: self.metric_dump,
            }
        );
        Ok(())
    }

    pub fn sample_sum_gte(self, v: f64) -> Result<(), Error> {
        let c = self.c?; // return previous errors

        ensure!(
            c.get_sample_sum() >= v,
            FailedMetricAssertionError {
                name: self.family_name,
                msg: format!("{:?} >= {:?} failed", c.get_sample_sum(), v),
                metrics: self.metric_dump,
            }
        );
        Ok(())
    }

    pub fn sample_sum_gt(self, v: f64) -> Result<(), Error> {
        let c = self.c?; // return previous errors

        ensure!(
            c.get_sample_sum() > v,
            FailedMetricAssertionError {
                name: self.family_name,
                msg: format!("{:?} > {:?} failed", c.get_sample_sum(), v),
                metrics: self.metric_dump,
            }
        );
        Ok(())
    }

    pub fn sample_sum_lte(self, v: f64) -> Result<(), Error> {
        let c = self.c?; // return previous errors

        ensure!(
            c.get_sample_sum() <= v,
            FailedMetricAssertionError {
                name: self.family_name,
                msg: format!("{:?} <= {:?} failed", c.get_sample_sum(), v),
                metrics: self.metric_dump,
            }
        );
        Ok(())
    }

    pub fn sample_sum_lt(self, v: f64) -> Result<(), Error> {
        let c = self.c?; // return previous errors

        ensure!(
            c.get_sample_sum() < v,
            FailedMetricAssertionError {
                name: self.family_name,
                msg: format!("{:?} < {:?} failed", c.get_sample_sum(), v),
                metrics: self.metric_dump,
            }
        );
        Ok(())
    }

    pub fn sample_count_eq(self, v: u64) -> Result<(), Error> {
        let c = self.c?; // return previous errors

        ensure!(
            c.get_sample_count() == v,
            FailedMetricAssertionError {
                name: self.family_name,
                msg: format!("{:?} == {:?} failed", c.get_sample_count(), v),
                metrics: self.metric_dump,
            }
        );
        Ok(())
    }

    pub fn sample_count_gte(self, v: u64) -> Result<(), Error> {
        let c = self.c?; // return previous errors

        ensure!(
            c.get_sample_count() >= v,
            FailedMetricAssertionError {
                name: self.family_name,
                msg: format!("{:?} >= {:?} failed", c.get_sample_count(), v),
                metrics: self.metric_dump,
            }
        );
        Ok(())
    }

    pub fn sample_count_gt(self, v: u64) -> Result<(), Error> {
        let c = self.c?; // return previous errors

        ensure!(
            c.get_sample_count() > v,
            FailedMetricAssertionError {
                name: self.family_name,
                msg: format!("{:?} > {:?} failed", c.get_sample_count(), v),
                metrics: self.metric_dump,
            }
        );
        Ok(())
    }

    pub fn sample_count_lte(self, v: u64) -> Result<(), Error> {
        let c = self.c?; // return previous errors

        ensure!(
            c.get_sample_count() <= v,
            FailedMetricAssertionError {
                name: self.family_name,
                msg: format!("{:?} <= {:?} failed", c.get_sample_count(), v),
                metrics: self.metric_dump,
            }
        );
        Ok(())
    }

    pub fn sample_count_lt(self, v: u64) -> Result<(), Error> {
        let c = self.c?; // return previous errors

        ensure!(
            c.get_sample_count() < v,
            FailedMetricAssertionError {
                name: self.family_name,
                msg: format!("{:?} < {:?} failed", c.get_sample_count(), v),
                metrics: self.metric_dump,
            }
        );
        Ok(())
    }
}
