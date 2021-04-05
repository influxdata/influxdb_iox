use super::run::Config;
use observability_deps::{
    opentelemetry, opentelemetry_prometheus,
    prometheus::{Encoder, TextEncoder},
    tracing::log::warn,
};
use parking_lot::{const_rwlock, RwLock};

// TODO(jacobmarble): better way to write-once-read-many without a lock
// TODO(jacobmarble): generic OTel exporter, rather than just prometheus
static PROMETHEUS_EXPORTER: RwLock<Option<opentelemetry_prometheus::PrometheusExporter>> =
    const_rwlock(None);

/// Initializes metrics. See [`meter`] for example usage
pub fn init_metrics(_config: &Config) {
    // TODO add flags to config, to configure the OpenTelemetry exporter (OTLP)
    // This sets the global meter provider, for other code to use
    let exporter = opentelemetry_prometheus::exporter().init();
    init_metrics_internal(exporter)
}

#[cfg(test)]
pub fn init_metrics_for_test() {
    // Plan to skip all config flags
    let exporter = opentelemetry_prometheus::exporter().init();
    init_metrics_internal(exporter)
}

pub fn init_metrics_internal(exporter: opentelemetry_prometheus::PrometheusExporter) {
    let mut guard = PROMETHEUS_EXPORTER.write();
    if guard.is_some() {
        warn!("metrics were already initialized, overwriting configuration");
    }
    *guard = Some(exporter);
}

/// Returns the global IOx [`Meter`] for reporting metrics
///
/// # Example
///
/// ```
/// let meter = crate::influxdb_ioxd::metrics::meter();
///
/// let counter = meter
///     .u64_counter("a.counter")
///     .with_description("Counts things")
///     .init();
/// let recorder = meter
///     .i64_value_recorder("a.value_recorder")
///     .with_description("Records values")
///     .init();
///
/// counter.add(100, &[KeyValue::new("key", "value")]);
/// recorder.record(100, &[KeyValue::new("key", "value")]);
/// ```
pub fn meter() -> opentelemetry::metrics::Meter {
    opentelemetry::global::meter("iox")
}

/// Gets current metrics state, in UTF-8 encoded Prometheus Exposition Format.
/// https://prometheus.io/docs/instrumenting/exposition_formats/
///
/// # Example
///
/// ```
/// # HELP a_counter Counts things
/// # TYPE a_counter counter
/// a_counter{key="value"} 100
/// # HELP a_value_recorder Records values
/// # TYPE a_value_recorder histogram
/// a_value_recorder_bucket{key="value",le="0.5"} 0
/// a_value_recorder_bucket{key="value",le="0.9"} 0
/// a_value_recorder_bucket{key="value",le="0.99"} 0
/// a_value_recorder_bucket{key="value",le="+Inf"} 1
/// a_value_recorder_sum{key="value"} 100
/// a_value_recorder_count{key="value"} 1
/// ```
pub fn metrics_as_text() -> Vec<u8> {
    let metric_families = PROMETHEUS_EXPORTER
        .read()
        .as_ref()
        .unwrap()
        .registry()
        .gather();
    let mut result = Vec::new();
    TextEncoder::new()
        .encode(&metric_families, &mut result)
        .unwrap();
    result
}
