//! Log and trace initialization and setup

use clap::arg_enum;
use observability_deps::{
    opentelemetry,
    opentelemetry::sdk::trace,
    opentelemetry::sdk::Resource,
    opentelemetry::KeyValue,
    opentelemetry_jaeger, opentelemetry_otlp, tracing, tracing_opentelemetry,
    tracing_subscriber::{self, fmt, layer::SubscriberExt, EnvFilter},
};

/// Start log or trace emitter. Panics on error.
pub fn init_logs_and_tracing(config: &crate::commands::run::Config) -> TracingGuard {
    let (traces_layer_filter, traces_layer_otel) = match &config.traces_filter {
        None => (None, None),
        Some(traces_filter) => match construct_opentelemetry_tracer(config) {
            None => (None, None),
            Some(tracer) => (
                Some(EnvFilter::try_new(traces_filter).unwrap()),
                Some(tracing_opentelemetry::OpenTelemetryLayer::new(tracer)),
            ),
        },
    };

    let (log_layer_filter, log_layer_format_full, log_layer_format_pretty, log_layer_format_json) = {
        match traces_layer_filter {
            Some(_) => (None, None, None, None),
            None => {
                let (log_format_full, log_format_pretty, log_format_json) = match config.log_format
                {
                    LogFormat::Full => (Some(fmt::layer()), None, None),
                    LogFormat::Pretty => (None, Some(fmt::layer().pretty()), None),
                    LogFormat::Json => (None, None, Some(fmt::layer().json())),
                };

                let log_layer_filter = match config.log_verbose_count {
                    0 => EnvFilter::try_new(&config.log_filter).unwrap(),
                    1 => EnvFilter::try_new("info").unwrap(),
                    2 => EnvFilter::try_new("debug,hyper::proto::h1=info,h2=info").unwrap(),
                    _ => EnvFilter::try_new("trace,hyper::proto::h1=info,h2=info").unwrap(),
                };
                (
                    Some(log_layer_filter),
                    log_format_full,
                    log_format_pretty,
                    log_format_json,
                )
            }
        }
    };

    let subscriber = tracing_subscriber::Registry::default()
        .with(log_layer_format_json)
        .with(log_layer_format_pretty)
        .with(log_layer_format_full)
        .with(log_layer_filter)
        .with(traces_layer_otel)
        .with(traces_layer_filter);

    let tracing_guard = tracing::subscriber::set_default(subscriber);

    TracingGuard(tracing_guard)
}

fn construct_opentelemetry_tracer(config: &crate::commands::run::Config) -> Option<trace::Tracer> {
    let trace_config = {
        let sampler = match config.traces_sampler {
            TracesSampler::AlwaysOn => trace::Sampler::AlwaysOn,
            TracesSampler::AlwaysOff => {
                return None;
            }
            TracesSampler::TraceIdRatio => {
                trace::Sampler::TraceIdRatioBased(config.traces_sampler_arg)
            }
            TracesSampler::ParentBasedAlwaysOn => {
                trace::Sampler::ParentBased(Box::new(trace::Sampler::AlwaysOn))
            }
            TracesSampler::ParentBasedAlwaysOff => {
                trace::Sampler::ParentBased(Box::new(trace::Sampler::AlwaysOff))
            }
            TracesSampler::ParentBasedTraceIdRatio => trace::Sampler::ParentBased(Box::new(
                trace::Sampler::TraceIdRatioBased(config.traces_sampler_arg),
            )),
        };
        let resource = Resource::new(vec![KeyValue::new("service.name", "influxdb-iox")]);
        trace::Config::default()
            .with_sampler(sampler)
            .with_resource(resource)
    };

    match config.traces_exporter {
        Some(TracesExporter::Jaeger) => {
            let agent_endpoint = format!(
                "{}:{}",
                config.traces_exporter_jaeger_agent_host.trim(),
                config.traces_exporter_jaeger_agent_port
            );
            opentelemetry::global::set_text_map_propagator(opentelemetry_jaeger::Propagator::new());
            Some(
                opentelemetry_jaeger::new_pipeline()
                    .with_trace_config(trace_config)
                    .with_agent_endpoint(agent_endpoint)
                    .install_batch(opentelemetry::runtime::Tokio)
                    .unwrap(),
            )
        }

        Some(TracesExporter::Otlp) => Some(
            opentelemetry_otlp::new_pipeline()
                .with_trace_config(trace_config)
                .with_endpoint(config.traces_exporter_otlp_endpoint.clone())
                .with_protocol(opentelemetry_otlp::Protocol::Grpc)
                .with_tonic()
                .install_batch(opentelemetry::runtime::Tokio)
                .unwrap(),
        ),

        None => None,
    }
}

/// An RAII guard. On Drop, tracing and OpenTelemetry are flushed and shut down.
pub struct TracingGuard(tracing::subscriber::DefaultGuard);

impl Drop for TracingGuard {
    fn drop(&mut self) {
        opentelemetry::global::shutdown_tracer_provider();
    }
}

arg_enum! {
    #[derive(Debug, Clone, Copy)]
    pub enum LogFormat {
        Full,
        Pretty,
        Json,
    }
}

// impl std::str::FromStr for LogFormat {
//     type Err = String;
//
//     fn from_str(s: &str) -> Result<Self, Self::Err> {
//         match s.to_ascii_lowercase().as_str() {
//             "full" => Ok(Self::Full),
//             "pretty" => Ok(Self::Pretty),
//             "json" => Ok(Self::Json),
//             _ => Err(format!(
//                 "Invalid log format '{}'. Valid options: full, pretty, json",
//                 s
//             )),
//         }
//     }
// }

arg_enum! {
    #[derive(Debug, Clone, Copy)]
    pub enum TracesExporter {
        Jaeger,
        Otlp,
    }
}

// impl std::str::FromStr for TracesExporter {
//     type Err = String;
//
//     fn from_str(s: &str) -> Result<Self, Self::Err> {
//         match s.to_ascii_lowercase().as_str() {
//             "jaeger" => Ok(Self::Jaeger),
//             "otlp" => Ok(Self::Otlp),
//             _ => Err(format!(
//                 "Invalid traces exporter '{}'. Valid options: jaeger, otlp",
//                 s
//             )),
//         }
//     }
// }

arg_enum! {
    #[derive(Debug, Clone, Copy)]
    pub enum TracesSampler {
        AlwaysOn,
        AlwaysOff,
        TraceIdRatio,
        ParentBasedAlwaysOn,
        ParentBasedAlwaysOff,
        ParentBasedTraceIdRatio,
    }
}

// impl std::str::FromStr for TracesSampler {
//     type Err = String;
//
//     fn from_str(s: &str) -> Result<Self, Self::Err> {
//         match s.to_ascii_lowercase().as_str() {
//             "always_on" => Ok(Self::AlwaysOn),
//             "always_off" => Ok(Self::AlwaysOff),
//             "traceidratio" => Ok(Self::TraceIdRatio),
//             "parentbased_always_on" => Ok(Self::ParentBasedAlwaysOn),
//             "parentbased_always_off" => Ok(Self::ParentBasedAlwaysOff),
//             "parentbased_traceidratio" => Ok(Self::ParentBasedTraceIdRatio),
//             _ => Err(format!(
//                 "Invalid traces sampler '{}'. Valid options: always_on,
// always_off, traceidratio, parentbased_always_on, parentbased_always_off,
// parentbased_traceidratio",                 s
//             )),
//         }
//     }
// }
