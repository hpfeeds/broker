//! hpfeeds-broker.
//!
//! This file is the entry point for the server implemented in the library. It
//! performs command line parsing and passes the arguments on to
//! `hpfeeds_broker::server`.
//!
//! The `clap` crate is used for parsing arguments.

use std::sync::Arc;

use hpfeeds_broker::{parse_endpoint, server, Endpoint};

use clap::Parser;
use tokio::signal;

#[cfg(feature = "otel")]
// To be able to set the XrayPropagator
use opentelemetry::global;
#[cfg(feature = "otel")]
// To configure certain options such as sampling rate
use opentelemetry::sdk::trace as sdktrace;
#[cfg(feature = "otel")]
// The `Ext` traits are to allow the Registry to accept the
// OpenTelemetry-specific types (such as `OpenTelemetryLayer`)
use tracing_subscriber::{
    fmt, layer::SubscriberExt, util::SubscriberInitExt, util::TryInitError, EnvFilter,
};

#[tokio::main]
pub async fn main() -> hpfeeds_broker::Result<()> {
    set_up_logging()?;

    let cli = Cli::parse();

    let mut users = hpfeeds_broker::Users::new();
    if let Some(paths) = cli.auth {
        for path in paths {
            users.add_user_set(path)?;
        }
    }

    let endpoints = match cli.endpoint {
        Some(endpoints) => endpoints,
        None => vec![parse_endpoint("tcp:interface=127.0.0.1:port=10000").unwrap()],
    };

    println!("{:?}", endpoints);

    server::run(Arc::new(users), endpoints, signal::ctrl_c()).await;

    Ok(())
}

#[derive(Parser, Debug)]
#[clap(
    name = "hpfeeds-broker",
    version,
    author,
    about = "A HPFeeds event broker"
)]
struct Cli {
    #[clap(long)]
    auth: Option<Vec<String>>,
    #[arg(long, value_parser = parse_endpoint)]
    endpoint: Option<Vec<Endpoint>>,
}

#[cfg(not(feature = "otel"))]
fn set_up_logging() -> hpfeeds_broker::Result<()> {
    // See https://docs.rs/tracing for more info
    tracing_subscriber::fmt::try_init()
}

#[cfg(feature = "otel")]
fn set_up_logging() -> Result<(), TryInitError> {
    let tracer = opentelemetry_otlp::new_pipeline()
        .tracing()
        .with_exporter(opentelemetry_otlp::new_exporter().tonic())
        .with_trace_config(sdktrace::config().with_sampler(sdktrace::Sampler::AlwaysOn))
        .install_simple()
        .expect("Unable to initialize OtlpPipeline");

    // Create a tracing layer with the configured tracer
    let opentelemetry = tracing_opentelemetry::layer().with_tracer(tracer);

    // Parse an `EnvFilter` configuration from the `RUST_LOG`
    // environment variable.
    let filter = EnvFilter::from_default_env();

    // Use the tracing subscriber `Registry`, or any other subscriber
    // that impls `LookupSpan`
    tracing_subscriber::registry()
        .with(opentelemetry)
        .with(filter)
        .with(fmt::Layer::default())
        .try_init()
}
