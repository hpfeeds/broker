//! hpfeeds-broker.
//!
//! This file is the entry point for the server implemented in the library. It
//! performs command line parsing and passes the arguments on to
//! `hpfeeds_broker::server`.
//!
//! The `clap` crate is used for parsing arguments.

use std::sync::Arc;

use clap::Parser;
use hpfeeds_broker::{
    parse_endpoint,
    server::{self, Listener},
    start_metrics_server, Db, Endpoint,
};
use prometheus_client::{metrics::counter::Counter, registry::Registry};
use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use tokio::signal;

#[tokio::main]
pub async fn main() -> hpfeeds_broker::Result<()> {
    set_up_logging()?;

    let cli = Cli::parse();

    let db = Db::new();

    let mut users = hpfeeds_broker::Users::new();
    if let Some(paths) = cli.auth {
        for path in paths {
            users.add_user_set(path)?;
        }
    }
    let users = Arc::new(users);

    let endpoints = match cli.endpoint {
        Some(endpoints) => endpoints,
        None => vec![parse_endpoint("tcp:interface=127.0.0.1:port=10000").unwrap()],
    };

    let (notify_shutdown_tx, notify_shutdown) = tokio::sync::watch::channel(false);
    let mut listeners = vec![];
    for endpoint in endpoints {
        listeners.push(
            Listener::new(endpoint, db.clone(), users.clone(), notify_shutdown.clone()).await,
        );
    }
    drop(notify_shutdown);

    let request_counter: Counter<u64> = Default::default();

    let mut registry = <Registry>::with_prefix("hpfeeds_broker");

    registry.register(
        "requests",
        "How many requests the application has received",
        request_counter.clone(),
    );

    // Spawn a server to serve the OpenMetrics endpoint.
    let metrics_addr = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 8001);

    start_metrics_server(metrics_addr, registry).await;

    let handle = tokio::spawn(server::run(listeners));

    signal::ctrl_c().await?;
    notify_shutdown_tx.send(true)?;
    drop(notify_shutdown_tx);

    handle.await?;

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

fn set_up_logging() -> hpfeeds_broker::Result<()> {
    // See https://docs.rs/tracing for more info
    tracing_subscriber::fmt::try_init()
}
