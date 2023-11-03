//! hpfeeds-broker.
//!
//! This file is the entry point for the server implemented in the library. It
//! performs command line parsing and passes the arguments on to
//! `hpfeeds_broker::server`.
//!
//! The `clap` crate is used for parsing arguments.

use std::sync::Arc;

use anyhow::Result;
use clap::Parser;
use hpfeeds_broker::{
    parse_endpoint,
    server::{self, Listener},
    start_metrics_server, Db, Endpoint,
};
use prometheus_client::registry::Registry;
use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use tokio::signal;

#[tokio::main]
pub async fn main() -> Result<()> {
    set_up_logging()?;

    let cli = Cli::parse();

    let mut registry = <Registry>::with_prefix("hpfeeds_broker");
    let db = Db::new(&mut registry);

    let mut users = hpfeeds_broker::Users::new();
    if let Some(paths) = cli.auth {
        for path in paths {
            users.add_user_set(path)?;
        }
    }
    let users = Arc::new(users);

    let endpoints = match cli.endpoint {
        Some(endpoints) => endpoints,
        None => vec![parse_endpoint("tcp:interface=127.0.0.1:port=10000")?],
    };

    let (notify_shutdown_tx, notify_shutdown) = tokio::sync::watch::channel(false);
    let mut listeners = vec![];
    for endpoint in endpoints {
        listeners.push(
            Listener::new(endpoint, db.clone(), users.clone(), notify_shutdown.clone()).await?,
        );
    }

    // Spawn a server to serve the OpenMetrics endpoint.
    let metrics_addr = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 8001);
    let metrics_handle =
        start_metrics_server(metrics_addr, registry, notify_shutdown.clone()).await;
    drop(notify_shutdown);

    let handle = tokio::spawn(server::run(listeners));

    signal::ctrl_c().await?;
    notify_shutdown_tx.send(true)?;
    drop(notify_shutdown_tx);

    handle.await?;
    metrics_handle.await??;

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

fn set_up_logging() -> Result<()> {
    // See https://docs.rs/tracing for more info
    Ok(tracing_subscriber::fmt::init())
}
