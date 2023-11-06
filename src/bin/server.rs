//! hpfeeds-broker.
//!
//! This file is the entry point for the server implemented in the library. It
//! performs command line parsing and passes the arguments on to
//! `hpfeeds_broker::server`.
//!
//! The `clap` crate is used for parsing arguments.

use std::sync::Arc;

use anyhow::{bail, Context, Result};
use clap::Parser;
use hpfeeds_broker::{
    parse_endpoint,
    server::{self, Listener},
    start_metrics_server, Db, Endpoint, ListenerClass,
};
use prometheus_client::registry::Registry;
use tokio::signal;

#[tokio::main]
pub async fn main() -> Result<()> {
    set_up_logging()?;

    let cli = Cli::parse();

    if cli.tlscert.is_some() && cli.tlskey.is_none() {
        bail!("--tlscert is set but --tlskey is not");
    }

    if cli.tlskey.is_some() && cli.tlscert.is_none() {
        bail!("--tlskey is set but --tlscert is not");
    }

    let mut registry = <Registry>::with_prefix("hpfeeds_broker");
    let db = Db::new(&mut registry);

    let mut users = hpfeeds_broker::Users::new();
    if let Some(paths) = cli.auth {
        for path in paths {
            users.add_user_set(path)?;
        }
    }
    let users = Arc::new(users);

    let mut endpoints = match cli.endpoint {
        Some(endpoints) => endpoints,
        None => vec![],
    };

    if cli.bind.is_some() || endpoints.is_empty() {
        let bind = match cli.bind {
            Some(bind) => bind,
            None => "127.0.0.1:20000".to_string(),
        };
        let (address, port) = bind.split_once(':').context("bind is incorrect")?;

        if let (Some(tlscert), Some(tlskey)) = (cli.tlscert, cli.tlskey) {
            endpoints.push(Endpoint {
                listener_class: ListenerClass::Tls {
                    certificate: tlscert,
                    private_key: tlskey,
                    chain: None,
                },
                interface: address.to_string(),
                port: port.parse()?,
                device: None,
            })
        } else {
            endpoints.push(Endpoint {
                listener_class: ListenerClass::Tcp,
                interface: address.to_string(),
                port: port.parse()?,
                device: None,
            })
        }
    }

    let (notify_shutdown_tx, notify_shutdown) = tokio::sync::watch::channel(false);
    let mut listeners = vec![];
    for endpoint in endpoints {
        listeners.push(
            Listener::new(endpoint, db.clone(), users.clone(), notify_shutdown.clone()).await?,
        );
    }

    let metrics_handle = match cli.exporter {
        Some(exporter) => {
            // Spawn a server to serve the OpenMetrics endpoint.
            let metrics_addr = exporter.parse()?;
            let metrics_handle =
                start_metrics_server(metrics_addr, registry, notify_shutdown.clone()).await;

            Some(metrics_handle)
        }
        None => None,
    };

    drop(notify_shutdown);

    let handle = tokio::spawn(server::run(listeners));

    signal::ctrl_c().await?;
    notify_shutdown_tx.send(true)?;
    drop(notify_shutdown_tx);

    handle.await?;

    if let Some(metrics_handle) = metrics_handle {
        metrics_handle.await??;
    }

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

    #[arg(short, long, value_parser = parse_endpoint)]
    endpoint: Option<Vec<Endpoint>>,

    #[clap(long, default_value = "hpfeeds-broker")]
    name: String,

    #[clap(long)]
    exporter: Option<String>,

    #[clap(long)]
    bind: Option<String>,

    #[clap(long)]
    tlscert: Option<String>,

    #[clap(long)]
    tlskey: Option<String>,

    // No-op for compatibility with legacy broker cli
    #[clap(long, default_value_t = false)]
    debug: bool,
}

fn set_up_logging() -> Result<()> {
    // See https://docs.rs/tracing for more info
    tracing_subscriber::fmt::init();
    Ok(())
}
