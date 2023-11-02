use hyper::{
    service::{make_service_fn, service_fn},
    Body, Request, Response, Server,
};
use prometheus_client::{
    encoding::{text::encode, EncodeLabelSet},
    metrics::{counter::Counter, family::Family},
    registry::Registry,
};
use std::{future::Future, io, net::SocketAddr, pin::Pin, sync::Arc};
use tokio::signal::unix::{signal, SignalKind};

#[derive(Clone, Debug, Hash, PartialEq, Eq, EncodeLabelSet)]
pub struct IdentLabels {
    // Use your own enum types to represent label values.
    pub ident: String,
}

#[derive(Clone, Debug, Hash, PartialEq, Eq, EncodeLabelSet)]
pub struct IdentChanLabels {
    // Use your own enum types to represent label values.
    pub ident: String,
    // Or just a plain string.
    pub chan: String,
}

#[derive(Clone)]
pub struct BrokerMetrics {
    pub connection_made: Counter,
    pub connection_ready: Family<IdentLabels, Counter>,

    pub receive_publish_count: Family<IdentChanLabels, Counter>,

    pub publish_sent: Family<IdentChanLabels, Counter>,
    pub publish_lag: Family<IdentChanLabels, Counter>,
}

impl BrokerMetrics {
    pub fn new(registry: &mut Registry) -> Self {
        let receive_publish_count = Family::<IdentChanLabels, Counter>::default();
        registry.register(
            "publish_received",
            "Number of events received by broker for a channel",
            receive_publish_count.clone(),
        );

        let publish_sent = Family::<IdentChanLabels, Counter>::default();
        registry.register(
            "publish_sent",
            "Number of events received by broker for a channel",
            publish_sent.clone(),
        );

        let publish_lag = Family::<IdentChanLabels, Counter>::default();
        registry.register(
            "publish_lag",
            "Number of events dropped because of backpressure",
            publish_lag.clone(),
        );

        let connection_made = Counter::default();
        registry.register(
            "connection_made",
            "Number of connections established",
            connection_made.clone(),
        );

        let connection_ready = Family::<IdentLabels, Counter>::default();
        registry.register(
            "connection_ready",
            "Number of connections established + authenticated",
            connection_ready.clone(),
        );

        BrokerMetrics {
            connection_made,
            connection_ready,

            receive_publish_count,

            publish_lag,
            publish_sent,
        }
    }
}

/// Start a HTTP server to report metrics.
pub async fn start_metrics_server(metrics_addr: SocketAddr, registry: Registry) {
    let mut shutdown_stream = signal(SignalKind::terminate()).unwrap();

    eprintln!("Starting metrics server on {metrics_addr}");

    let registry = Arc::new(registry);
    Server::bind(&metrics_addr)
        .serve(make_service_fn(move |_conn| {
            let registry = registry.clone();
            async move {
                let handler = make_handler(registry);
                Ok::<_, io::Error>(service_fn(handler))
            }
        }))
        .with_graceful_shutdown(async move {
            shutdown_stream.recv().await;
        })
        .await
        .unwrap();
}

/// This function returns a HTTP handler (i.e. another function)
pub fn make_handler(
    registry: Arc<Registry>,
) -> impl Fn(Request<Body>) -> Pin<Box<dyn Future<Output = io::Result<Response<Body>>> + Send>> {
    // This closure accepts a request and responds with the OpenMetrics encoding of our metrics.
    move |_req: Request<Body>| {
        let reg = registry.clone();
        Box::pin(async move {
            let mut buf = String::new();
            encode(&mut buf, &reg.clone())
                .map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e))
                .map(|_| {
                    let body = Body::from(buf);
                    Response::builder()
                        .header(
                            hyper::header::CONTENT_TYPE,
                            "application/openmetrics-text; version=1.0.0; charset=utf-8",
                        )
                        .body(body)
                        .unwrap()
                })
        })
    }
}
