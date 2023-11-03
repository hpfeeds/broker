use std::sync::Arc;

use prometheus_client::collector::Collector;
use prometheus_client::encoding::EncodeMetric;
use prometheus_client::metrics::gauge::ConstGauge;
use prometheus_client::metrics::MetricType;
use tokio::sync::Semaphore;

/// Maximum number of concurrent connections the redis server will accept.
///
/// When this limit is reached, the server will stop accepting connections until
/// an active connection terminates.
///
const MAX_CONNECTIONS: usize = 4000;

#[derive(Debug, Clone)]
pub struct ConnectionLimits {
    pub limit_connections: Arc<Semaphore>,
}

impl Default for ConnectionLimits {
    fn default() -> Self {
        Self {
            limit_connections: Semaphore::new(MAX_CONNECTIONS).into(),
        }
    }
}

impl Collector for ConnectionLimits {
    fn encode(
        &self,
        mut encoder: prometheus_client::encoding::DescriptorEncoder,
    ) -> Result<(), std::fmt::Error> {
        let available_permits = self.limit_connections.available_permits() as i64;
        let max_connections = MAX_CONNECTIONS as i64;

        let gauge = ConstGauge::new(max_connections - available_permits);
        let metric_encoder = encoder.encode_descriptor(
            "client_connections_used",
            "The number of clients current connected",
            None,
            MetricType::Gauge,
        )?;
        gauge.encode(metric_encoder)?;

        let gauge = ConstGauge::new(available_permits);
        let metric_encoder = encoder.encode_descriptor(
            "client_connections_available",
            "The number of clients that can connect before the server is full",
            None,
            MetricType::Gauge,
        )?;
        gauge.encode(metric_encoder)?;

        Ok(())
    }
}
