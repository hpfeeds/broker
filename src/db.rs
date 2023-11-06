use prometheus_client::collector::Collector;
use prometheus_client::metrics::gauge::ConstGauge;
use prometheus_client::metrics::MetricType;
use prometheus_client::registry::Registry;
use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use tokio::sync::broadcast;

use crate::frame::Publish;
use crate::prometheus::BrokerMetrics;
use crate::ConnectionLimits;

/// Server state shared across all connections.
///
/// `Db` contains a `HashMap` storing the key/value data and all
/// `broadcast::Sender` values for active pub/sub channels.
///
/// A `Db` instance is a handle to shared state. Cloning `Db` is shallow and
/// only incurs an atomic ref count increment.
///
/// When a `Db` value is created, a background task is spawned. This task is
/// used to expire values after the requested duration has elapsed. The task
/// runs until all instances of `Db` are dropped, at which point the task
/// terminates.
#[derive(Debug, Clone)]
pub struct Db {
    /// Handle to shared state. The background task will also have an
    /// `Arc<Shared>`.
    shared: Arc<Shared>,
    pub metrics: BrokerMetrics,
    pub limit_connections: ConnectionLimits,
}

#[derive(Debug)]
struct Shared {
    /// The shared state is guarded by a mutex. This is a `std::sync::Mutex` and
    /// not a Tokio mutex. This is because there are no asynchronous operations
    /// being performed while holding the mutex. Additionally, the critical
    /// sections are very small.
    ///
    /// A Tokio mutex is mostly intended to be used when locks need to be held
    /// across `.await` yield points. All other cases are **usually** best
    /// served by a std mutex. If the critical section does not include any
    /// async operations but is long (CPU intensive or performing blocking
    /// operations), then the entire operation, including waiting for the mutex,
    /// is considered a "blocking" operation and `tokio::task::spawn_blocking`
    /// should be used.
    state: Mutex<State>,
}

#[derive(Debug)]
struct State {
    /// The pub/sub key-space. Redis uses a **separate** key space for key-value
    /// and pub/sub. `hpfeeds-broker` handles this by using a separate `HashMap`.
    pub_sub: HashMap<String, broadcast::Sender<Publish>>,
}

impl Db {
    /// Create a new, empty, `Db` instance. Allocates shared state and spawns a
    /// background task to manage key expiration.
    pub fn new(registry: &mut Registry) -> Db {
        let shared = Arc::new(Shared {
            state: Mutex::new(State {
                pub_sub: HashMap::new(),
            }),
        });

        let db = Db {
            shared,
            metrics: BrokerMetrics::new(registry),
            limit_connections: ConnectionLimits::default(),
        };

        registry.register_collector(Box::new(db.limit_connections.clone()));
        registry.register_collector(Box::new(db.clone()));

        db
    }

    /// Returns a `Receiver` for the requested channel.
    ///
    /// The returned `Receiver` is used to receive values broadcast by `PUBLISH`
    /// commands.
    pub(crate) fn subscribe(&self, key: String) -> broadcast::Receiver<Publish> {
        use std::collections::hash_map::Entry;

        // Acquire the mutex
        let mut state = self.shared.state.lock().unwrap();

        // If there is no entry for the requested channel, then create a new
        // broadcast channel and associate it with the key. If one already
        // exists, return an associated receiver.
        match state.pub_sub.entry(key) {
            Entry::Occupied(e) => e.get().subscribe(),
            Entry::Vacant(e) => {
                // No broadcast channel exists yet, so create one.
                //
                // The channel is created with a capacity of `1024` messages. A
                // message is stored in the channel until **all** subscribers
                // have seen it. This means that a slow subscriber could result
                // in messages being held indefinitely.
                //
                // When the channel's capacity fills up, publishing will result
                // in old messages being dropped. This prevents slow consumers
                // from blocking the entire system.
                let (tx, rx) = broadcast::channel(16384);
                e.insert(tx);
                rx
            }
        }
    }

    /// Publish a message to the channel. Returns the number of subscribers
    /// listening on the channel.
    pub(crate) fn publish(&self, key: &str, value: Publish) -> usize {
        let state = self.shared.state.lock().unwrap();

        state
            .pub_sub
            .get(key)
            // On a successful message send on the broadcast channel, the number
            // of subscribers is returned. An error indicates there are no
            // receivers, in which case, `0` should be returned.
            .map(|tx| tx.send(value).unwrap_or(0))
            // If there is no entry for the channel key, then there are no
            // subscribers. In this case, return `0`.
            .unwrap_or(0)
    }
}

use prometheus_client::encoding::EncodeMetric;

impl Collector for Db {
    fn encode(
        &self,
        mut encoder: prometheus_client::encoding::DescriptorEncoder,
    ) -> Result<(), std::fmt::Error> {
        let mut family = encoder.encode_descriptor(
            "subscriptions",
            "Total number of active subscriptions",
            None,
            MetricType::Gauge,
        )?;

        let state = self.shared.state.lock().unwrap();
        for (topic, tx) in state.pub_sub.iter() {
            let labels = [("chan", topic.as_str())];
            let encoder = family.encode_family(&labels)?;
            ConstGauge::new(tx.receiver_count() as i64).encode(encoder)?;
        }

        Ok(())
    }
}
