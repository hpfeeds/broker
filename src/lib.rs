//! A hpfeeds events broker.
//!
//!
//! # Layout
//!
//! The library is structured such that it can be used with guides. There are
//! modules that are public that probably would not be public in a "real" redis
//! client library.
//!
//! The major components are:
//!
//! * `server`: Redis server implementation. Includes a single `run` function
//!   that takes a `TcpListener` and starts accepting redis client connections.
//!
//! * `cmd`: implementations of the supported Redis commands.
//!
//! * `frame`: represents a single Redis protocol frame. A frame is used as an
//!   intermediate representation between a "command" and the byte
//!   representation.

/// The size in bytes of the write buffer
pub const TUNING_WRITE_BUFFER: usize = 16384;

/// The number of events we can wait to write to the network. After we have this
/// many held in the queue we start to drop old events (lag).
pub const TUNING_CHANNEL_BACKPRESSURE: usize = 8192;

mod endpoint;
pub use endpoint::{parse_endpoint, Endpoint, ListenerClass};

mod auth;
pub use auth::{sign, User, UserSet, Users};

mod connection;
pub use connection::Connection;

pub mod frame;
pub use frame::Frame;

mod db;
pub use db::Db;

pub mod server;

mod shutdown;
use shutdown::Shutdown;

mod prometheus;
pub use prometheus::{start_metrics_server, IdentChanLabels};

mod connection_limits;
pub use connection_limits::ConnectionLimits;

mod stream;
pub use stream::MultiStream;

mod certificates;
pub use certificates::Resolver;
