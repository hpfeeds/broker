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

mod auth;
pub use auth::Users;

mod connection;
pub use connection::Connection;

pub mod frame;
pub use frame::Frame;

mod db;
use db::Db;
use db::DbDropGuard;

pub mod server;

mod shutdown;
use shutdown::Shutdown;

/// Default port that a redis server listens on.
///
/// Used if no port is specified.
pub const DEFAULT_PORT: u16 = 6379;

/// Error returned by most functions.
///
/// When writing a real application, one might want to consider a specialized
/// error handling crate or defining an error type as an `enum` of causes.
/// However, for our example, using a boxed `std::error::Error` is sufficient.
///
/// For performance reasons, boxing is avoided in any hot path. For example, in
/// `parse`, a custom error `enum` is defined. This is because the error is hit
/// and handled during normal execution when a partial frame is received on a
/// socket. `std::error::Error` is implemented for `parse::Error` which allows
/// it to be converted to `Box<dyn std::error::Error>`.
pub type Error = Box<dyn std::error::Error + Send + Sync>;

/// A specialized `Result` type for hpfeeds-broker operations.
///
/// This is defined as a convenience.
pub type Result<T> = std::result::Result<T, Error>;
