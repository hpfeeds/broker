//! HPFeeds broker server implementation
//!
//! Provides an async `run` function that listens for inbound connections,
//! spawning a task per connection.

use crate::endpoint::ListenerClass;
use crate::frame::{Auth, Error, Info, Publish, Subscribe, Unsubscribe};
use crate::prometheus::{IdentChanErrorLabels, IdentLabels};
use crate::stream::MultiStream;
use crate::{auth, sign, Connection, Db, Endpoint, Frame, IdentChanLabels, Resolver, Shutdown};

use anyhow::{bail, Context, Result};
use constant_time_eq::constant_time_eq;
use rand::RngCore;

use socket2::{SockRef, TcpKeepalive};

use std::net::SocketAddr;
use std::sync::Arc;
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::{broadcast, mpsc, watch};
use tokio::task::JoinSet;
use tokio::time::{self, Duration};
use tokio_rustls::TlsAcceptor;
use tokio_stream::StreamExt;
use tokio_stream::StreamMap;
use tracing::{error, info, instrument};

/// Server listener state. Created in the `run` call. It includes a `run` method
/// which performs the TCP listening and initialization of per-connection state.
pub struct Listener {
    /// Shared database handle.
    ///
    /// Contains the key / value store as well as the broadcast channels for
    /// pub/sub.
    ///
    /// This holds a wrapper around an `Arc`. The internal `Db` can be
    /// retrieved and passed into the per connection state (`Handler`).
    db: Db,

    /// TCP listener supplied by the `run` caller.
    listener: TcpListener,

    acceptor: Option<TlsAcceptor>,

    /// Broadcasts a shutdown signal to all active connections.
    ///
    /// The initial `shutdown` trigger is provided by the `run` caller. The
    /// server is responsible for gracefully shutting down active connections.
    /// When a connection task is spawned, it is passed a broadcast receiver
    /// handle. When a graceful shutdown is initiated, a `()` value is sent via
    /// the broadcast::Sender. Each active connection receives it, reaches a
    /// safe terminal state, and completes the task.
    notify_shutdown: watch::Receiver<bool>,

    /// Used as part of the graceful shutdown process to wait for client
    /// connections to complete processing.
    ///
    /// Tokio channels are closed once all `Sender` handles go out of scope.
    /// When a channel is closed, the receiver receives `None`. This is
    /// leveraged to detect all connection handlers completing. When a
    /// connection handler is initialized, it is assigned a clone of
    /// `shutdown_complete_tx`. When the listener shuts down, it drops the
    /// sender held by this `shutdown_complete_tx` field. Once all handler tasks
    /// complete, all clones of the `Sender` are also dropped. This results in
    /// `shutdown_complete_rx.recv()` completing with `None`. At this point, it
    /// is safe to exit the server process.
    shutdown_complete_rx: mpsc::Receiver<()>,
    shutdown_complete_tx: mpsc::Sender<()>,

    users: Arc<auth::Users>,
}

/// Per-connection handler. Reads requests from `connection` and applies the
/// commands to `db`.
struct Handler {
    /// Shared database handle.
    ///
    /// When a command is received from `connection`, it is applied with `db`.
    /// The implementation of the command is in the `cmd` module. Each command
    /// will need to interact with `db` in order to complete the work.
    db: Db,

    users: Arc<auth::Users>,

    ident: Option<String>,
    user: Option<auth::User>,
    acceptor: Option<TlsAcceptor>,

    /// Listen for shutdown notifications.
    ///
    /// A wrapper around the `broadcast::Receiver` paired with the sender in
    /// `Listener`. The connection handler processes requests from the
    /// connection until the peer disconnects **or** a shutdown notification is
    /// received from `shutdown`. In the latter case, any in-flight work being
    /// processed for the peer is continued until it reaches a safe state, at
    /// which point the connection is terminated.
    shutdown: Shutdown,

    /// Not used directly. Instead, when `Handler` is dropped...?
    _shutdown_complete: mpsc::Sender<()>,
}

/// Run the broker.
///
/// Accepts connections from the supplied listener. For each inbound connection,
/// a task is spawned to handle that connection. The server runs until the
/// `shutdown` future completes, at which point the server shuts down
/// gracefully.
///
/// `tokio::signal::ctrl_c()` can be used as the `shutdown` argument. This will
/// listen for a SIGINT signal.
pub async fn run(listeners: Vec<Listener>) {
    let mut tasks = JoinSet::new();

    for server in listeners {
        tasks.spawn(async move { server.run().await });
    }

    while let Some(task) = tasks.join_next().await {
        match task {
            Ok(result) => match result {
                Ok(listener) => {
                    // Extract the `shutdown_complete` receiver and transmitter
                    // explicitly drop `shutdown_transmitter`. This is important, as the
                    // `.await` below would otherwise never complete.
                    let Listener {
                        mut shutdown_complete_rx,
                        shutdown_complete_tx,
                        ..
                    } = listener;

                    // Drop final `Sender` so the `Receiver` below can complete
                    drop(shutdown_complete_tx);

                    // Wait for all active connections to finish processing. As the `Sender`
                    // handle held by the listener has been dropped above, the only remaining
                    // `Sender` instances are held by connection handler tasks. When those drop,
                    // the `mpsc` channel will close and `recv()` will return `None`.
                    let _ = shutdown_complete_rx.recv().await;
                }
                Err(e) => {
                    error!("Error occurred during shutdown: {:?}", e);
                }
            },
            Err(e) => {
                error!("Error occurred during shutdown: {:?}", e);
            }
        }
    }
}

impl Listener {
    pub async fn new(
        endpoint: Endpoint,
        db: Db,
        users: Arc<crate::Users>,
        notify_shutdown: watch::Receiver<bool>,
    ) -> Result<Self> {
        let acceptor = match endpoint.listener_class {
            ListenerClass::Tls {
                private_key,
                certificate,
                chain,
            } => {
                let cert_resolver = Resolver::new(private_key, certificate, chain)?;
                let config = rustls::ServerConfig::builder()
                    .with_no_client_auth()
                    .with_cert_resolver(Arc::new(cert_resolver));

                let acceptor = TlsAcceptor::from(Arc::new(config));

                Some(acceptor)
            }
            _ => None,
        };

        // Bind a TCP listener
        let listener =
            TcpListener::bind(&format!("{}:{}", endpoint.interface, endpoint.port)).await?;

        // When the provided `shutdown` future completes, we must send a shutdown
        // message to all active connections. We use a broadcast channel for this
        // purpose. The call below ignores the receiver of the broadcast pair, and when
        // a receiver is needed, the subscribe() method on the sender is used to create
        // one.
        let (shutdown_complete_tx, shutdown_complete_rx) = mpsc::channel(1);

        // Initialize the listener state
        Ok(Listener {
            users: users.clone(),
            listener,
            acceptor,
            db,
            notify_shutdown: notify_shutdown.clone(),
            shutdown_complete_tx,
            shutdown_complete_rx,
        })
    }

    pub fn local_addr(&self) -> SocketAddr {
        self.listener.local_addr().unwrap()
    }

    /// Run the server
    ///
    /// Listen for inbound connections. For each inbound connection, spawn a
    /// task to process that connection.
    ///
    /// # Errors
    ///
    /// Returns `Err` if accepting returns an error. This can happen for a
    /// number reasons that resolve over time. For example, if the underlying
    /// operating system has reached an internal limit for max number of
    /// sockets, accept will fail.
    ///
    /// The process is not able to detect when a transient error resolves
    /// itself. One strategy for handling this is to implement a back off
    /// strategy, which is what we do here.
    async fn run(mut self) -> Result<Self> {
        info!("accepting inbound connections");

        loop {
            // Wait for a permit to become available
            let permit = self
                .db
                .limit_connections
                .limit_connections
                .clone()
                .acquire_owned()
                .await?;

            // Accept a new socket. This will attempt to perform error handling.
            // The `accept` method internally attempts to recover errors, so an
            // error here is non-recoverable.
            let mut notify_shutdown = self.notify_shutdown.clone();
            let socket = tokio::select! {
                res = self.accept() => res?,
                _ = notify_shutdown.changed() => {
                    // The shutdown signal has been received.
                    return Ok(self)
                }
            };

            // Create the necessary per-connection handler state.
            let mut handler = Handler {
                // Get a handle to the shared database.
                db: self.db.clone(),

                users: self.users.clone(),
                user: None,
                ident: None,

                // Initialize the connection state. This allocates read/write
                // buffers to perform redis protocol frame parsing.
                acceptor: self.acceptor.clone(),

                // Receive shutdown notifications.
                shutdown: Shutdown::new(self.notify_shutdown.clone()),

                // Notifies the receiver half once all clones are
                // dropped.
                _shutdown_complete: self.shutdown_complete_tx.clone(),
            };

            let connection_lost = self.db.metrics.connection_lost.clone();

            // Spawn a new task to process the connections. Tokio tasks are like
            // asynchronous green threads and are executed concurrently.
            tokio::spawn(async move {
                // Process the connection. If an error is encountered, log it.
                if let Err(err) = handler.run(socket).await {
                    error!(cause = ?err, "connection error");
                }

                connection_lost.inc();

                // Move the permit into the task and drop it after completion.
                // This returns the permit back to the semaphore.
                drop(permit);
            });
        }
    }

    /// Accept an inbound connection.
    async fn accept_once(&mut self) -> Result<TcpStream> {
        let (socket, _addr) = match self.listener.accept().await {
            Ok(res) => res,
            Err(e) => {
                self.db
                    .metrics
                    .connection_error
                    .get_or_create(&IdentChanErrorLabels {
                        ident: None,
                        chan: None,
                        error: crate::prometheus::ErrorLabel::SocketAcceptFailure,
                    })
                    .inc();
                return Err(e.into());
            }
        };

        self.db.metrics.connection_made.inc();

        Ok(socket)
    }

    /// Accept an inbound connection.
    ///
    /// Errors are handled by backing off and retrying. An exponential backoff
    /// strategy is used. After the first failure, the task waits for 1 second.
    /// After the second failure, the task waits for 2 seconds. Each subsequent
    /// failure doubles the wait time. If accepting fails on the 6th try after
    /// waiting for 64 seconds, then this function returns with an error.
    async fn accept(&mut self) -> Result<TcpStream> {
        let mut backoff = 1;

        // Try to accept a few times
        loop {
            match self.accept_once().await {
                Ok(connection) => {
                    return Ok(connection);
                }
                Err(err) => {
                    if backoff > 64 {
                        // Accept has failed too many times. Return the error.
                        return Err(err);
                    }
                }
            };

            // Pause execution until the back off period elapses.
            time::sleep(Duration::from_secs(backoff)).await;

            // Double the back off
            backoff *= 2;
        }
    }
}

impl Handler {
    /// Process a single connection.
    ///
    /// Request frames are read from the socket and processed. Responses are
    /// written back to the socket.
    ///
    /// Currently, pipelining is not implemented. Pipelining is the ability to
    /// process more than one request concurrently per connection without
    /// interleaving frames. See for more details:
    /// https://redis.io/topics/pipelining
    ///
    /// When the shutdown signal is received, the connection is processed until
    /// it reaches a safe state, at which point it is terminated.
    #[instrument(skip(self))]
    async fn run(&mut self, socket: TcpStream) -> Result<()> {
        let sock = SockRef::from(&socket);
        let ka = TcpKeepalive::new()
            .with_time(std::time::Duration::from_secs(10))
            .with_interval(std::time::Duration::from_secs(5))
            .with_retries(3);

        if let Err(e) = sock.set_tcp_keepalive(&ka) {
            self.db
                .metrics
                .connection_error
                .get_or_create(&IdentChanErrorLabels {
                    ident: None,
                    chan: None,
                    error: crate::prometheus::ErrorLabel::SocketConfigurationFailure,
                })
                .inc();
            return Err(e.into());
        }

        let mut connection = Connection::new(match &self.acceptor {
            Some(acceptor) => match acceptor.accept(socket).await {
                Ok(socket) => MultiStream::Tls(socket),
                Err(e) => {
                    self.db
                        .metrics
                        .connection_error
                        .get_or_create(&IdentChanErrorLabels {
                            ident: None,
                            chan: None,
                            error: crate::prometheus::ErrorLabel::TlsFailure,
                        })
                        .inc();
                    return Err(e.into());
                }
            },
            None => MultiStream::Tcp(socket),
        });

        let mut data = [0u8; 4];
        rand::thread_rng().fill_bytes(&mut data);

        connection
            .write_frame(&Frame::Info(Info {
                broker_name: "hpfeeds-broker".into(),
                nonce: data,
            }))
            .await?;

        // An individual client may subscribe to multiple channels and may
        // dynamically add and remove channels from its subscription set. To
        // handle this, a `StreamMap` is used to track active subscriptions. The
        // `StreamMap` merges messages from individual broadcast channels as
        // they are received.
        let mut subscriptions = StreamMap::new();

        // As long as the shutdown signal has not been received, try to read a
        // new request frame.
        while !self.shutdown.is_shutdown() {
            // While reading a request frame, also listen for the shutdown
            // signal.
            let maybe_frame = tokio::select! {
                res = connection.read_frame() => res?,
                Some((_, Publish {ident, channel, payload})) = subscriptions.next() => {
                    let written = connection.write_frame(&Frame::Publish(Publish { ident: ident.clone(), channel: channel.clone(), payload })).await?;
                    let labels = IdentChanLabels { ident: self.ident.clone().context("Received pub before auth")?, chan: channel };
                    self.db.metrics.publish_sent.get_or_create(&labels).inc();
                    self.db.metrics.publish_sent_bytes.get_or_create(&labels).inc_by(written as u64);
                    continue;
                },
                _ = self.shutdown.recv() => {
                    // If a shutdown signal is received, return from `run`.
                    // This will result in the task terminating.
                    return Ok(());
                }
            };

            // If `None` is returned from `read_frame()` then the peer closed
            // the socket. There is no further work to do and the task can be
            // terminated.
            let frame = match maybe_frame {
                Some(frame) => frame,
                None => return Ok(()),
            };

            match frame {
                Frame::Auth(Auth { ident, signature }) => {
                    if self.user.is_none() {
                        if let Some(user) = self.users.get(&ident) {
                            let result = sign(data, &user.secret);

                            if !constant_time_eq(&result, &signature[..]) {
                                connection
                                    .write_frame(&Frame::Error(Error {
                                        message: "Authentication failed".into(),
                                    }))
                                    .await?;

                                self.db
                                    .metrics
                                    .connection_error
                                    .get_or_create(&IdentChanErrorLabels {
                                        ident: Some(ident),
                                        chan: None,
                                        error: crate::prometheus::ErrorLabel::SignatureInvalid,
                                    })
                                    .inc();

                                return Ok(());
                            }

                            self.user = Some(user);
                            self.ident = Some(ident.clone());

                            self.db
                                .metrics
                                .connection_ready
                                .get_or_create(&IdentLabels { ident })
                                .inc();
                            continue;
                        }
                    }

                    self.db
                        .metrics
                        .connection_error
                        .get_or_create(&IdentChanErrorLabels {
                            ident: Some(ident),
                            chan: None,
                            error: crate::prometheus::ErrorLabel::IdentInvalid,
                        })
                        .inc();

                    connection
                        .write_frame(&Frame::Error(Error {
                            message: "Authentication failed".into(),
                        }))
                        .await?;
                    return Ok(());
                }
                Frame::Publish(Publish {
                    ident,
                    channel,
                    payload,
                }) => {
                    if let Some(user) = &self.user {
                        if user.pubchans.contains(&channel) {
                            let plen = payload.len() as f64;
                            let size = (7 + ident.len() + channel.len() + payload.len()) as u64;

                            self.db.publish(
                                &channel,
                                Publish {
                                    ident: ident.clone(),
                                    channel: channel.clone(),
                                    payload,
                                },
                            );

                            let labels = IdentChanLabels {
                                ident,
                                chan: channel,
                            };
                            self.db
                                .metrics
                                .publish_size
                                .get_or_create(&labels)
                                .observe(plen);
                            self.db
                                .metrics
                                .receive_publish_count
                                .get_or_create(&labels)
                                .inc();
                            self.db
                                .metrics
                                .receive_publish_size
                                .get_or_create(&labels)
                                .inc_by(size);

                            continue;
                        }
                    }

                    self.db
                        .metrics
                        .connection_error
                        .get_or_create(&IdentChanErrorLabels {
                            ident: Some(ident),
                            chan: None,
                            error: crate::prometheus::ErrorLabel::PublishNotAuthorized,
                        })
                        .inc();

                    connection
                        .write_frame(&Frame::Error(Error {
                            message: "Publish not authorized".into(),
                        }))
                        .await?;
                    return Ok(());
                }
                Frame::Subscribe(Subscribe { ident, channel }) => {
                    if let Some(user) = &self.user {
                        if user.subchans.contains(&channel) {
                            let mut rx = self.db.subscribe(channel.clone());

                            let publish_lag = self.db.metrics.publish_lag.clone();
                            let chan = channel.clone();

                            // Subscribe to the channel.
                            let rx = Box::pin(async_stream::stream! {
                                loop {
                                    match rx.recv().await {
                                        Ok(msg) => yield msg,
                                        // If we lagged in consuming messages, just resume.
                                        Err(broadcast::error::RecvError::Lagged(amt)) => {
                                            publish_lag.get_or_create(&IdentChanLabels { ident: ident.clone(), chan: chan.clone() }).inc_by(amt);
                                        }
                                        Err(_) => break,
                                    }
                                }
                            });

                            // Track subscription in this client's subscription set.
                            subscriptions.insert(channel.clone(), rx);

                            continue;
                        }
                    }

                    self.db
                        .metrics
                        .connection_error
                        .get_or_create(&IdentChanErrorLabels {
                            ident: Some(ident),
                            chan: None,
                            error: crate::prometheus::ErrorLabel::SubscribeNotAuthorized,
                        })
                        .inc();

                    connection
                        .write_frame(&Frame::Error(Error {
                            message: "Subscribe not authorized".into(),
                        }))
                        .await?;
                    return Ok(());
                }
                Frame::Unsubscribe(Unsubscribe { ident: _, channel }) => {
                    subscriptions.remove(&channel);
                }
                _ => {
                    bail!("protocol err; unexpected action");
                }
            };
        }

        Ok(())
    }
}
