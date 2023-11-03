use crate::frame::{self, Auth, Error, Frame, Info, Publish, Subscribe, Unsubscribe};

use anyhow::{bail, Result};
use bytes::{Buf, BytesMut};
use std::io::Cursor;
use tokio::io::{AsyncReadExt, AsyncWriteExt, BufWriter};
use tokio::net::TcpStream;
use tokio_rustls::server::TlsStream;

pub enum Writer {
    Tcp(BufWriter<TcpStream>),
    Tls(BufWriter<TlsStream<TcpStream>>),
}

impl Writer {
    pub fn new_with_tcp_stream(stream: TcpStream) -> Self {
        Self::Tcp(BufWriter::new(stream))
    }

    pub fn new_with_tls_stream(stream: TlsStream<TcpStream>) -> Self {
        Self::Tls(BufWriter::new(stream))
    }

    async fn write_u8(&mut self, n: u8) -> Result<()> {
        match self {
            Writer::Tcp(stream) => stream.write_u8(n).await?,
            Writer::Tls(stream) => stream.write_u8(n).await?,
        };
        Ok(())
    }

    async fn write_u32(&mut self, n: u32) -> Result<()> {
        match self {
            Writer::Tcp(stream) => stream.write_u32(n).await?,
            Writer::Tls(stream) => stream.write_u32(n).await?,
        };
        Ok(())
    }

    async fn write_all<'a>(&mut self, src: &'a [u8]) -> Result<()> {
        match self {
            Writer::Tcp(stream) => stream.write_all(src).await?,
            Writer::Tls(stream) => stream.write_all(src).await?,
        };
        Ok(())
    }

    async fn flush<'a>(&mut self) -> Result<()> {
        match self {
            Writer::Tcp(stream) => stream.flush().await?,
            Writer::Tls(stream) => stream.flush().await?,
        };
        Ok(())
    }
}

/// Send and receive `Frame` values from a remote peer.
///
/// When implementing networking protocols, a message on that protocol is
/// often composed of several smaller messages known as frames. The purpose of
/// `Connection` is to read and write frames on the underlying `TcpStream`.
///
/// To read frames, the `Connection` uses an internal buffer, which is filled
/// up until there are enough bytes to create a full frame. Once this happens,
/// the `Connection` creates the frame and returns it to the caller.
///
/// When sending frames, the frame is first encoded into the write buffer.
/// The contents of the write buffer are then written to the socket.
pub struct Connection {
    // The `TcpStream`. It is decorated with a `BufWriter`, which provides write
    // level buffering. The `BufWriter` implementation provided by Tokio is
    // sufficient for our needs.
    stream: Writer,

    // The buffer for reading frames.
    buffer: BytesMut,
}

impl Connection {
    /// Create a new `Connection`, backed by `socket`. Read and write buffers
    /// are initialized.
    pub fn new(writer: Writer) -> Connection {
        Connection {
            stream: writer,
            // Default to a 4KB read buffer. For the use case of mini redis,
            // this is fine. However, real applications will want to tune this
            // value to their specific use case. There is a high likelihood that
            // a larger read buffer will work better.
            buffer: BytesMut::with_capacity(4 * 1024),
        }
    }

    /// Read a single `Frame` value from the underlying stream.
    ///
    /// The function waits until it has retrieved enough data to parse a frame.
    /// Any data remaining in the read buffer after the frame has been parsed is
    /// kept there for the next call to `read_frame`.
    ///
    /// # Returns
    ///
    /// On success, the received frame is returned. If the `TcpStream`
    /// is closed in a way that doesn't break a frame in half, it returns
    /// `None`. Otherwise, an error is returned.
    pub async fn read_frame(&mut self) -> Result<Option<Frame>> {
        loop {
            // Attempt to parse a frame from the buffered data. If enough data
            // has been buffered, the frame is returned.
            if let Some(frame) = self.parse_frame()? {
                return Ok(Some(frame));
            }

            let bytes_read = match &mut self.stream {
                Writer::Tcp(stream) => stream.read_buf(&mut self.buffer).await?,
                Writer::Tls(stream) => stream.read_buf(&mut self.buffer).await?,
            };

            // There is not enough buffered data to read a frame. Attempt to
            // read more data from the socket.
            //
            // On success, the number of bytes is returned. `0` indicates "end
            // of stream".
            if 0 == bytes_read {
                // The remote closed the connection. For this to be a clean
                // shutdown, there should be no data in the read buffer. If
                // there is, this means that the peer closed the socket while
                // sending a frame.
                if self.buffer.is_empty() {
                    return Ok(None);
                } else {
                    bail!("connection reset by peer");
                }
            }
        }
    }

    /// Tries to parse a frame from the buffer. If the buffer contains enough
    /// data, the frame is returned and the data removed from the buffer. If not
    /// enough data has been buffered yet, `Ok(None)` is returned. If the
    /// buffered data does not represent a valid frame, `Err` is returned.
    fn parse_frame(&mut self) -> Result<Option<Frame>> {
        use frame::FrameError::Incomplete;

        // Cursor is used to track the "current" location in the
        // buffer. Cursor also implements `Buf` from the `bytes` crate
        // which provides a number of helpful utilities for working
        // with bytes.
        let mut buf = Cursor::new(&self.buffer[..]);

        // The first step is to check if enough data has been buffered to parse
        // a single frame. This step is usually much faster than doing a full
        // parse of the frame, and allows us to skip allocating data structures
        // to hold the frame data unless we know the full frame has been
        // received.
        match Frame::check(&mut buf) {
            Ok(len) => {
                // Reset the position to zero before passing the cursor to
                // `Frame::parse`.
                buf.set_position(0);

                // Parse the frame from the buffer. This allocates the necessary
                // structures to represent the frame and returns the frame
                // value.
                //
                // If the encoded frame representation is invalid, an error is
                // returned. This should terminate the **current** connection
                // but should not impact any other connected client.
                let frame = Frame::parse(&mut buf)?;

                // Discard the parsed data from the read buffer.
                //
                // When `advance` is called on the read buffer, all of the data
                // up to `len` is discarded. The details of how this works is
                // left to `BytesMut`. This is often done by moving an internal
                // cursor, but it may be done by reallocating and copying data.
                self.buffer.advance(len);

                // Return the parsed frame to the caller.
                Ok(Some(frame))
            }
            // There is not enough data present in the read buffer to parse a
            // single frame. We must wait for more data to be received from the
            // socket. Reading from the socket will be done in the statement
            // after this `match`.
            //
            // We do not want to return `Err` from here as this "error" is an
            // expected runtime condition.
            Err(Incomplete) => Ok(None),
            // An error was encountered while parsing the frame. The connection
            // is now in an invalid state. Returning `Err` from here will result
            // in the connection being closed.
            Err(e) => Err(e.into()),
        }
    }

    /// Write a single `Frame` value to the underlying stream.
    ///
    /// The `Frame` value is written to the socket using the various `write_*`
    /// functions provided by `AsyncWrite`. Calling these functions directly on
    /// a `TcpStream` is **not** advised, as this will result in a large number of
    /// syscalls. However, it is fine to call these functions on a *buffered*
    /// write stream. The data will be written to the buffer. Once the buffer is
    /// full, it is flushed to the underlying socket.
    pub async fn write_frame(&mut self, frame: &Frame) -> Result<usize> {
        // Arrays are encoded by encoding each entry. All other frame types are
        // considered literals. For now, hpfeeds-broker is not able to encode
        // recursive frame structures. See below for more details.
        let written = match frame {
            Frame::Error(Error { message }) => {
                let size = 5 + message.len();
                self.stream.write_u32(size as u32).await?;
                self.stream.write_u8(0).await?;
                self.stream.write_u8(message.len() as u8).await?;
                self.stream.write_all(message.as_bytes()).await?;

                size
            }
            Frame::Info(Info { broker_name, nonce }) => {
                let size = 6 + broker_name.len() + nonce.len();
                self.stream.write_u32(size as u32).await?;
                self.stream.write_u8(1).await?;
                self.stream.write_u8(broker_name.len() as u8).await?;
                self.stream.write_all(broker_name.as_bytes()).await?;
                self.stream.write_all(nonce).await?;

                size
            }
            Frame::Auth(Auth { ident, signature }) => {
                let size = 6 + ident.len() + signature.len();
                self.stream.write_u32(size as u32).await?;
                self.stream.write_u8(2).await?;
                self.stream.write_u8(ident.len() as u8).await?;
                self.stream.write_all(ident.as_bytes()).await?;
                self.stream.write_all(signature).await?;

                size
            }
            Frame::Publish(Publish {
                ident,
                channel,
                payload,
            }) => {
                let size = 7 + ident.len() + channel.len() + payload.len();
                self.stream.write_u32(size as u32).await?;
                self.stream.write_u8(3).await?;
                self.stream.write_u8(ident.len() as u8).await?;
                self.stream.write_all(ident.as_bytes()).await?;
                self.stream.write_u8(channel.len() as u8).await?;
                self.stream.write_all(channel.as_bytes()).await?;
                self.stream.write_all(payload).await?;

                size
            }
            Frame::Subscribe(Subscribe { ident, channel }) => {
                let size = 6 + ident.len() + channel.len();
                self.stream.write_u32(size as u32).await?;
                self.stream.write_u8(4).await?;
                self.stream.write_u8(ident.len() as u8).await?;
                self.stream.write_all(ident.as_bytes()).await?;
                self.stream.write_all(channel.as_bytes()).await?;

                size
            }
            Frame::Unsubscribe(Unsubscribe { ident, channel }) => {
                let size = 6 + ident.len() + channel.len();
                self.stream.write_u32(size as u32).await?;
                self.stream.write_u8(5).await?;
                self.stream.write_u8(ident.len() as u8).await?;
                self.stream.write_all(ident.as_bytes()).await?;
                self.stream.write_all(channel.as_bytes()).await?;

                size
            }
        };

        // Ensure the encoded frame is written to the socket. The calls above
        // are to the buffered stream and writes. Calling `flush` writes the
        // remaining contents of the buffer to the socket.
        self.stream.flush().await?;

        Ok(written)
    }
}
