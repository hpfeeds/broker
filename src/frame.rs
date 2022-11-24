//! Provides a type representing a Redis protocol frame as well as utilities for
//! parsing frames from a byte array.

use bytes::{Buf, Bytes};
use std::convert::TryInto;
use std::fmt;
use std::io::Cursor;
use std::num::TryFromIntError;
use std::string::FromUtf8Error;

/// A frame in the Redis protocol.
#[derive(Clone, Debug)]
pub enum Frame {
    Error(String),
    Info {
        broker_name: String,
        nonce: Bytes,
    },
    Auth {
        ident: String,
        signature: Bytes,
    },
    Publish {
        ident: String,
        channel: String,
        payload: Bytes,
    },
    Subscribe {
        ident: String,
        channel: String,
    },
    Unsubscribe {
        ident: String,
        channel: String,
    },
    Bulk(Bytes),
    Array(Vec<Frame>),
}

#[derive(Debug)]
pub enum Error {
    /// Not enough data is available to parse a message
    Incomplete,

    /// Invalid message encoding
    Other(crate::Error),
}

impl Frame {
    /// Returns an empty array
    pub(crate) fn array() -> Frame {
        Frame::Array(vec![])
    }

    /// Push a "bulk" frame into the array. `self` must be an Array frame.
    ///
    /// # Panics
    ///
    /// panics if `self` is not an array
    pub(crate) fn push_bulk(&mut self, bytes: Bytes) {
        match self {
            Frame::Array(vec) => {
                vec.push(Frame::Bulk(bytes));
            }
            _ => panic!("not an array frame"),
        }
    }

    /// Checks if an entire message can be decoded from `src`
    pub fn check(src: &mut Cursor<&[u8]>) -> Result<(), Error> {
        let size = peek_u32(src)?;

        if src.remaining() < size as usize {
            return Err(Error::Incomplete);
        }

        Ok(())
    }

    /// The message has already been validated with `check`.
    pub fn parse(src: &mut Cursor<&[u8]>) -> Result<Frame, Error> {
        let pos = src.position();
        let size = src.get_u32();

        match src.get_u8() {
            // OP_ERROR
            0 => {
                // Rest of this message is a utf-8 string
                let error = String::from_utf8(get_remaining(src, pos, size)?.to_vec())?;

                Ok(Frame::Error(error))
            }
            // OP_INFO
            1 => {
                let broker_name = String::from_utf8(get_sized_string(src)?.to_vec())?;
                let nonce = get_remaining(src, pos, size)?.to_vec();

                Ok(Frame::Info {
                    broker_name,
                    nonce: nonce.into(),
                })
            }
            // OP_AUTH
            2 => {
                let ident = String::from_utf8(get_sized_string(src)?.to_vec())?;
                let signature = get_remaining(src, pos, size)?.to_vec();

                Ok(Frame::Auth {
                    ident,
                    signature: signature.into(),
                })
            }
            // OP_PUBLISH
            3 => {
                let ident = String::from_utf8(get_sized_string(src)?.to_vec())?;
                let channel = String::from_utf8(get_sized_string(src)?.to_vec())?;
                let payload = get_remaining(src, pos, size)?.to_vec();

                Ok(Frame::Publish {
                    ident,
                    channel,
                    payload: payload.into(),
                })
            }
            // OP_SUBSCRIBE
            4 => {
                let ident = String::from_utf8(get_sized_string(src)?.to_vec())?;
                let channel = String::from_utf8(get_remaining(src, pos, size)?.to_vec())?;

                Ok(Frame::Subscribe { ident, channel })
            }
            // OP_UNSUBSCRIBE
            5 => {
                let ident = String::from_utf8(get_sized_string(src)?.to_vec())?;
                let channel = String::from_utf8(get_remaining(src, pos, size)?.to_vec())?;

                Ok(Frame::Unsubscribe { ident, channel })
            }
            _ => Err("protocol error; invalid opcode".into()),
        }
    }
}

impl PartialEq<&str> for Frame {
    fn eq(&self, other: &&str) -> bool {
        match self {
            Frame::Bulk(s) => s.eq(other),
            _ => false,
        }
    }
}

impl fmt::Display for Frame {
    fn fmt(&self, fmt: &mut fmt::Formatter) -> fmt::Result {
        write!(fmt, "{:?}", self)?;
        Ok(())
    }
}

fn peek_u32(src: &mut Cursor<&[u8]>) -> Result<u32, Error> {
    if !src.has_remaining() {
        return Err(Error::Incomplete);
    }

    if src.remaining() < 4 {
        return Err(Error::Incomplete);
    };

    Ok(u32::from_be_bytes(src.chunk()[..4].try_into().unwrap()))
}

fn get_sized_string<'a>(src: &mut Cursor<&'a [u8]>) -> Result<&'a [u8], Error> {
    let size = src.get_u8() as usize;
    let start = src.position() as usize;
    let end = start + size;

    src.advance(size);

    Ok(&src.get_ref()[start..end])
}

fn get_remaining<'a>(src: &mut Cursor<&'a [u8]>, pos: u64, size: u32) -> Result<&'a [u8], Error> {
    let cur_pos = src.position() as usize;
    let consumed = (src.position() - pos) as usize;
    let left = (size as usize) - consumed;

    src.advance(left);

    Ok(&src.get_ref()[cur_pos..cur_pos + left])
}

impl From<String> for Error {
    fn from(src: String) -> Error {
        Error::Other(src.into())
    }
}

impl From<&str> for Error {
    fn from(src: &str) -> Error {
        src.to_string().into()
    }
}

impl From<FromUtf8Error> for Error {
    fn from(_src: FromUtf8Error) -> Error {
        "protocol error; invalid frame format".into()
    }
}

impl From<TryFromIntError> for Error {
    fn from(_src: TryFromIntError) -> Error {
        "protocol error; invalid frame format".into()
    }
}

impl std::error::Error for Error {}

impl fmt::Display for Error {
    fn fmt(&self, fmt: &mut fmt::Formatter) -> fmt::Result {
        match self {
            Error::Incomplete => "stream ended early".fmt(fmt),
            Error::Other(err) => err.fmt(fmt),
        }
    }
}
