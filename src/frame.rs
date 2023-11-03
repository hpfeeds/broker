//! Provides a type representing a Redis protocol frame as well as utilities for
//! parsing frames from a byte array.

use anyhow::{bail, Result};
use bytes::{Buf, Bytes};
use std::convert::TryInto;
use std::fmt;
use std::io::Cursor;

#[derive(Clone, Debug)]
pub struct Error {
    pub message: String,
}

#[derive(Clone, Debug)]
pub struct Info {
    pub broker_name: String,
    pub nonce: [u8; 4],
}

#[derive(Clone, Debug)]
pub struct Auth {
    pub ident: String,
    pub signature: Bytes,
}

#[derive(Clone, Debug)]
pub struct Publish {
    pub ident: String,
    pub channel: String,
    pub payload: Bytes,
}

#[derive(Clone, Debug)]
pub struct Subscribe {
    pub ident: String,
    pub channel: String,
}

#[derive(Clone, Debug)]
pub struct Unsubscribe {
    pub ident: String,
    pub channel: String,
}

/// A frame in the Redis protocol.
#[derive(Clone, Debug)]
pub enum Frame {
    Error(Error),
    Info(Info),
    Auth(Auth),
    Publish(Publish),
    Subscribe(Subscribe),
    Unsubscribe(Unsubscribe),
}

#[derive(Debug)]
pub enum FrameError {
    /// Not enough data is available to parse a message
    Incomplete,
}

impl Frame {
    /// Checks if an entire message can be decoded from `src`
    pub fn check(src: &mut Cursor<&[u8]>) -> Result<usize, FrameError> {
        let size = peek_u32(src)?;

        if src.remaining() < size as usize {
            return Err(FrameError::Incomplete);
        }

        Ok(size as usize)
    }

    /// The message has already been validated with `check`.
    pub fn parse(src: &mut Cursor<&[u8]>) -> Result<Frame> {
        let pos = src.position();
        let size = src.get_u32();

        match src.get_u8() {
            // OP_ERROR
            0 => {
                // Rest of this message is a utf-8 string
                let message = String::from_utf8(get_remaining(src, pos, size)?.to_vec())?;

                Ok(Frame::Error(Error { message }))
            }
            // OP_INFO
            1 => {
                let broker_name = String::from_utf8(get_sized_string(src)?.to_vec())?;
                let nonce = get_remaining(src, pos, size)?;

                Ok(Frame::Info(Info {
                    broker_name,
                    nonce: nonce.try_into().unwrap(),
                }))
            }
            // OP_AUTH
            2 => {
                let ident = String::from_utf8(get_sized_string(src)?.to_vec())?;
                let signature = get_remaining(src, pos, size)?.to_vec();

                Ok(Frame::Auth(Auth {
                    ident,
                    signature: signature.into(),
                }))
            }
            // OP_PUBLISH
            3 => {
                let ident = String::from_utf8(get_sized_string(src)?.to_vec())?;
                let channel = String::from_utf8(get_sized_string(src)?.to_vec())?;
                let payload = get_remaining(src, pos, size)?.to_vec();

                Ok(Frame::Publish(Publish {
                    ident,
                    channel,
                    payload: payload.into(),
                }))
            }
            // OP_SUBSCRIBE
            4 => {
                let ident = String::from_utf8(get_sized_string(src)?.to_vec())?;
                let channel = String::from_utf8(get_remaining(src, pos, size)?.to_vec())?;

                Ok(Frame::Subscribe(Subscribe { ident, channel }))
            }
            // OP_UNSUBSCRIBE
            5 => {
                let ident = String::from_utf8(get_sized_string(src)?.to_vec())?;
                let channel = String::from_utf8(get_remaining(src, pos, size)?.to_vec())?;

                Ok(Frame::Unsubscribe(Unsubscribe { ident, channel }))
            }
            _ => bail!("protocol error; invalid opcode"),
        }
    }
}

impl fmt::Display for Frame {
    fn fmt(&self, fmt: &mut fmt::Formatter) -> fmt::Result {
        write!(fmt, "{:?}", self)?;
        Ok(())
    }
}

fn peek_u32(src: &mut Cursor<&[u8]>) -> Result<u32, FrameError> {
    if !src.has_remaining() {
        return Err(FrameError::Incomplete);
    }

    if src.remaining() < 4 {
        return Err(FrameError::Incomplete);
    };

    Ok(u32::from_be_bytes(src.chunk()[..4].try_into().unwrap()))
}

fn get_sized_string<'a>(src: &mut Cursor<&'a [u8]>) -> Result<&'a [u8], FrameError> {
    let size = src.get_u8() as usize;
    let start = src.position() as usize;
    let end = start + size;

    src.advance(size);

    Ok(&src.get_ref()[start..end])
}

fn get_remaining<'a>(
    src: &mut Cursor<&'a [u8]>,
    pos: u64,
    size: u32,
) -> Result<&'a [u8], FrameError> {
    let cur_pos = src.position() as usize;
    let consumed = (src.position() - pos) as usize;
    let left = (size as usize) - consumed;

    src.advance(left);

    Ok(&src.get_ref()[cur_pos..cur_pos + left])
}

impl std::error::Error for FrameError {}

impl fmt::Display for FrameError {
    fn fmt(&self, fmt: &mut fmt::Formatter) -> fmt::Result {
        match self {
            FrameError::Incomplete => "stream ended early".fmt(fmt),
        }
    }
}
