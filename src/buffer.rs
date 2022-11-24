use crate::Result;

use bytes::Bytes;
use tokio::sync::oneshot;

// Enum used to message pass the requested command from the `Buffer` handle
#[derive(Debug)]
enum Command {
}

// Message type sent over the channel to the connection task.
//
// `Command` is the command to forward to the connection.
//
// `oneshot::Sender` is a channel type that sends a **single** value. It is used
// here to send the response received from the connection back to the original
// requester.
type Message = (Command, oneshot::Sender<Result<Option<Bytes>>>);
