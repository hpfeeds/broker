use crate::{Db, Parse};

use bytes::Bytes;

/// Posts a message to the given channel.
///
/// Send a message into a channel without any knowledge of individual consumers.
/// Consumers may subscribe to channels in order to receive the messages.
///
/// Channel names have no relation to the key-value namespace. Publishing on a
/// channel named "foo" has no relation to setting the "foo" key.
#[derive(Debug)]
pub struct Publish {
    /// Name of the channel on which the message should be published.
    channel: String,

    /// The message to publish.
    message: Bytes,
}

impl Publish {
    /// Parse a `Publish` instance from a received frame.
    ///
    /// The `Parse` argument provides a cursor-like API to read fields from the
    /// `Frame`. At this point, the entire frame has already been received from
    /// the socket.
    ///
    /// The `PUBLISH` string has already been consumed.
    ///
    /// # Returns
    ///
    /// On success, the `Publish` value is returned. If the frame is malformed,
    /// `Err` is returned.
    ///
    /// # Format
    ///
    /// Expects an array frame containing three entries.
    ///
    /// ```text
    /// PUBLISH channel message
    /// ```
    pub(crate) fn parse_frames(parse: &mut Parse) -> crate::Result<Publish> {
        // The `PUBLISH` string has already been consumed. Extract the `channel`
        // and `message` values from the frame.
        //
        // The `channel` must be a valid string.
        let channel = parse.next_string()?;

        // The `message` is arbitrary bytes.
        let message = parse.next_bytes()?;

        Ok(Publish { channel, message })
    }

    /// Apply the `Publish` command to the specified `Db` instance.
    ///
    /// The response is written to `dst`. This is called by the server in order
    /// to execute a received command.
    pub(crate) async fn apply(self, db: &Db) -> crate::Result<()> {
        // The shared state contains the `tokio::sync::broadcast::Sender` for
        // all active channels. Calling `db.publish` dispatches the message into
        // the appropriate channel.
        //
        // The number of subscribers currently listening on the channel is
        // returned. This does not mean that `num_subscriber` channels will
        // receive the message. Subscribers may drop before receiving the
        // message. Given this, `num_subscribers` should only be used as a
        // "hint".
        db.publish(&self.channel, self.message);

        Ok(())
    }
}
