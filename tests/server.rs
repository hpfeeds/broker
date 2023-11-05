use anyhow::Result;
use bytes::Bytes;
use prometheus_client::registry::Registry;
use std::{
    collections::{BTreeMap, BTreeSet},
    net::SocketAddr,
    sync::Arc,
};

use tokio::{net::TcpStream, sync::watch::Sender};

use hpfeeds_broker::{
    frame::{Auth, Info, Publish, Subscribe, Unsubscribe},
    parse_endpoint,
    server::{self, Listener},
    sign, Connection, Db, Frame, MultiStream, User, UserSet, Users,
};

async fn start_server() -> (SocketAddr, Sender<bool>) {
    let mut registry = <Registry>::with_prefix("hpfeeds_broker");
    let db = Db::new(&mut registry);

    let mut subchans = BTreeSet::new();
    subchans.insert("bar".into());

    let mut pubchans = BTreeSet::new();
    pubchans.insert("bar".into());

    let mut records = BTreeMap::new();
    records.insert(
        "bob".to_string(),
        User {
            owner: "bob".into(),
            secret: "password".into(),
            subchans,
            pubchans,
        },
    );

    let mut users = Users::new();
    users.user_sets.push(UserSet::Static(records));

    let (shutdown_tx, notify_shutdown) = tokio::sync::watch::channel(false);

    let endpoint = parse_endpoint("tcp:interface=127.0.0.1:port=0").unwrap();
    let listener = Listener::new(endpoint, db, Arc::new(users), notify_shutdown)
        .await
        .unwrap();
    let addr = listener.local_addr();

    tokio::spawn(async move { server::run(vec![listener]).await });

    (addr, shutdown_tx)
}

async fn start_client(addr: SocketAddr) -> Connection {
    let mut conn = Connection::new(MultiStream::Tcp(TcpStream::connect(addr).await.unwrap()));

    let info = conn.read_frame().await.unwrap().unwrap();

    match info {
        Frame::Info(Info { broker_name, nonce }) => {
            assert_eq!(broker_name, "hpfeeds-broker");

            let signature = sign(nonce, "password");

            conn.write_frame(&Frame::Auth(Auth {
                ident: "bob".into(),
                signature: Bytes::from_iter(signature),
            }))
            .await
            .unwrap();
        }
        _ => panic!("Expected OP_INFO"),
    }

    conn
}

fn assert_published(
    frame: &Result<Option<Frame>>,
    expected_ident: &str,
    expected_channel: &str,
    expected_payload: &Bytes,
) {
    match frame {
        Ok(Some(Frame::Publish(Publish {
            ident,
            channel,
            payload,
        }))) => {
            assert_eq!(ident, expected_ident);
            assert_eq!(channel, expected_channel);
            assert_eq!(payload, expected_payload);
        }
        Ok(Some(frame)) => {
            panic!("Received unexpected frame: {}", frame);
        }
        Ok(None) => {
            panic!("No frame received");
        }
        Err(e) => {
            panic!("Received error: {}", e);
        }
    }
}

#[tokio::test]
async fn pub_sub() {
    /*
    Test sending a publish, starting a sub, sending a publish, starting a sub, sending a publish.

    sub1 should see the 2nd and 3rd publish.
    sub2 should see the 3rd publish.
    */
    let (addr, _notify) = start_server().await;

    let mut conn = start_client(addr).await;
    conn.write_frame(&Frame::Publish(Publish {
        ident: "foo".into(),
        channel: "bar".into(),
        payload: Bytes::from_static(b"this is a byte string"),
    }))
    .await
    .unwrap();

    let mut sub1 = start_client(addr).await;
    sub1.write_frame(&Frame::Subscribe(Subscribe {
        ident: "sub1".into(),
        channel: "bar".into(),
    }))
    .await
    .unwrap();

    conn.write_frame(&Frame::Publish(Publish {
        ident: "foo".into(),
        channel: "bar".into(),
        payload: Bytes::from_static(b"this is a byte strin2"),
    }))
    .await
    .unwrap();

    assert_published(
        &sub1.read_frame().await,
        "foo",
        "bar",
        &Bytes::from_static(b"this is a byte strin2"),
    );

    let mut sub2 = start_client(addr).await;
    sub2.write_frame(&Frame::Subscribe(Subscribe {
        ident: "sub2".into(),
        channel: "bar".into(),
    }))
    .await
    .unwrap();

    conn.write_frame(&Frame::Publish(Publish {
        ident: "foo".into(),
        channel: "bar".into(),
        payload: Bytes::from_static(b"this is a byte strin3"),
    }))
    .await
    .unwrap();

    assert_published(
        &sub1.read_frame().await,
        "foo",
        "bar",
        &Bytes::from_static(b"this is a byte strin3"),
    );
    assert_published(
        &sub2.read_frame().await,
        "foo",
        "bar",
        &Bytes::from_static(b"this is a byte strin3"),
    );
}

#[tokio::test]
async fn nsubscribe() {
    /*
    Test unsubscribing and resubscribing
    */
    let (addr, _notify) = start_server().await;

    let mut conn = start_client(addr).await;
    conn.write_frame(&Frame::Publish(Publish {
        ident: "foo".into(),
        channel: "bar".into(),
        payload: Bytes::from_static(b"this is a byte string"),
    }))
    .await
    .unwrap();

    // Start a subscriber
    let mut sub1 = start_client(addr).await;
    sub1.write_frame(&Frame::Subscribe(Subscribe {
        ident: "sub1".into(),
        channel: "bar".into(),
    }))
    .await
    .unwrap();

    conn.write_frame(&Frame::Publish(Publish {
        ident: "foo".into(),
        channel: "bar".into(),
        payload: Bytes::from_static(b"this is a byte strin2"),
    }))
    .await
    .unwrap();

    assert_published(
        &sub1.read_frame().await,
        "foo",
        "bar",
        &Bytes::from_static(b"this is a byte strin2"),
    );

    // Subscriber unsubscribes
    sub1.write_frame(&Frame::Unsubscribe(Unsubscribe {
        ident: "sub1".into(),
        channel: "bar".into(),
    }))
    .await
    .unwrap();

    // Subscriber misses a publish
    conn.write_frame(&Frame::Publish(Publish {
        ident: "foo".into(),
        channel: "bar".into(),
        payload: Bytes::from_static(b"this is a byte strin3"),
    }))
    .await
    .unwrap();

    // Subscriber subscribes
    let mut sub1 = start_client(addr).await;
    sub1.write_frame(&Frame::Subscribe(Subscribe {
        ident: "sub1".into(),
        channel: "bar".into(),
    }))
    .await
    .unwrap();

    // They should see this publish
    conn.write_frame(&Frame::Publish(Publish {
        ident: "foo".into(),
        channel: "bar".into(),
        payload: Bytes::from_static(b"this is a byte strin4"),
    }))
    .await
    .unwrap();

    assert_published(
        &sub1.read_frame().await,
        "foo",
        "bar",
        &Bytes::from_static(b"this is a byte strin4"),
    );
}
