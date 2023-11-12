use bencher::{benchmark_group, benchmark_main, Bencher};
use bytes::Bytes;
use prometheus_client::registry::Registry;
use std::{
    collections::{BTreeMap, BTreeSet},
    net::SocketAddr,
    sync::Arc,
};

use tokio::{net::TcpStream, sync::watch::Sender};

use hpfeeds_broker::{
    frame::{Auth, Info, Publish, Subscribe},
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

    let (notify_tx, notify_shutdown) = tokio::sync::watch::channel(false);

    let endpoint = parse_endpoint("tcp:interface=127.0.0.1:port=0").unwrap();
    let listener = Listener::new(endpoint, db, Arc::new(users), notify_shutdown)
        .await
        .unwrap();
    let addr = listener.local_addr();

    tokio::spawn(async move { server::run(vec![listener]).await });

    (addr, notify_tx)
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

fn rt() -> tokio::runtime::Runtime {
    tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .build()
        .unwrap()
}

fn single_subscriber(bench: &mut Bencher) {
    let rt = rt();

    let (mut p, mut s, _guard) = rt.block_on(async {
        let (server, guard) = start_server().await;

        let mut s = start_client(server).await;

        s.write_frame(&Frame::Subscribe(Subscribe {
            ident: "foo".into(),
            channel: "bar".into(),
        }))
        .await
        .unwrap();

        let mut p = start_client(server).await;

        p.write_frame(&Frame::Publish(Publish {
            ident: "foo".into(),
            channel: "bar".into(),
            payload: Arc::new(Bytes::from_static(b"hello world")),
        }))
        .await
        .unwrap();

        s.read_frame().await.unwrap().unwrap();

        (p, s, guard)
    });

    bench.iter(|| {
        rt.block_on(async {
            p.write_frame(&Frame::Publish(Publish {
                ident: "foo".into(),
                channel: "bar".into(),
                payload: Arc::new(Bytes::from_static(b"hello world")),
            }))
            .await
            .unwrap();

            s.read_frame().await.unwrap().unwrap();
        })
    });
}

fn twenty_subscribers(bench: &mut Bencher) {
    let rt = rt();

    let (mut p, mut subscribers, _guard) = rt.block_on(async {
        let (server, guard) = start_server().await;

        let mut subscribers = vec![];
        for _i in 1..20 {
            let mut s = start_client(server).await;
            s.write_frame(&Frame::Subscribe(Subscribe {
                ident: "foo".into(),
                channel: "bar".into(),
            }))
            .await
            .unwrap();
            subscribers.push(s);
        }

        let mut p = start_client(server).await;

        p.write_frame(&Frame::Publish(Publish {
            ident: "foo".into(),
            channel: "bar".into(),
            payload: Arc::new(Bytes::from_static(b"hello world")),
        }))
        .await
        .unwrap();

        (p, subscribers, guard)
    });

    bench.iter(|| {
        rt.block_on(async {
            p.write_frame(&Frame::Publish(Publish {
                ident: "foo".into(),
                channel: "bar".into(),
                payload: Arc::new(Bytes::from_static(b"hello world")),
            }))
            .await
            .unwrap();

            for sub in subscribers.iter_mut() {
                sub.read_frame().await.unwrap().unwrap();
            }
        })
    });

    rt.shutdown_background();
}

benchmark_group!(benches, single_subscriber, twenty_subscribers);
benchmark_main!(benches);
