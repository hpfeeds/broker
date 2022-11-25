use bencher::{benchmark_group, benchmark_main, Bencher};
use bytes::Bytes;
use std::net::SocketAddr;

use tokio::net::{TcpListener, TcpStream};

use hpfeeds_broker::{server, Connection, Frame};

async fn start_server() -> SocketAddr {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();

    tokio::spawn(async move { server::run(listener, tokio::signal::ctrl_c()).await });

    addr
}

async fn start_client(addr: SocketAddr) -> Connection {
    let mut conn = Connection::new(TcpStream::connect(addr).await.unwrap());

    let info = conn.read_frame().await.unwrap().unwrap();

    match info {
        Frame::Info {
            broker_name,
            nonce: _,
        } => {
            assert_eq!(broker_name, "hpfeeds-broker");
        }
        _ => panic!("Expected OP_INFO"),
    }

    // FIXME: Here, need to send an OP_AUTH...

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

    let [mut p, mut s] = rt.block_on(async {
        let server = start_server().await;

        let mut s = start_client(server).await;

        s.write_frame(&Frame::Subscribe {
            ident: "foo".into(),
            channel: "bar".into(),
        })
        .await
        .unwrap();

        let mut p = start_client(server).await;

        p.write_frame(&Frame::Publish {
            ident: "foo".into(),
            channel: "bar".into(),
            payload: Bytes::from_static(b"hello world"),
        })
        .await
        .unwrap();

        s.read_frame().await.unwrap().unwrap();

        [p, s]
    });

    bench.iter(|| {
        rt.block_on(async {
            p.write_frame(&Frame::Publish {
                ident: "foo".into(),
                channel: "bar".into(),
                payload: Bytes::from_static(b"hello world"),
            })
            .await
            .unwrap();

            s.read_frame().await.unwrap().unwrap();
        })
    });
}

fn twenty_subscribers(bench: &mut Bencher) {
    let rt = rt();

    let (mut p, mut subscribers) = rt.block_on(async {
        let server = start_server().await;

        let mut subscribers = vec![];
        for _i in 1..20 {
            let mut s = start_client(server).await;
            s.write_frame(&Frame::Subscribe {
                ident: "foo".into(),
                channel: "bar".into(),
            })
            .await
            .unwrap();
            subscribers.push(s);
        }

        let mut p = start_client(server).await;

        p.write_frame(&Frame::Publish {
            ident: "foo".into(),
            channel: "bar".into(),
            payload: Bytes::from_static(b"hello world"),
        })
        .await
        .unwrap();

        (p, subscribers)
    });

    bench.iter(|| {
        rt.block_on(async {
            p.write_frame(&Frame::Publish {
                ident: "foo".into(),
                channel: "bar".into(),
                payload: Bytes::from_static(b"hello world"),
            })
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
