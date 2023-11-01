use std::collections::BTreeMap;

use crate::Result;

#[derive(PartialEq, Debug, Clone)]
pub enum ListenerClass {
    Tcp,
    Tls {
        private_key: String,
        certificate: String,
        chain: Option<String>,
    },
}

#[derive(PartialEq, Debug, Clone)]
pub struct Endpoint {
    pub listener_class: ListenerClass,
    pub interface: String,
    pub port: u32,
    pub device: Option<String>,
}

pub fn parse_endpoint(endpoint: &str) -> Result<Endpoint> {
    // Parses a twisted style endpoint descriptor
    let mut parts = endpoint.split(':');
    let first = parts.next().unwrap();

    let mut kv = BTreeMap::new();
    for part in parts {
        let (k, v) = part.split_once('=').unwrap();
        kv.insert(k.to_string(), v.to_string());
    }

    let interface = kv
        .get("interface")
        .cloned()
        .unwrap_or("127.0.0.1".to_string());

    let port = kv
        .get("port")
        .cloned()
        .unwrap_or("10000".to_string())
        .parse()
        .unwrap();

    let device = kv.get("device").cloned();

    Ok(Endpoint {
        interface,
        port,
        device,
        listener_class: match first {
            "tcp" => ListenerClass::Tcp,
            "tls" => ListenerClass::Tls {
                private_key: kv.get("key").cloned().unwrap(),
                certificate: kv.get("cert").cloned().unwrap(),
                chain: kv.get("chain").cloned(),
            },
            _ => {
                panic!("Unknwon listener class. Must be tcp or tls.");
            }
        },
    })
}

#[cfg(test)]
mod tests {
    use crate::{
        endpoint::{Endpoint, ListenerClass},
        parse_endpoint,
    };

    #[test]
    fn test_parse_endpoint() {
        assert_eq!(
            parse_endpoint("tcp").unwrap(),
            Endpoint {
                listener_class: ListenerClass::Tcp,
                interface: "127.0.0.1".to_string(),
                port: 10000,
                device: None,
            }
        );

        assert_eq!(
            parse_endpoint("tcp:interface=127.0.0.2:port=10000:device=eth0").unwrap(),
            Endpoint {
                listener_class: ListenerClass::Tcp,
                interface: "127.0.0.2".to_string(),
                port: 10000,
                device: Some("eth0".to_string()),
            }
        );

        assert_eq!(
            parse_endpoint(
                "tls:port=30000:key=/app/var/tls/tls.key:cert=/app/var/tls/tls.crt:device=eth0"
            )
            .unwrap(),
            Endpoint {
                listener_class: ListenerClass::Tls {
                    certificate: "/app/var/tls/tls.crt".to_string(),
                    private_key: "/app/var/tls/tls.key".to_string(),
                    chain: None,
                },
                interface: "127.0.0.1".to_string(),
                port: 30000,
                device: Some("eth0".to_string()),
            }
        );
    }
}
