use std::{fs::File, io::BufReader, path::Path, sync::Arc};

use anyhow::{bail, Result};
use arc_swap::ArcSwap;
use notify::{recommended_watcher, RecommendedWatcher, RecursiveMode, Watcher};
use rustls::crypto::ring::sign::any_supported_type;
use rustls::{
    server::{ClientHello, ResolvesServerCert},
    sign::CertifiedKey,
};
use rustls_pki_types::CertificateDer;
use rustls_pki_types::PrivateKeyDer;
use tracing::{error, info};

fn load_certs(path: String) -> Result<Vec<CertificateDer<'static>>> {
    let file = File::open(path)?;
    let mut reader = BufReader::new(file);
    let mut certs = vec![];
    for item in rustls_pemfile::certs(&mut reader) {
        certs.push(item?.into_owned());
    }
    Ok(certs)
}

fn load_keys(path: &str) -> Result<PrivateKeyDer> {
    let file = File::open(path)?;
    let mut reader = BufReader::new(file);

    let keys = rustls_pemfile::read_all(&mut reader);
    for item in keys {
        match item? {
            rustls_pemfile::Item::X509Certificate(_) => continue,
            rustls_pemfile::Item::Pkcs1Key(key) => return Ok(PrivateKeyDer::Pkcs1(key)),
            rustls_pemfile::Item::Pkcs8Key(key) => return Ok(PrivateKeyDer::Pkcs8(key)),
            rustls_pemfile::Item::Crl(_) => continue,
            _ => continue,
        }
    }
    bail!("No private keys found in {path}");
}

fn load_certified_key(private_key: &str, certificate: String) -> Result<CertifiedKey> {
    let certs = load_certs(certificate)?;
    let key = load_keys(private_key)?;

    let key = any_supported_type(&key)?;

    Ok(rustls::sign::CertifiedKey::new(certs, key))
}

#[derive(Debug)]
pub struct Resolver {
    pub certificate: Arc<ArcSwap<CertifiedKey>>,
    pub watcher: RecommendedWatcher,
}

impl Resolver {
    pub fn new(private_key: String, certificate: String, _chain: Option<String>) -> Result<Self> {
        let watched_private_key = private_key.clone();
        let watched_certificate = certificate.clone();

        let key = load_certified_key(&private_key, certificate.clone())?;
        let certified_key = Arc::new(ArcSwap::new(Arc::new(key)));
        let watched_certified_key = certified_key.clone();

        let mut watcher = recommended_watcher(move |res| {
            info!("Got inotify event: {res:?}");

            let key = match load_certified_key(&private_key, certificate.clone()) {
                Ok(key) => key,
                Err(e) => {
                    error!("Failed to reload certificates {e}");
                    return;
                }
            };

            watched_certified_key.store(Arc::new(key));

            info!("Certificate reloaded");
        })?;

        watcher.watch(Path::new(&watched_private_key), RecursiveMode::Recursive)?;
        watcher.watch(Path::new(&watched_certificate), RecursiveMode::Recursive)?;

        Ok(Resolver {
            certificate: certified_key,
            watcher,
        })
    }
}

impl ResolvesServerCert for Resolver {
    fn resolve(&self, _client_hello: ClientHello) -> Option<std::sync::Arc<CertifiedKey>> {
        Some(self.certificate.load_full())
    }
}
