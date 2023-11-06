use std::{
    fs::File,
    io::BufReader,
    path::Path,
    sync::{Arc, RwLock},
};

use anyhow::{bail, Result};
use notify::{recommended_watcher, RecommendedWatcher, RecursiveMode, Watcher};
use rustls::{
    server::{ClientHello, ResolvesServerCert},
    sign::CertifiedKey,
    Certificate, PrivateKey,
};
use tracing::{error, info};

fn load_certs(path: &str) -> Result<Vec<Certificate>> {
    let file = File::open(path)?;
    let mut reader = BufReader::new(file);
    let certs = rustls_pemfile::certs(&mut reader)?;
    Ok(certs.into_iter().map(Certificate).collect())
}

fn load_keys(path: &str) -> Result<PrivateKey> {
    let file = File::open(path)?;
    let mut reader = BufReader::new(file);
    let mut keys = rustls_pemfile::pkcs8_private_keys(&mut reader)?;
    match keys.len() {
        0 => bail!("No PKCS8-encoded private key found in {path}"),
        1 => Ok(PrivateKey(keys.remove(0))),
        _ => bail!("More than one PKCS8-encoded private key found in {path}"),
    }
}

fn load_certified_key(private_key: &str, certificate: &str) -> Result<CertifiedKey> {
    let certs = load_certs(&certificate)?;
    let key = load_keys(&private_key)?;

    let key = rustls::sign::any_supported_type(&key)?;

    Ok(rustls::sign::CertifiedKey::new(certs, key))
}

pub struct Resolver {
    pub certificate: Arc<RwLock<Arc<CertifiedKey>>>,
    pub watcher: RecommendedWatcher,
}

impl Resolver {
    pub fn new(private_key: String, certificate: String, _chain: Option<String>) -> Result<Self> {
        let watched_private_key = private_key.clone();
        let watched_certificate = certificate.clone();

        let key = load_certified_key(&private_key, &certificate)?;
        let certified_key = Arc::new(RwLock::new(Arc::new(key)));
        let watched_certified_key = certified_key.clone();

        let mut watcher = recommended_watcher(move |res| {
            info!("Got inotify event: {res:?}");

            let key = match load_certified_key(&private_key, &certificate) {
                Ok(key) => key,
                Err(e) => {
                    error!("Failed to reload certificates {e}");
                    return;
                }
            };

            let mut guard = watched_certified_key
                .write()
                .expect("Could not lock certificate");
            *guard = Arc::new(key);

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
        Some(self.certificate.read().unwrap().clone())
    }
}
