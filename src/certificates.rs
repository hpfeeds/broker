use std::{sync::{RwLock, Arc}, io::BufReader, fs::File, path::Path};

use anyhow::{Result, bail};
use notify::{recommended_watcher, RecursiveMode, Watcher};
use rustls::{sign::CertifiedKey, server::{ResolvesServerCert, ClientHello}, Certificate, PrivateKey};
use tracing::{info, error};

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

pub struct Resolver {
    pub certificate: Arc<RwLock<Arc<CertifiedKey>>>
}

impl Resolver {
    pub fn new(private_key: String, certificate: String, chain: Option<String>) -> Result<Self> {
        let watched_private_key = private_key.clone();
        let watched_certificate = certificate.clone();

        let certs = load_certs(&certificate)?;
        let key = load_keys(&private_key)?;

        let mut watcher = recommended_watcher(move |res| {
            info!("Got inotify event: {res:?}");

            let mut guard = users.write().expect("Could not lock user data");
            *guard = new_users;
        })?;

        watcher.watch(Path::new(&watched_private_key), RecursiveMode::Recursive)?;
        watcher.watch(Path::new(&watched_certificate), RecursiveMode::Recursive)?;

        Resolver { certificate: ... }
    }
}

impl ResolvesServerCert for Resolver
{
    fn resolve(
        &self,
        _client_hello: ClientHello,
    ) -> Option<std::sync::Arc<CertifiedKey>> {
        Some(self.certificate.read().unwrap().clone())
    }
}