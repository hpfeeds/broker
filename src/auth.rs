use anyhow::Result;
use arc_swap::ArcSwap;
use notify::{recommended_watcher, RecommendedWatcher, RecursiveMode, Watcher};
use serde::Deserialize;
use sha1::{Digest, Sha1};
use std::{
    collections::{BTreeMap, BTreeSet},
    path::Path,
    sync::Arc,
};
use tracing::{error, info};

#[derive(Deserialize, Clone, Debug)]
pub struct User {
    pub owner: String,
    pub secret: String,
    pub pubchans: BTreeSet<String>,
    pub subchans: BTreeSet<String>,
}

#[derive(Debug)]
pub enum UserSet {
    Static(BTreeMap<String, User>),
    Json {
        users: Arc<ArcSwap<BTreeMap<String, User>>>,
        watcher: RecommendedWatcher,
    },
}

impl UserSet {
    pub fn from_json(path: &str) -> Result<Self> {
        let path = path.to_string();
        let watched_path = path.clone();

        let users_string = std::fs::read_to_string(&path)?;
        let users: BTreeMap<String, User> = serde_json::from_str(&users_string)?;
        let users = Arc::new(ArcSwap::new(Arc::new(users)));
        let users_ro_view = users.clone();

        let mut watcher = recommended_watcher(move |res| {
            info!("Got inotify event: {res:?}");

            let users_string = match std::fs::read_to_string(&path) {
                Ok(data) => data,
                Err(e) => {
                    error!("UserSet: Failed to load path: {path}: {e:?}");
                    return;
                }
            };
            let new_users: BTreeMap<String, User> = match serde_json::from_str(&users_string) {
                Ok(data) => data,
                Err(e) => {
                    error!("UserSet: Failed to parse json: {path}: {e:?}");
                    return;
                }
            };

            let len = new_users.len();

            users.swap(Arc::new(new_users));

            info!("Reloaded user data: {path}. Found {} records.", len);
        })?;

        watcher.watch(Path::new(&watched_path), RecursiveMode::Recursive)?;

        Ok(UserSet::Json {
            users: users_ro_view,
            watcher,
        })
    }

    pub fn get_user(&self, user: &str) -> Option<User> {
        match self {
            Self::Static(users) => users.get(user).cloned(),
            Self::Json { users, .. } => users.load_full().get(user).cloned(),
        }
    }
}

#[derive(Debug)]
pub struct Users {
    pub user_sets: Vec<UserSet>,
}

pub fn sign(data: [u8; 4], secret: &str) -> [u8; 20] {
    let mut hasher = Sha1::new();
    hasher.update(data);
    hasher.update(secret);

    hasher.finalize().into()
}

impl Users {
    pub fn new() -> Self {
        let mut users = BTreeMap::new();

        for (key, value) in std::env::vars() {
            if !key.starts_with("HPFEEDS_") || !key.ends_with("_SECRET") {
                continue;
            }

            // If is starts with HPFEEDS_ and ends with _SECRET then none of these will fail
            // APART from HPFEEDS_SECRET, and thats not valid
            let (_, rest) = key.split_once('_').unwrap();
            let (rest, _) = rest.split_once('_').unwrap();

            let owner = match std::env::var(format!("HPFEEDS_{}_OWNER", rest)) {
                Ok(owner) => owner,
                Err(_) => "".to_string(),
            };
            let pubchans = match std::env::var(format!("HPFEEDS_{}_PUBCHANS", rest)) {
                Ok(pubchans) => pubchans.split(',').map(|v| v.to_string()).collect(),
                Err(_) => BTreeSet::new(),
            };
            let subchans = match std::env::var(format!("HPFEEDS_{}_SUBCHANS", rest)) {
                Ok(pubchans) => pubchans.split(',').map(|v| v.to_string()).collect(),
                Err(_) => BTreeSet::new(),
            };

            users.insert(
                key,
                User {
                    owner,
                    secret: value,
                    pubchans,
                    subchans,
                },
            );
        }

        Users {
            user_sets: vec![UserSet::Static(users)],
        }
    }

    pub fn add_user_set(&mut self, path: String) -> Result<()> {
        let user_set = UserSet::from_json(&path)?;
        self.user_sets.push(user_set);

        Ok(())
    }

    pub fn get(&self, username: &str) -> Option<User> {
        for set in &self.user_sets {
            if let Some(user) = set.get_user(username) {
                return Some(user.clone());
            }
        }

        None
    }
}

impl Default for Users {
    fn default() -> Self {
        Users::new()
    }
}
