use crate::Result;
use serde::Deserialize;
use std::collections::{BTreeMap, BTreeSet};

#[derive(Deserialize, Clone, Debug)]
pub struct User {
    pub owner: String,
    pub secret: String,
    pub pubchans: BTreeSet<String>,
    pub subchans: BTreeSet<String>,
}

#[derive(Debug)]
pub struct UserSet {
    pub users: BTreeMap<String, User>,
}

#[derive(Debug)]
pub struct Users {
    pub user_sets: Vec<UserSet>,
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
            let (_, rest) = key.split_once("_").unwrap();
            let (rest, _) = rest.split_once("_").unwrap();

            let owner = match std::env::var(format!("HPFEEDS_{}_OWNER", rest)) {
                Ok(owner) => owner,
                Err(_) => "".to_string(),
            };
            let pubchans = match std::env::var(format!("HPFEEDS_{}_PUBCHANS", rest)) {
                Ok(pubchans) => pubchans.split(",").map(|v| v.to_string()).collect(),
                Err(_) => BTreeSet::new(),
            };
            let subchans = match std::env::var(format!("HPFEEDS_{}_SUBCHANS", rest)) {
                Ok(pubchans) => pubchans.split(",").map(|v| v.to_string()).collect(),
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
            user_sets: vec![UserSet { users }],
        }
    }

    pub fn add_user_set(&mut self, path: String) -> Result<()> {
        let users_string = std::fs::read_to_string(path)?;
        let users: BTreeMap<String, User> = serde_json::from_str(&users_string)?;

        self.user_sets.push(UserSet { users });

        Ok(())
    }

    pub fn get(&self, username: &String) -> Option<User> {
        for set in &self.user_sets {
            if let Some(user) = set.users.get(username) {
                return Some(user.clone());
            }
        }

        None
    }
}
