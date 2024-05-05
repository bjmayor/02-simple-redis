use std::{ops::Deref, sync::Arc};

use dashmap::{DashMap, DashSet};

use crate::RespFrame;

#[derive(Debug, Clone)]
pub struct Backend(Arc<BackendInner>);

#[derive(Debug)]
pub struct BackendInner {
    pub(crate) map: DashMap<String, RespFrame>,
    pub(crate) hmap: DashMap<String, DashMap<String, RespFrame>>,
    pub(crate) set: DashMap<String, DashSet<String>>,
}

impl Deref for Backend {
    type Target = BackendInner;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl Default for Backend {
    fn default() -> Self {
        Self(Arc::new(BackendInner::default()))
    }
}

impl Default for BackendInner {
    fn default() -> Self {
        Self {
            map: DashMap::new(),
            hmap: DashMap::new(),
            set: DashMap::new(),
        }
    }
}

impl Backend {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn get(&self, key: &str) -> Option<RespFrame> {
        self.map.get(key).map(|v| v.value().clone())
    }

    pub fn set(&self, key: String, value: RespFrame) {
        self.map.insert(key, value);
    }

    pub fn hget(&self, key: &str, field: &str) -> Option<RespFrame> {
        self.hmap
            .get(key)
            .and_then(|inner| inner.get(field).map(|v| v.value().clone()))
    }

    pub fn hgetall(&self, key: &str) -> Option<DashMap<String, RespFrame>> {
        self.hmap.get(key).map(|v| v.clone())
    }

    pub fn hset(&self, key: String, field: String, value: RespFrame) {
        let inner = self.hmap.entry(key).or_default();
        inner.insert(field, value);
    }

    pub fn hmget(&self, key: &str, fields: &[String]) -> Vec<Option<RespFrame>> {
        let hmap = self.hmap.get(key);

        match hmap {
            Some(hmap) => {
                let mut data = Vec::with_capacity(fields.len());
                for field in fields {
                    let value = hmap.get(field).map(|v| v.value().clone());
                    data.push(value);
                }

                data
            }
            None => vec![None; fields.len()],
        }
    }

    pub fn sadd(&self, key: String, members: Vec<String>) -> usize {
        let set = self.set.entry(key).or_default();
        let mut added = 0;
        for member in members {
            if set.insert(member) {
                added += 1;
            }
        }
        added
    }

    pub fn smembers(&self, key: &str) -> Option<DashSet<String>> {
        self.set.get(key).map(|v| v.clone())
    }

    pub fn sismember(&self, key: &str, member: &str) -> bool {
        self.set.get(key).map_or(false, |v| v.contains(member))
    }
}
