use std::{
    ops::Deref,
    sync::{Arc, Mutex},
};

use dashmap::DashMap;

use crate::{RespEncode, RespError, RespFrame};

#[derive(Debug, Clone)]
pub struct Backend(Arc<BackendInner>);

#[derive(Debug)]
pub struct BackendInner {
    pub(crate) map: DashMap<String, RespFrame>,
    pub(crate) hmap: DashMap<String, DashMap<String, RespFrame>>,

    pub(crate) smap: DashMap<String, Arc<Mutex<Vec<RespFrame>>>>,
}

impl Deref for Backend {
    type Target = BackendInner;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl Default for BackendInner {
    fn default() -> Self {
        Self {
            map: DashMap::new(),
            hmap: DashMap::new(),
            smap: DashMap::new(),
        }
    }
}

impl Default for Backend {
    fn default() -> Self {
        Self(Arc::new(BackendInner::default()))
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

    pub fn hget(&self, key: &str, field: String) -> Option<RespFrame> {
        self.hmap
            .get(key)
            .and_then(|v| v.get(&field).map(|v| v.value().clone()))
    }

    pub fn hset(&self, key: String, field: String, value: RespFrame) {
        let hmap = self.hmap.entry(key).or_default();
        hmap.insert(field, value);
    }

    pub fn hgetall(&self, key: &str) -> Option<DashMap<String, RespFrame>> {
        self.hmap.get(key).map(|v| v.clone())
    }

    pub fn sadd(&self, key: String, values: Vec<RespFrame>) -> Result<usize, RespError> {
        let mut ret: usize = 0;
        let key_entry = self.smap.entry(key).or_default();
        let Ok(mut entry) = key_entry.value().lock() else {
            eprintln!("Failed to acquire lock");
            return Err(RespError::InternalServerError);
        };

        // 由于 f64 不支持比较的特性，导致不能使用set之类的数据结构
        // 改用 vector，但比较对象的唯一性时 使用编解码的方式(可用，但效率不一定高)
        for v in values {
            let encoded = v.clone().encode();
            // 对象不存在才添加
            if !entry.iter().any(|v| v.clone().encode() == encoded) {
                ret += 1; // 统计新增了多少
                entry.push(v);
            }
        }

        Ok(ret)
    }
}
