use std::num::NonZeroUsize;
use std::sync::Arc;

use lru::LruCache;
use parking_lot::Mutex;
use pxar::accessor::cache::Cache;
use pxar::format::GoodbyeItem;

pub struct GoodbyeTableCache {
    inner: Mutex<LruCache<u64, Arc<[GoodbyeItem]>>>,
}

impl GoodbyeTableCache {
    pub fn new(capacity: usize) -> Self {
        let cap = NonZeroUsize::new(capacity.max(64)).unwrap();
        Self {
            inner: Mutex::new(LruCache::new(cap)),
        }
    }
}

impl Cache<u64, [GoodbyeItem]> for GoodbyeTableCache {
    fn fetch(&self, key: u64) -> Option<Arc<[GoodbyeItem]>> {
        self.inner.lock().get(&key).cloned()
    }

    fn insert(&self, key: u64, value: Arc<[GoodbyeItem]>) {
        self.inner.lock().put(key, value);
    }
}
