use std::fs::File;
use std::path::PathBuf;
use std::sync::Arc;

use anyhow::Error;
use lru::LruCache;
use parking_lot::Mutex;
use pbs_datastore::data_blob::DataBlob;
use pbs_datastore::read_chunk::ReadChunk;
use pbs_tools::crypt_config::CryptConfig;
use sha2::{Digest, Sha256};

const HEX_TABLE: &[u8; 16] = b"0123456789abcdef";

pub type ChunkKey = [u8; 32];

#[inline]
fn hex_encode(digest: &[u8; 32]) -> [u8; 64] {
    let mut out = [0u8; 64];
    for (i, &byte) in digest.iter().enumerate() {
        out[i * 2] = HEX_TABLE[(byte >> 4) as usize];
        out[i * 2 + 1] = HEX_TABLE[(byte & 0xf) as usize];
    }
    out
}

struct ChunkCache {
    lru: LruCache<ChunkKey, Arc<[u8]>>,
    current_bytes: usize,
    max_bytes: usize,
}

impl ChunkCache {
    fn new(max_bytes: usize) -> Self {
        Self {
            lru: LruCache::unbounded(),
            current_bytes: 0,
            max_bytes,
        }
    }

    fn get(&mut self, key: &ChunkKey) -> Option<Arc<[u8]>> {
        self.lru.get(key).cloned()
    }

    fn insert(&mut self, key: ChunkKey, data: Arc<[u8]>) {
        let entry_bytes = data.len();

        if entry_bytes > self.max_bytes {
            return;
        }

        if let Some(existing) = self.lru.peek(&key) {
            if Arc::ptr_eq(existing, &data) {
                return;
            }
        }

        while self.current_bytes + entry_bytes > self.max_bytes {
            if let Some((_k, evicted)) = self.lru.pop_lru() {
                self.current_bytes -= evicted.len();
            } else {
                break;
            }
        }

        if let Some((_old_key, old_val)) = self.lru.push(key, data) {
            self.current_bytes -= old_val.len();
        }
        self.current_bytes += entry_bytes;
    }
}

#[derive(Clone)]
pub struct LocalChunkStore {
    chunks_root: PathBuf,
    crypt: Option<Arc<CryptConfig>>,
    verify: bool,
    cache: Arc<Mutex<ChunkCache>>,
}

impl LocalChunkStore {
    pub fn with_cache_size(
        root: impl Into<PathBuf>,
        crypt: Option<Arc<CryptConfig>>,
        verify: bool,
        cache_bytes: usize,
    ) -> Self {
        let root = root.into();
        let chunks_root = root.join(".chunks");
        Self {
            chunks_root,
            crypt,
            verify,
            cache: Arc::new(Mutex::new(ChunkCache::new(cache_bytes))),
        }
    }

    fn chunk_path(&self, digest: &ChunkKey) -> PathBuf {
        let hex = hex_encode(digest);
        let hex_str = std::str::from_utf8(&hex).unwrap();
        let mut path = PathBuf::with_capacity(self.chunks_root.as_os_str().len() + 70);
        path.push(&self.chunks_root);
        path.push(&hex_str[..4]);
        path.push(hex_str);
        path
    }

    pub fn load_and_decode(&self, digest: &ChunkKey) -> Result<Arc<[u8]>, Error> {
        {
            let mut cache = self.cache.lock();
            if let Some(data) = cache.get(digest) {
                return Ok(data);
            }
        }

        let path = self.chunk_path(digest);
        let mut file = File::open(&path)
            .map_err(|e| anyhow::anyhow!("open chunk {} failed: {e}", path.display()))?;

        let blob = DataBlob::load_from_reader(&mut file)?;
        let data = blob.decode(self.crypt.as_deref(), None)?;

        if self.verify {
            let mut hasher = Sha256::new();
            hasher.update(&data);
            let got = hasher.finalize();
            if got[..] != digest[..] {
                return Err(anyhow::anyhow!(
                    "chunk digest mismatch for {}",
                    path.display()
                ));
            }
        }

        let arc: Arc<[u8]> = data.into();

        {
            let mut cache = self.cache.lock();
            cache.insert(*digest, Arc::clone(&arc));
        }

        Ok(arc)
    }
}

impl ReadChunk for LocalChunkStore {
    fn read_chunk(&self, digest: &ChunkKey) -> Result<Vec<u8>, Error> {
        let arc = self.load_and_decode(digest)?;
        Ok(arc.to_vec())
    }

    fn read_raw_chunk(&self, digest: &ChunkKey) -> Result<DataBlob, Error> {
        let path = self.chunk_path(digest);
        let mut file = File::open(&path)
            .map_err(|e| anyhow::anyhow!("open chunk {} failed: {e}", path.display()))?;
        DataBlob::load_from_reader(&mut file)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn chunk_cache_eviction_respects_byte_limit() {
        let mut cache = ChunkCache::new(100);
        let k1 = [1u8; 32];
        let k2 = [2u8; 32];
        let k3 = [3u8; 32];
        cache.insert(k1, Arc::from(vec![0u8; 40].as_slice()));
        cache.insert(k2, Arc::from(vec![0u8; 40].as_slice()));
        assert!(cache.get(&k1).is_some());
        assert!(cache.get(&k2).is_some());
        cache.insert(k3, Arc::from(vec![0u8; 40].as_slice()));
        assert!(cache.get(&k1).is_none());
        assert!(cache.get(&k2).is_some());
        assert!(cache.get(&k3).is_some());
        assert!(cache.current_bytes <= 100);
    }

    #[test]
    fn chunk_cache_rejects_oversized_entry() {
        let mut cache = ChunkCache::new(10);
        let k = [1u8; 32];
        cache.insert(k, Arc::from(vec![0u8; 20].as_slice()));
        assert!(cache.get(&k).is_none());
        assert_eq!(cache.current_bytes, 0);
    }
}
