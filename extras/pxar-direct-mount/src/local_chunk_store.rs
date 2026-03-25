use std::fs::File;
use std::num::NonZeroUsize;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::{Duration, Instant};

use anyhow::Error;
use lru::LruCache;
use parking_lot::Mutex;
use pbs_datastore::data_blob::DataBlob;
use pbs_datastore::read_chunk::ReadChunk;
use pbs_tools::crypt_config::CryptConfig;
use sha2::{Digest, Sha256};

const DEFAULT_MAX_CACHED_CHUNKS: usize = 4096;
const DEFAULT_TTL_SECS: u64 = 300; // 5 minutes

// 64 independent shards keyed on digest[0] & 63.
// Must be a power of 2.
const SHARD_COUNT: usize = 64;

pub struct ChunkCacheConfig {
    pub max_entries: usize,
    /// How long an unaccessed chunk stays in the cache.
    pub ttl: Duration,
}

impl Default for ChunkCacheConfig {
    fn default() -> Self {
        Self {
            max_entries: DEFAULT_MAX_CACHED_CHUNKS,
            ttl: Duration::from_secs(DEFAULT_TTL_SECS),
        }
    }
}

type ChunkKey = [u8; 32];

struct CacheEntry {
    data: Arc<Vec<u8>>,
    /// Updated on every access; entries idle longer than `ttl` are evicted.
    last_access: Instant,
}

/// Sharded LRU + TTL cache for decoded chunks.
///
/// Sharding by `digest[0] & 63` gives ~64× lower Mutex contention under
/// concurrent load.  TTL ensures memory is reclaimed when no one is actively
/// reading — important for a mount that may be idle for extended periods.
struct ShardedChunkCache {
    shards: Vec<Mutex<LruCache<ChunkKey, CacheEntry>>>,
    ttl: Duration,
}

impl ShardedChunkCache {
    fn new(total_capacity: usize, ttl: Duration) -> Self {
        let per_shard = NonZeroUsize::new((total_capacity / SHARD_COUNT).max(4)).unwrap();
        let shards = (0..SHARD_COUNT)
            .map(|_| Mutex::new(LruCache::new(per_shard)))
            .collect();
        Self { shards, ttl }
    }

    #[inline]
    fn shard_idx(digest: &ChunkKey) -> usize {
        (digest[0] as usize) & (SHARD_COUNT - 1)
    }

    /// Return cached data if present and not expired; evict and return `None`
    /// if expired.
    fn get(&self, digest: &ChunkKey) -> Option<Arc<Vec<u8>>> {
        let mut shard = self.shards[Self::shard_idx(digest)].lock();

        // peek() checks without updating LRU order so we can evict if stale.
        let expired = shard
            .peek(digest)
            .map(|e| e.last_access.elapsed() > self.ttl)
            .unwrap_or(false);

        if expired {
            shard.pop(digest);
            return None;
        }

        // Valid — get() updates LRU order; also refresh last_access.
        if let Some(entry) = shard.get_mut(digest) {
            entry.last_access = Instant::now();
            return Some(Arc::clone(&entry.data));
        }
        None
    }

    fn put(&self, digest: ChunkKey, data: Arc<Vec<u8>>) {
        self.shards[Self::shard_idx(&digest)].lock().put(
            digest,
            CacheEntry {
                data,
                last_access: Instant::now(),
            },
        );
    }

    /// Remove all entries that have not been accessed within the TTL.
    /// Called periodically by the background cleanup task.
    pub fn sweep_expired(&self) {
        let now = Instant::now();
        for shard in &self.shards {
            let mut guard = shard.lock();
            let expired: Vec<ChunkKey> = guard
                .iter()
                .filter_map(|(k, e)| {
                    if now.duration_since(e.last_access) > self.ttl {
                        Some(*k)
                    } else {
                        None
                    }
                })
                .collect();
            for key in expired {
                guard.pop(&key);
            }
        }
    }
}

#[derive(Clone)]
pub struct LocalChunkStore {
    root: PathBuf,
    crypt: Option<Arc<CryptConfig>>,
    verify: bool,
    cache: Arc<ShardedChunkCache>,
}

impl LocalChunkStore {
    pub fn new(root: impl Into<PathBuf>, crypt: Option<Arc<CryptConfig>>, verify: bool) -> Self {
        Self::with_cache_config(root, crypt, verify, ChunkCacheConfig::default())
    }

    pub fn with_cache_config(
        root: impl Into<PathBuf>,
        crypt: Option<Arc<CryptConfig>>,
        verify: bool,
        config: ChunkCacheConfig,
    ) -> Self {
        Self {
            root: root.into(),
            crypt,
            verify,
            cache: Arc::new(ShardedChunkCache::new(
                config.max_entries.max(SHARD_COUNT * 4),
                config.ttl,
            )),
        }
    }

    /// Periodically sweep expired cache entries.  Run this as a background
    /// tokio task alongside the FUSE session; it exits when dropped.
    pub async fn run_cleanup(self, sweep_interval: Duration) {
        let mut ticker = tokio::time::interval(sweep_interval);
        ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        loop {
            ticker.tick().await;
            self.cache.sweep_expired();
        }
    }

    fn chunk_path(&self, digest: &ChunkKey) -> PathBuf {
        let hex = hex::encode(digest);
        let dir4 = &hex[..4];
        self.root.join(".chunks").join(dir4).join(&hex)
    }

    fn load_and_decode(&self, digest: &ChunkKey) -> Result<Arc<Vec<u8>>, Error> {
        if let Some(cached) = self.cache.get(digest) {
            return Ok(cached);
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

        let data = Arc::new(data);
        self.cache.put(*digest, Arc::clone(&data));
        Ok(data)
    }
}

impl ReadChunk for LocalChunkStore {
    fn read_chunk(&self, digest: &ChunkKey) -> Result<Vec<u8>, Error> {
        self.load_and_decode(digest).map(|arc| (*arc).clone())
    }

    fn read_raw_chunk(&self, digest: &ChunkKey) -> Result<DataBlob, Error> {
        let path = self.chunk_path(digest);
        let mut file = File::open(&path)
            .map_err(|e| anyhow::anyhow!("open chunk {} failed: {e}", path.display()))?;
        DataBlob::load_from_reader(&mut file)
    }
}

pub trait ChunkCacheStore: ReadChunk + Send + Sync + 'static {
    fn read_chunk_cached(&self, digest: &ChunkKey) -> Result<Arc<Vec<u8>>, Error>;
    /// Return cached data without triggering a disk load.
    /// Returns `None` if the chunk is not in the cache (or expired).
    fn peek_cached(&self, digest: &ChunkKey) -> Option<Arc<Vec<u8>>>;
}

impl ChunkCacheStore for LocalChunkStore {
    fn read_chunk_cached(&self, digest: &ChunkKey) -> Result<Arc<Vec<u8>>, Error> {
        self.load_and_decode(digest)
    }

    fn peek_cached(&self, digest: &ChunkKey) -> Option<Arc<Vec<u8>>> {
        self.cache.get(digest)
    }
}
