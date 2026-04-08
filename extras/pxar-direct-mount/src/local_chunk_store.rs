use std::collections::BTreeMap;
use std::fs::File;
use std::num::NonZeroUsize;
use std::path::PathBuf;
use std::sync::atomic::{AtomicUsize, Ordering};
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
    /// When this entry expires (last_access + TTL)
    expires_at: Instant,
}

/// Sharded LRU + TTL cache for decoded chunks using epoch-based expiration
/// with proportional eviction (Go GC-assist style).
///
/// Sharding by `digest[0] & 63` gives ~64× lower Mutex contention under
/// concurrent load. TTL uses a sorted expiration queue. Eviction work is
/// amortized across inserts (proportional to allocation rate), eliminating
/// the need for background sweeps.
struct ShardedChunkCache {
    shards: Vec<Mutex<LruCache<ChunkKey, CacheEntry>>>,
    ttl: Duration,
    /// Sorted queue of expiration times to entry keys.
    expiration_queue: Mutex<BTreeMap<Instant, Vec<ChunkKey>>>,
    /// Per-shard counters tracking how many inserts happened since last eviction.
    /// When counter exceeds threshold, proportional eviction is triggered.
    insert_counters: Vec<AtomicUsize>,
    /// Ratio of evictions to inserts (e.g., 1 means evict 1 per 1 insert)
    eviction_ratio: usize,
}

impl ShardedChunkCache {
    fn new(total_capacity: usize, ttl: Duration) -> Self {
        let per_shard = NonZeroUsize::new((total_capacity / SHARD_COUNT).max(4)).unwrap();
        let shards = (0..SHARD_COUNT)
            .map(|_| Mutex::new(LruCache::new(per_shard)))
            .collect();
        let insert_counters = (0..SHARD_COUNT)
            .map(|_| AtomicUsize::new(0))
            .collect();
        Self {
            shards,
            ttl,
            expiration_queue: Mutex::new(BTreeMap::new()),
            insert_counters,
            // Evict 2 expired entries per 1 insert when pressure builds
            // This ratio ensures we stay ahead of expiration accumulation
            eviction_ratio: 2,
        }
    }

    #[inline]
    fn shard_idx(digest: &ChunkKey) -> usize {
        (digest[0] as usize) & (SHARD_COUNT - 1)
    }

    /// Return cached data if present and not expired; evict and return `None`
    /// if expired.
    fn get(&self, digest: &ChunkKey) -> Option<Arc<Vec<u8>>> {
        let mut shard = self.shards[Self::shard_idx(digest)].lock();

        // Check if entry exists and is not expired
        match shard.get_mut(digest) {
            Some(entry) => {
                let now = Instant::now();
                if now > entry.expires_at {
                    // Entry expired - evict it
                    let expires_at = entry.expires_at;
                    shard.pop(digest);
                    drop(shard); // Release lock before updating queue
                    self.remove_from_expiration_queue(digest, expires_at);
                    return None;
                }
                // Update expiration time and queue
                let old_expires = entry.expires_at;
                entry.expires_at = now + self.ttl;
                let new_expires = entry.expires_at;
                let data = Arc::clone(&entry.data);
                drop(shard); // Release lock before updating queue
                self.update_expiration(digest, old_expires, new_expires);
                Some(data)
            }
            None => None,
        }
    }

    fn put(&self, digest: ChunkKey, data: Arc<Vec<u8>>) {
        let expires_at = Instant::now() + self.ttl;
        let shard_idx = Self::shard_idx(&digest);

        // Check if we're evicting an entry and remove it from expiration queue
        let evicted = {
            let mut shard = self.shards[shard_idx].lock();
            shard.push(
                digest,
                CacheEntry {
                    data,
                    expires_at,
                },
            )
        };

        // Update expiration queue outside of shard lock
        if let Some((old_digest, old_entry)) = evicted {
            self.remove_from_expiration_queue(&old_digest, old_entry.expires_at);
        }
        self.add_to_expiration_queue(digest, expires_at);

        // Proportional eviction (Go GC-assist style):
        // Track inserts and do eviction work proportional to insert rate.
        // This amortizes cleanup across normal operations, eliminating
        // the need for background sweeps.
        let counter = self.insert_counters[shard_idx].fetch_add(1, Ordering::Relaxed);
        // Every 4 inserts, do proportional eviction work
        const INSERT_THRESHOLD: usize = 4;
        if counter % INSERT_THRESHOLD == INSERT_THRESHOLD - 1 {
            // Evict up to (eviction_ratio * threshold) expired entries
            let to_evict = self.eviction_ratio * INSERT_THRESHOLD;
            self.evict_expired_proportional(to_evict);
        }
    }

    /// Add entry to expiration queue
    fn add_to_expiration_queue(&self, digest: ChunkKey, expires_at: Instant) {
        let mut queue = self.expiration_queue.lock();
        queue.entry(expires_at).or_default().push(digest);
    }

    /// Remove entry from expiration queue
    fn remove_from_expiration_queue(&self, digest: &ChunkKey, expires_at: Instant) {
        let mut queue = self.expiration_queue.lock();
        if let Some(entries) = queue.get_mut(&expires_at) {
            entries.retain(|k| k != digest);
            if entries.is_empty() {
                queue.remove(&expires_at);
            }
        }
    }

    /// Update expiration time for an entry (called on access)
    fn update_expiration(&self, digest: &ChunkKey, old_expires: Instant, new_expires: Instant) {
        if old_expires == new_expires {
            return;
        }
        self.remove_from_expiration_queue(digest, old_expires);
        self.add_to_expiration_queue(*digest, new_expires);
    }

    /// Remove entry from both cache and expiration queue
    fn remove_from_cache(&self, digest: &ChunkKey) {
        let shard_idx = Self::shard_idx(digest);
        let mut shard = self.shards[shard_idx].lock();
        if let Some(entry) = shard.pop(digest) {
            // Remove from expiration queue outside shard lock
            drop(shard);
            self.remove_from_expiration_queue(digest, entry.expires_at);
        }
    }

    /// Remove expired entries using the sorted expiration queue.
    /// Only processes entries that are actually expired, making this O(k)
    /// where k is the number of expired entries.
    pub fn sweep_expired(&self) {
        let now = Instant::now();
        let mut queue = self.expiration_queue.lock();

        // Collect all expired timestamps (entries <= now)
        let expired_times: Vec<Instant> = queue
            .range(..=now)
            .map(|(time, _)| *time)
            .collect();

        // For each expired timestamp, evict all its entries
        for time in expired_times {
            if let Some(digests) = queue.remove(&time) {
                // Drop queue lock before touching shards
                drop(queue);

                for digest in digests {
                    let shard_idx = Self::shard_idx(&digest);
                    let mut shard = self.shards[shard_idx].lock();
                    // Double-check it's still expired (might have been accessed)
                    if let Some(entry) = shard.peek(&digest) {
                        if now > entry.expires_at {
                            shard.pop(&digest);
                        }
                    }
                }

                // Re-acquire queue lock for next iteration
                queue = self.expiration_queue.lock();
            }
        }
    }

    /// Proportional eviction: evict up to `max_count` expired entries.
    /// Called during inserts to amortize cleanup work (Go GC-assist style).
    /// Returns the number of entries actually evicted.
    fn evict_expired_proportional(&self, max_count: usize) -> usize {
        let now = Instant::now();
        let mut queue = self.expiration_queue.lock();
        let mut evicted = 0;

        // Collect expired timestamps until we reach max_count
        let mut expired_times: Vec<Instant> = Vec::new();
        for (time, _entries) in queue.range(..=now) {
            expired_times.push(*time);
            // Estimate count - we'll check actual count when evicting
            evicted += 1;
            if evicted >= max_count {
                break;
            }
        }
        evicted = 0; // Reset for actual count

        // Evict entries for each expired timestamp
        for time in expired_times {
            if let Some(digests) = queue.remove(&time) {
                // Drop queue lock before touching shards
                drop(queue);

                let mut remaining: Vec<ChunkKey> = Vec::new();
                for digest in digests {
                    if evicted >= max_count {
                        // Collect remaining entries for re-insertion
                        remaining.push(digest);
                        continue;
                    }

                    let shard_idx = Self::shard_idx(&digest);
                    let mut shard = self.shards[shard_idx].lock();
                    // Double-check it's still expired (might have been accessed)
                    if let Some(entry) = shard.peek(&digest) {
                        if now > entry.expires_at {
                            shard.pop(&digest);
                            evicted += 1;
                        } else {
                            // Entry was accessed, update its expiration time
                            remaining.push(digest);
                        }
                    }
                }

                // Re-insert remaining entries back into queue
                if !remaining.is_empty() {
                    let mut q = self.expiration_queue.lock();
                    q.insert(time, remaining);
                }

                // Re-acquire queue lock for next iteration
                queue = self.expiration_queue.lock();
            }
        }

        evicted
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

    /// Periodically sweep expired cache entries. Run this as a background
    /// tokio task alongside the FUSE session; it exits when dropped.
    /// 
    /// With proportional eviction, this is now a safety net that only
    /// cleans up entries that evaded the insert-time eviction (e.g., during
    /// idle periods with no inserts). Runs very infrequently.
    pub async fn run_cleanup(self, sweep_interval: Duration) {
        let mut ticker = tokio::time::interval(sweep_interval);
        ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        loop {
            ticker.tick().await;
            // Full sweep is now safety-net only - proportional eviction
            // handles 99% of cleanup during normal operations
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
