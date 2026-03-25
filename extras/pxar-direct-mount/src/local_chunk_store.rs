use std::fs::File;
use std::num::NonZeroUsize;
use std::path::PathBuf;
use std::sync::Arc;

use anyhow::Error;
use lru::LruCache;
use parking_lot::Mutex;
use pbs_datastore::data_blob::DataBlob;
use pbs_datastore::read_chunk::ReadChunk;
use pbs_tools::crypt_config::CryptConfig;
use sha2::{Digest, Sha256};

const DEFAULT_MAX_CACHED_CHUNKS: usize = 4096;

// 64 independent shards: reduces write-lock contention ~64x under concurrent load.
// Must be a power of 2.
const SHARD_COUNT: usize = 64;

pub struct ChunkCacheConfig {
    pub max_entries: usize,
}

impl Default for ChunkCacheConfig {
    fn default() -> Self {
        Self {
            max_entries: DEFAULT_MAX_CACHED_CHUNKS,
        }
    }
}

type ChunkKey = [u8; 32];

/// Sharded LRU cache for decoded chunks.
///
/// Sharding by the first byte of the SHA-256 digest (which is uniformly
/// distributed) gives ~64x reduction in lock contention compared to a single
/// global lock.  Each shard is an independent `Mutex<LruCache>`, so concurrent
/// readers hitting different shards never block each other.
struct ShardedChunkCache {
    shards: Vec<Mutex<LruCache<ChunkKey, Arc<Vec<u8>>>>>,
}

impl ShardedChunkCache {
    fn new(total_capacity: usize) -> Self {
        let per_shard = NonZeroUsize::new((total_capacity / SHARD_COUNT).max(4)).unwrap();
        let shards = (0..SHARD_COUNT)
            .map(|_| Mutex::new(LruCache::new(per_shard)))
            .collect();
        Self { shards }
    }

    #[inline]
    fn shard_idx(digest: &ChunkKey) -> usize {
        // SHA-256 is pseudorandom, so the first byte distributes uniformly
        // across all 64 shards when masked with SHARD_COUNT - 1.
        (digest[0] as usize) & (SHARD_COUNT - 1)
    }

    fn get(&self, digest: &ChunkKey) -> Option<Arc<Vec<u8>>> {
        self.shards[Self::shard_idx(digest)]
            .lock()
            .get(digest)
            .map(Arc::clone)
    }

    fn put(&self, digest: ChunkKey, data: Arc<Vec<u8>>) {
        self.shards[Self::shard_idx(&digest)]
            .lock()
            .put(digest, data);
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
            )),
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
}

impl ChunkCacheStore for LocalChunkStore {
    fn read_chunk_cached(&self, digest: &ChunkKey) -> Result<Arc<Vec<u8>>, Error> {
        self.load_and_decode(digest)
    }
}
