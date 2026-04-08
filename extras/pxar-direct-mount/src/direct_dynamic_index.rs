use std::cmp::min;
use std::pin::Pin;
use std::sync::Arc;
use std::task::Context;

use anyhow::{bail, format_err, Error};
use pxar::accessor::{MaybeReady, ReadAt, ReadAtOperation};

use pbs_datastore::dynamic_index::DynamicIndexReader;
use pbs_datastore::index::IndexFile;

use crate::local_chunk_store::ChunkCacheStore;

/// Concurrent, fully-stateless chunk reader over a dynamic index.
///
/// The struct itself has NO mutable state — it is safe to share a single
/// `Arc<ConcurrentDynamicReader>` across any number of concurrent readers
/// without any locking or cache-line contention.
///
/// Parallel loading: for each `read_at_range` call, all chunks spanning the
/// requested byte range are identified up front. Chunks already in the cache
/// are reused immediately via `peek_cached`. Remaining cache-miss chunks are
/// loaded in parallel using `std::thread::scope`, so data starts flowing to
/// the FUSE buffer as soon as the first needed chunk is ready.
pub struct ConcurrentDynamicReader<S: ChunkCacheStore + Clone> {
    store: S,
    index: DynamicIndexReader,
    archive_size: u64,
}

impl<S: ChunkCacheStore + Clone> ConcurrentDynamicReader<S> {
    pub fn new(index: DynamicIndexReader, store: S) -> Self {
        let archive_size = index.index_bytes();
        Self {
            store,
            index,
            archive_size,
        }
    }

    /// O(log n) binary search — called at most once per `read_at_range` call.
    #[inline]
    fn locate_chunk_index(&self, offset: u64) -> Result<usize, Error> {
        let n = self.index.index().len();
        if n == 0 {
            bail!("empty dynamic index");
        }
        let end_idx = n - 1;
        let end = self.index.chunk_end(end_idx);
        self.index.binary_search(0, 0, end_idx, end, offset)
    }

    fn read_at_range(&self, offset: u64, buf: &mut [u8]) -> Result<usize, Error> {
        if offset >= self.archive_size {
            return Ok(0);
        }

        // ── Phase 1: collect chunk metadata for the entire read range ─────────
        // One binary search for the first chunk; subsequent chunks are adjacent.
        let first_idx = self.locate_chunk_index(offset)?;

        struct ChunkMeta {
            digest: [u8; 32],
            range_start: u64,
        }

        let mut chunks: Vec<ChunkMeta> = Vec::new();
        {
            let mut pos = offset;
            let mut remaining = buf.len();
            let mut chunk_idx = first_idx;

            loop {
                let info = self
                    .index
                    .chunk_info(chunk_idx)
                    .ok_or_else(|| format_err!("chunk index out of range"))?;

                let available = (info.range.end - pos) as usize;
                let to_copy = min(available, remaining);

                if to_copy == 0 {
                    break;
                }

                chunks.push(ChunkMeta {
                    digest: info.digest,
                    range_start: info.range.start,
                });

                pos += to_copy as u64;
                remaining -= to_copy;

                if remaining == 0 || pos >= self.archive_size {
                    break;
                }

                chunk_idx += 1;
            }
        }

        if chunks.is_empty() {
            return Ok(0);
        }

        // ── Phase 2: peek cache for all chunks ────────────────────────────────
        // Avoid holding any lock while doing I/O: collect Arc refs first.
        let mut loaded: Vec<Option<Arc<Vec<u8>>>> = chunks
            .iter()
            .map(|c| self.store.peek_cached(&c.digest))
            .collect();

        // ── Phase 3: parallel load for cache misses ───────────────────────────
        // Identify which chunks need disk I/O.
        let miss_indices: Vec<usize> = loaded
            .iter()
            .enumerate()
            .filter_map(|(i, v)| if v.is_none() { Some(i) } else { None })
            .collect();

        // Load cache misses — sequential to avoid thread spawning overhead.
        // Thread::scope per-read causes thread storms under heavy concurrent load
        // (FreeFileSync scanning). Sequential loading with fast NVMe is often
        // faster than thread creation overhead anyway.
        for &i in &miss_indices {
            loaded[i] = Some(self.store.read_chunk_cached(&chunks[i].digest)?);
        }

        // ── Phase 4: copy loaded chunks into the output buffer ────────────────
        let mut total = 0usize;
        let mut pos = offset;

        for (meta, data_opt) in chunks.iter().zip(loaded.iter()) {
            let data = data_opt.as_ref().expect("all chunks must be loaded by now");
            let chunk_offset = (pos - meta.range_start) as usize;
            let available = data.len().saturating_sub(chunk_offset);
            let to_copy = min(available, buf.len() - total);

            if to_copy == 0 {
                break;
            }

            buf[total..total + to_copy]
                .copy_from_slice(&data[chunk_offset..chunk_offset + to_copy]);
            total += to_copy;
            pos += to_copy as u64;
        }

        Ok(total)
    }
}

impl<S: ChunkCacheStore + Clone + Send + Sync + 'static> ReadAt for ConcurrentDynamicReader<S> {
    fn start_read_at<'a>(
        self: Pin<&'a Self>,
        _cx: &mut Context,
        buf: &'a mut [u8],
        offset: u64,
    ) -> MaybeReady<std::io::Result<usize>, ReadAtOperation<'a>> {
        MaybeReady::Ready(tokio::task::block_in_place(move || {
            self.read_at_range(offset, buf)
                .map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e))
        }))
    }

    fn poll_complete<'a>(
        self: Pin<&'a Self>,
        _op: ReadAtOperation<'a>,
    ) -> MaybeReady<std::io::Result<usize>, ReadAtOperation<'a>> {
        panic!("ConcurrentDynamicReader::start_read_at returned Pending");
    }
}

pub type ConcurrentLocalReader<S> = ConcurrentDynamicReader<S>;
