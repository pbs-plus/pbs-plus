use std::cmp::min;
use std::pin::Pin;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::task::Context;

use anyhow::{bail, format_err, Error};
use pxar::accessor::{MaybeReady, ReadAt, ReadAtOperation};

use pbs_datastore::dynamic_index::DynamicIndexReader;
use pbs_datastore::index::IndexFile;

use crate::local_chunk_store::ChunkCacheStore;

/// Concurrent, lock-free-read chunk reader over a dynamic index.
///
/// All caching is delegated to the underlying `ChunkCacheStore` (which uses a
/// sharded LRU).  This reader adds only a single atomic `chunk_hint` to
/// accelerate the common case of sequential reads: if the requested offset
/// falls in the previously used chunk (or the next one), the binary search is
/// skipped entirely.
pub struct ConcurrentDynamicReader<S: ChunkCacheStore + Clone> {
    store: S,
    index: DynamicIndexReader,
    archive_size: u64,
    /// Best-guess chunk index for the next read (relaxed atomic, so stale under
    /// concurrent access — that just falls back to binary search, which is safe).
    chunk_hint: AtomicUsize,
}

impl<S: ChunkCacheStore + Clone> ConcurrentDynamicReader<S> {
    pub fn new(index: DynamicIndexReader, store: S) -> Self {
        let archive_size = index.index_bytes();
        Self {
            store,
            index,
            archive_size,
            chunk_hint: AtomicUsize::new(0),
        }
    }

    /// Locate the chunk index for `offset`.
    ///
    /// Fast path: the hint covers the offset, or the hint+1 chunk does (the
    /// common sequential case).  Slow path: binary search over the full index.
    #[inline]
    fn locate_chunk_index(&self, offset: u64) -> Result<usize, Error> {
        let n = self.index.index().len();
        if n == 0 {
            bail!("empty dynamic index");
        }

        let hint = self.chunk_hint.load(Ordering::Relaxed);

        if hint < n {
            let hint_end = self.index.chunk_end(hint);
            let hint_start = if hint == 0 {
                0
            } else {
                self.index.chunk_end(hint - 1)
            };

            if offset >= hint_start && offset < hint_end {
                return Ok(hint);
            }

            // Try the immediately following chunk (sequential read pattern).
            let next = hint + 1;
            if next < n {
                let next_end = self.index.chunk_end(next);
                if offset >= hint_end && offset < next_end {
                    self.chunk_hint.store(next, Ordering::Relaxed);
                    return Ok(next);
                }
            }
        }

        // General case: O(log n) binary search.
        let end_idx = n - 1;
        let end = self.index.chunk_end(end_idx);
        let idx = self.index.binary_search(0, 0, end_idx, end, offset)?;
        self.chunk_hint.store(idx, Ordering::Relaxed);
        Ok(idx)
    }

    fn read_at_range(&self, offset: u64, buf: &mut [u8]) -> Result<usize, Error> {
        if offset >= self.archive_size {
            return Ok(0);
        }

        let mut total = 0usize;
        let mut pos = offset;

        while total < buf.len() && pos < self.archive_size {
            let chunk_idx = self.locate_chunk_index(pos)?;
            let info = self
                .index
                .chunk_info(chunk_idx)
                .ok_or_else(|| format_err!("chunk index out of range"))?;

            // read_chunk_cached returns Arc<Vec<u8>> — no extra copy, just an
            // Arc clone from the shared sharded LRU in LocalChunkStore.
            let data = self.store.read_chunk_cached(&info.digest)?;

            let chunk_offset = (pos - info.range.start) as usize;
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
