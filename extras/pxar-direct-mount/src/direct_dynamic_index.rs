use std::cmp::min;
use std::pin::Pin;
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
/// Sequential-read optimisation lives entirely inside `read_at_range` as a
/// local variable: after finding the first chunk with a single O(log n) binary
/// search, each subsequent chunk is reached with a plain `chunk_idx += 1` — no
/// further searching, no shared state, no false sharing.
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

        let mut total = 0usize;
        let mut pos = offset;

        // Binary search ONCE to find the chunk that contains `offset`.
        // All subsequent chunks are reached by incrementing `chunk_idx`
        // directly — sequential reads (the dominant pattern under SMB copy)
        // never call `locate_chunk_index` again.
        let mut chunk_idx = self.locate_chunk_index(pos)?;

        loop {
            let info = self
                .index
                .chunk_info(chunk_idx)
                .ok_or_else(|| format_err!("chunk index out of range"))?;

            // Arc clone from the sharded LRU — no extra Vec copy.
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

            if total >= buf.len() || pos >= self.archive_size {
                break;
            }

            // Advance to the next chunk without any search.
            chunk_idx += 1;
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
