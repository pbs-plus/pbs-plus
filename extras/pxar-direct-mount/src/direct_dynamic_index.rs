use std::cmp::min;
use std::pin::Pin;
use std::task::Context;

use anyhow::{bail, format_err, Error};
use pxar::accessor::{MaybeReady, ReadAt, ReadAtOperation};

use pbs_datastore::dynamic_index::DynamicIndexReader;
use pbs_datastore::index::IndexFile;

use crate::local_chunk_store::LocalChunkStore;

pub struct ConcurrentDynamicReader {
    store: LocalChunkStore,
    index: DynamicIndexReader,
    archive_size: u64,
}

impl ConcurrentDynamicReader {
    pub fn new(index: DynamicIndexReader, store: LocalChunkStore) -> Self {
        let archive_size = index.index_bytes();
        Self {
            store,
            index,
            archive_size,
        }
    }

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
        if buf.is_empty() || offset >= self.archive_size {
            return Ok(0);
        }

        let first_idx = self.locate_chunk_index(offset)?;
        let first_info = self
            .index
            .chunk_info(first_idx)
            .ok_or_else(|| format_err!("chunk index out of range"))?;

        let data = self.store.load_and_decode(&first_info.digest)?;
        let chunk_offset = (offset - first_info.range.start) as usize;
        let first_copy = min(data.len().saturating_sub(chunk_offset), buf.len());
        buf[..first_copy].copy_from_slice(&data[chunk_offset..chunk_offset + first_copy]);

        if first_copy >= buf.len() || first_info.range.end >= self.archive_size {
            return Ok(first_copy);
        }

        let mut total = first_copy;
        let mut pos = offset + first_copy as u64;
        let mut chunk_idx = first_idx + 1;

        while total < buf.len() && pos < self.archive_size {
            let info = self
                .index
                .chunk_info(chunk_idx)
                .ok_or_else(|| format_err!("chunk index out of range"))?;

            let data = self.store.load_and_decode(&info.digest)?;
            let to_copy = min(data.len(), buf.len() - total);
            buf[total..total + to_copy].copy_from_slice(&data[..to_copy]);

            total += to_copy;
            pos += to_copy as u64;
            chunk_idx += 1;
        }

        Ok(total)
    }
}

impl ReadAt for ConcurrentDynamicReader {
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

pub type ConcurrentLocalReader = ConcurrentDynamicReader;
