use std::cmp::min;
use std::io::{Read, Seek, SeekFrom};
use std::pin::Pin;
use std::sync::{Arc, Mutex};
use std::task::Context;

use anyhow::{bail, format_err, Error};
use pxar::accessor::{MaybeReady, ReadAt, ReadAtOperation};

use pbs_datastore::dynamic_index::DynamicIndexReader;
use pbs_datastore::index::IndexFile;
use pbs_datastore::read_chunk::ReadChunk;

struct EphemeralChunk {
    start: u64,
    end: u64,
    data: Vec<u8>,
}

impl EphemeralChunk {
    #[inline]
    fn contains(&self, offset: u64) -> bool {
        offset >= self.start && offset < self.end
    }

    #[inline]
    fn slice_from(&self, offset: u64) -> &[u8] {
        let off = (offset - self.start) as usize;
        &self.data[off..]
    }
}

pub struct DirectDynamicReader<S: ReadChunk> {
    store: S,
    index: DynamicIndexReader,
    archive_size: u64,
    read_offset: u64,
    cur_chunk_idx: Option<usize>,
    cur_chunk: Option<EphemeralChunk>,
}

impl<S: ReadChunk> DirectDynamicReader<S> {
    pub fn new(index: DynamicIndexReader, store: S) -> Self {
        let archive_size = index.index_bytes();
        Self {
            store,
            index,
            archive_size,
            read_offset: 0,
            cur_chunk_idx: None,
            cur_chunk: None,
        }
    }

    pub fn archive_size(&self) -> u64 {
        self.archive_size
    }

    #[inline]
    fn locate_chunk_index(&self, offset: u64) -> Result<usize, Error> {
        if self.index.index().is_empty() {
            bail!("empty dynamic index");
        }
        let end_idx = self.index.index().len() - 1;
        let end = self.index.chunk_end(end_idx);
        self.index.binary_search(0, 0, end_idx, end, offset)
    }

    fn load_chunk(&mut self, idx: usize) -> Result<EphemeralChunk, Error> {
        let info = self
            .index
            .chunk_info(idx)
            .ok_or_else(|| format_err!("chunk index out of range"))?;

        let data = self.store.read_chunk(&info.digest)?;

        let start = info.range.start;
        let end = info.range.end;

        if data.len() as u64 != end - start {
            bail!(
                "read chunk with wrong size ({} != {})",
                data.len(),
                end - start
            );
        }

        Ok(EphemeralChunk { start, end, data })
    }

    fn ensure_chunk_for_offset(&mut self, offset: u64) -> Result<(), Error> {
        if offset == self.archive_size {
            self.cur_chunk = None;
            self.cur_chunk_idx = None;
            return Ok(());
        }

        if let Some(c) = self.cur_chunk.as_ref() {
            if c.contains(offset) {
                return Ok(());
            }
        }

        if let Some(idx) = self.cur_chunk_idx {
            if let Some(c) = self.cur_chunk.as_ref() {
                if offset >= c.end {
                    let next_idx = idx + 1;
                    if next_idx < self.index.index().len() {
                        let next_end = self.index.chunk_end(next_idx);
                        if offset < next_end {
                            let chunk = self.load_chunk(next_idx)?;
                            self.cur_chunk_idx = Some(next_idx);
                            self.cur_chunk = Some(chunk);
                            return Ok(());
                        }
                    }
                }
            }
        }

        let idx = self.locate_chunk_index(offset)?;
        let chunk = self.load_chunk(idx)?;
        self.cur_chunk_idx = Some(idx);
        self.cur_chunk = Some(chunk);
        Ok(())
    }

    fn buffered_slice_from(&mut self, offset: u64) -> Result<&[u8], Error> {
        if offset == self.archive_size {
            return Ok(&[]);
        }
        self.ensure_chunk_for_offset(offset)?;
        let c = self
            .cur_chunk
            .as_ref()
            .ok_or_else(|| format_err!("no current chunk after ensure"))?;
        Ok(c.slice_from(offset))
    }
}

impl<S: ReadChunk> Read for DirectDynamicReader<S> {
    fn read(&mut self, out: &mut [u8]) -> Result<usize, std::io::Error> {
        use std::io::{Error, ErrorKind};

        if out.is_empty() {
            return Ok(0);
        }

        let mut total = 0usize;
        let mut offset = self.read_offset;

        while total < out.len() {
            let chunk_slice = match self.buffered_slice_from(offset) {
                Ok(s) => s,
                Err(err) => return Err(Error::new(ErrorKind::Other, err.to_string())),
            };

            if chunk_slice.is_empty() {
                break;
            }

            let n = min(chunk_slice.len(), out.len() - total);
            out[total..total + n].copy_from_slice(&chunk_slice[..n]);

            total += n;
            offset += n as u64;
        }

        self.read_offset = offset;
        Ok(total)
    }
}

impl<S: ReadChunk> Seek for DirectDynamicReader<S> {
    fn seek(&mut self, pos: SeekFrom) -> Result<u64, std::io::Error> {
        use std::io::{Error, ErrorKind};
        let new_offset = match pos {
            SeekFrom::Start(s) => s as i64,
            SeekFrom::End(delta) => (self.archive_size as i64) + delta,
            SeekFrom::Current(delta) => (self.read_offset as i64) + delta,
        };
        if new_offset < 0 || new_offset > (self.archive_size as i64) {
            return Err(Error::new(
                ErrorKind::Other,
                format!(
                    "seek is out of range {} ([0..{}])",
                    new_offset, self.archive_size
                ),
            ));
        }
        self.read_offset = new_offset as u64;

        if let Some(c) = self.cur_chunk.as_ref() {
            if !c.contains(self.read_offset) {
                self.cur_chunk = None;
                self.cur_chunk_idx = None;
            }
        }

        Ok(self.read_offset)
    }
}

#[derive(Clone)]
pub struct DirectLocalDynamicReadAt<R: ReadChunk> {
    inner: Arc<Mutex<DirectDynamicReader<R>>>,
}

impl<R: ReadChunk> DirectLocalDynamicReadAt<R> {
    pub fn new(inner: DirectDynamicReader<R>) -> Self {
        Self {
            inner: Arc::new(Mutex::new(inner)),
        }
    }
}

impl<R: ReadChunk> ReadAt for DirectLocalDynamicReadAt<R> {
    fn start_read_at<'a>(
        self: Pin<&'a Self>,
        _cx: &mut Context,
        buf: &'a mut [u8],
        offset: u64,
    ) -> MaybeReady<std::io::Result<usize>, ReadAtOperation<'a>> {
        MaybeReady::Ready(tokio::task::block_in_place(move || {
            use std::io::{Read, Seek, SeekFrom};
            let mut reader = self.inner.lock().unwrap();
            reader.seek(SeekFrom::Start(offset))?;
            reader.read(buf)
        }))
    }

    fn poll_complete<'a>(
        self: Pin<&'a Self>,
        _op: ReadAtOperation<'a>,
    ) -> MaybeReady<std::io::Result<usize>, ReadAtOperation<'a>> {
        panic!("DirectLocalDynamicReadAt::start_read_at returned Pending");
    }
}
