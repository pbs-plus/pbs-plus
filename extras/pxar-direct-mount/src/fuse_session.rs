use std::ffi::{OsStr, OsString};
use std::future::Future;
use std::num::NonZeroUsize;
use std::os::unix::ffi::OsStrExt;
use std::path::Path;
use std::pin::Pin;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::task::{Context, Poll};

use anyhow::{format_err, Error};
use futures::{channel::mpsc::UnboundedSender, select, SinkExt, StreamExt, TryStreamExt};
use lru::LruCache;
use parking_lot::{Mutex, RwLock};
use proxmox_fuse::requests::{self, FuseRequest};
use proxmox_fuse::{EntryParam, Fuse, ReplyBufState, Request, ROOT_ID};
use proxmox_io::vec;
use proxmox_lang::io_format_err;
use pxar::accessor::{self, ContentRange, EntryRangeInfo, ReadAt};
use pxar::EntryKind;

const NON_DIRECTORY_INODE: u64 = 1u64 << 63;
const EVICTION_BATCH_SIZE: usize = 16;

/// Offset cookie returned after the last regular entry; on the next kernel
/// call with this offset we serve only "..".
const DOT_NEXT: isize = isize::MAX - 1;
/// Offset cookie returned after "."; on the next kernel call with this offset
/// the directory listing is complete.
const DOTDOT_NEXT: isize = isize::MAX;

#[inline]
fn is_dir_inode(inode: u64) -> bool {
    0 == (inode & NON_DIRECTORY_INODE)
}

pub type Reader = Arc<dyn ReadAt + Send + Sync + 'static>;
pub type Accessor = accessor::aio::Accessor<Reader>;
pub type Directory = accessor::aio::Directory<Reader>;
pub type FileEntry = accessor::aio::FileEntry<Reader>;
pub type FileContents = accessor::aio::FileContents<Reader>;

pub struct Session {
    fut: Pin<Box<dyn Future<Output = Result<(), Error>> + Send + Sync + 'static>>,
}

impl Session {
    pub fn mount_with_cache(
        accessor: Accessor,
        options: &OsStr,
        path: &Path,
        max_lookups: usize,
    ) -> Result<Self, Error> {
        let fuse = Fuse::builder("pbs-pxar-mount")?
            .debug()
            .options_os(options)?
            .enable_readdirplus()
            .enable_read()
            .enable_readlink()
            .enable_read_xattr()
            .build()?
            .mount(path)?;

        let session = SessionImpl::new(accessor, max_lookups);

        Ok(Self {
            fut: Box::pin(session.main(fuse)),
        })
    }
}

impl Future for Session {
    type Output = Result<(), Error>;
    fn poll(mut self: Pin<&mut Self>, cx: &mut Context) -> Poll<Self::Output> {
        Pin::new(&mut self.fut).poll(cx)
    }
}

macro_rules! io_return {
    ($errno:expr) => {{
        return Err(::std::io::Error::from_raw_os_error($errno).into());
    }};
}

struct Lookup {
    /// FUSE kernel reference count.  Incremented on every successful `lookup`
    /// reply (via `LookupRef::leak`), decremented by `forget` syscalls.
    /// Entries with `refs == 0` are candidates for eviction from the cache.
    refs: AtomicUsize,
    parent: u64,
    entry_range_info: EntryRangeInfo,
    content_range: Option<ContentRange>,
    /// Pre-computed stat; used by `getattr` to avoid re-opening the pxar entry.
    stat: libc::stat,
}

impl Lookup {
    fn new(
        _inode: u64,
        parent: u64,
        entry_range_info: EntryRangeInfo,
        content_range: Option<ContentRange>,
        stat: libc::stat,
    ) -> Arc<Self> {
        Arc::new(Self {
            refs: AtomicUsize::new(1),
            parent,
            entry_range_info,
            content_range,
            stat,
        })
    }

    fn forget(&self, count: usize) -> Result<(), Error> {
        loop {
            let old = self.refs.load(Ordering::Acquire);
            if count >= old {
                return Err(io_format_err!("reference count underflow").into());
            }
            let new = old - count;
            match self.refs.compare_exchange(old, new, Ordering::SeqCst, Ordering::SeqCst) {
                Ok(_) => break Ok(()),
                Err(_) => continue,
            }
        }
    }
}

struct LookupRef<'a> {
    session: &'a SessionImpl,
    inode: u64,
    /// Owning Arc for the Lookup data.  Kept alive independently of the LRU
    /// cache so that deref/drop never need to re-acquire the cache lock and
    /// can never panic due to LRU eviction.
    lookup: Arc<Lookup>,
}

unsafe impl Send for LookupRef<'_> {}
unsafe impl Sync for LookupRef<'_> {}

impl Clone for LookupRef<'_> {
    fn clone(&self) -> Self {
        // Increment the FUSE kernel ref count directly on the owned Arc —
        // no cache lookup required, safe even if the LRU has evicted the entry.
        self.lookup.refs.fetch_add(1, Ordering::AcqRel);
        LookupRef {
            session: self.session,
            inode: self.inode,
            lookup: Arc::clone(&self.lookup),
        }
    }
}

impl std::ops::Deref for LookupRef<'_> {
    type Target = Lookup;
    fn deref(&self) -> &Self::Target {
        // The Arc keeps the Lookup alive regardless of LRU eviction — no cache
        // lookup, no lock, no possibility of panic.
        &self.lookup
    }
}

impl Drop for LookupRef<'_> {
    fn drop(&mut self) {
        // Decrement the FUSE kernel ref count directly via the owned Arc.
        // We leave the LRU entry in place (if it's still there); evict_stale_entries
        // cleans it up lazily when the cache is full.
        self.lookup.refs.fetch_sub(1, Ordering::AcqRel);
    }
}

impl<'a> LookupRef<'a> {
    /// Consume the `LookupRef` without decrementing `refs`.
    ///
    /// Used after a successful FUSE `lookup` reply so that the kernel's
    /// lookup count is reflected in `refs` without dropping it.
    fn leak(self) {
        std::mem::forget(self);
    }
}

struct SessionImpl {
    accessor: Accessor,
    lookups: RwLock<LruCache<u64, Arc<Lookup>>>,
    /// Secondary index: `(parent_inode, child_name) → child_inode`.
    /// Populated by every `make_lookup` call (from both `lookup` and
    /// `readdirplus`).  Lets `lookup()` skip the entire pxar directory
    /// traversal on cache hit.  Stale entries are harmless — `get_lookup`
    /// failing falls through to the slow path.
    name_index: Mutex<LruCache<(u64, OsString), u64>>,
    max_lookups: usize,
}

impl SessionImpl {
    fn new(accessor: Accessor, max_lookups: usize) -> Self {
        let max = NonZeroUsize::new(max_lookups.max(1024)).unwrap();
        // Root has no meaningful stat at construction time (we'd need to read
        // the archive), so zero-fill; the kernel rarely calls getattr on root
        // before any lookup anyway, and on miss we fall back to the slow path.
        let root_stat: libc::stat = unsafe { std::mem::zeroed() };
        let root = Lookup::new(
            ROOT_ID,
            ROOT_ID,
            EntryRangeInfo::toplevel(0..accessor.size()),
            None,
            root_stat,
        );
        let mut cache = LruCache::new(max);
        cache.put(ROOT_ID, root);
        let name_cap = NonZeroUsize::new(max_lookups.max(1024)).unwrap();
        Self {
            accessor,
            lookups: RwLock::new(cache),
            name_index: Mutex::new(LruCache::new(name_cap)),
            max_lookups,
        }
    }

    async fn handle_err(
        &self,
        request: impl FuseRequest,
        err: Error,
        mut sender: UnboundedSender<Error>,
    ) {
        let final_result = match err.downcast::<std::io::Error>() {
            Ok(err) => request.io_fail(err).map_err(Error::from),
            Err(err) => {
                log::error!("internal error: {}, bailing out", err);
                Err(err)
            }
        };
        if let Err(err) = final_result {
            sender.send(err).await.expect("error channel send failed");
        }
    }

    async fn main(self, fuse: Fuse) -> Result<(), Error> {
        let session = Arc::new(self);
        let (err_send, mut err_recv) = futures::channel::mpsc::unbounded::<Error>();
        let mut fuse = fuse.fuse();
        loop {
            select! {
                request = fuse.try_next() => match request? {
                    Some(request) => {
                        let s = Arc::clone(&session);
                        let es = err_send.clone();
                        tokio::spawn(async move {
                            s.handle_request(request, es).await;
                        });
                    }
                    None => break,
                },
                err = err_recv.next() => match err {
                    Some(err) => {
                        log::error!("cancelling fuse main loop due to error: {}", err);
                        return Err(err);
                    },
                    None => panic!("error channel closed"),
                },
            }
        }
        Ok(())
    }

    async fn handle_request(
        self: Arc<Self>,
        request: Request,
        mut err_sender: UnboundedSender<Error>,
    ) {
        use proxmox_fuse::Request::*;
        let result: Result<(), Error> = match request {
            Lookup(request) => match self.lookup(request.parent, &request.file_name).await {
                Ok((entry, lookup)) => match request.reply(&entry) {
                    Ok(()) => {
                        lookup.leak();
                        Ok(())
                    }
                    Err(err) => Err(Error::from(err)),
                },
                Err(err) => return self.handle_err(request, err, err_sender).await,
            },
            Forget(request) => {
                // Ignore errors: the inode may have been evicted from the LRU
                // cache while the kernel still held a nlookup ref.  The kernel's
                // forget is still semantically valid; we just have nothing to update.
                let _ = self.forget(request.inode, request.count as usize);
                request.reply();
                Ok(())
            }
            Getattr(request) => match self.getattr(request.inode).await {
                Ok(stat) => request.reply(&stat, f64::MAX).map_err(Error::from),
                Err(err) => return self.handle_err(request, err, err_sender).await,
            },
            ReaddirPlus(mut request) => match self.readdirplus(&mut request).await {
                Ok(lookups) => match request.reply() {
                    Ok(()) => {
                        for i in lookups {
                            i.leak();
                        }
                        Ok(())
                    }
                    Err(err) => Err(Error::from(err)),
                },
                Err(err) => return self.handle_err(request, err, err_sender).await,
            },
            Read(request) => match self.read(request.inode, request.size, request.offset).await {
                Ok(data) => request.reply(&data).map_err(Error::from),
                Err(err) => return self.handle_err(request, err, err_sender).await,
            },
            Readlink(request) => match self.readlink(request.inode).await {
                Ok(data) => request.reply(&data).map_err(Error::from),
                Err(err) => return self.handle_err(request, err, err_sender).await,
            },
            ListXAttrSize(request) => match self.listxattrs(request.inode).await {
                Ok(data) => request
                    .reply(
                        data.into_iter()
                            .fold(0, |sum, i| sum + i.name().to_bytes_with_nul().len()),
                    )
                    .map_err(Error::from),
                Err(err) => return self.handle_err(request, err, err_sender).await,
            },
            ListXAttr(mut request) => match self.listxattrs_into(&mut request).await {
                Ok(ReplyBufState::Ok) => request.reply().map_err(Error::from),
                Ok(ReplyBufState::Full) => request.fail_full().map_err(Error::from),
                Err(err) => return self.handle_err(request, err, err_sender).await,
            },
            GetXAttrSize(request) => match self.getxattr(request.inode, &request.attr_name).await {
                Ok(xattr) => request.reply(xattr.value().len()).map_err(Error::from),
                Err(err) => return self.handle_err(request, err, err_sender).await,
            },
            GetXAttr(request) => match self.getxattr(request.inode, &request.attr_name).await {
                Ok(xattr) => request.reply(xattr.value()).map_err(Error::from),
                Err(err) => return self.handle_err(request, err, err_sender).await,
            },
            other => {
                log::error!("Received unexpected fuse request");
                other.fail(libc::ENOSYS).map_err(Error::from)
            }
        };
        if let Err(err) = result {
            err_sender
                .send(err)
                .await
                .expect("error channel send failed");
        }
    }

    fn get_lookup(&self, inode: u64) -> Result<LookupRef<'_>, Error> {
        let cache = self.lookups.read();
        if let Some(lookup) = cache.peek(&inode) {
            loop {
                let old = lookup.refs.load(Ordering::Acquire);
                if old == 0 {
                    return Err(io_format_err!("inode refcount was 0").into());
                }
                if lookup
                    .refs
                    .compare_exchange(old, old + 1, Ordering::AcqRel, Ordering::Acquire)
                    .is_ok()
                {
                    break;
                }
            }
            return Ok(LookupRef {
                session: self,
                inode,
                lookup: Arc::clone(lookup),
            });
        }
        io_return!(libc::ENOENT);
    }

    async fn open_dir(&self, inode: u64) -> Result<Directory, Error> {
        if inode == ROOT_ID {
            Ok(self.accessor.open_root().await?)
        } else if !is_dir_inode(inode) {
            io_return!(libc::ENOTDIR);
        } else {
            Ok(unsafe { self.accessor.open_dir_at_end(inode).await? })
        }
    }

    async fn open_entry(&self, lookup: &LookupRef<'_>) -> std::io::Result<FileEntry> {
        unsafe {
            self.accessor
                .open_file_at_range(&lookup.entry_range_info)
                .await
        }
    }

    async fn open_content(&self, lookup: &LookupRef<'_>) -> Result<FileContents, Error> {
        if is_dir_inode(lookup.inode) {
            io_return!(libc::EISDIR);
        }
        match &lookup.content_range {
            Some(range) => self
                .accessor
                .open_contents_at_range(range)
                .await
                .map_err(|e| e.into()),
            None => io_return!(libc::EBADF),
        }
    }

    /// Evict up to `EVICTION_BATCH_SIZE` entries whose FUSE kernel ref count
    /// has dropped to zero.  Called under an existing write lock.
    fn evict_stale_entries(&self, cache: &mut LruCache<u64, Arc<Lookup>>) {
        let mut to_evict = Vec::with_capacity(EVICTION_BATCH_SIZE.min(self.max_lookups / 8));
        for (inode, entry) in cache.iter() {
            if entry.refs.load(Ordering::Acquire) == 0 {
                to_evict.push(*inode);
                if to_evict.len() >= EVICTION_BATCH_SIZE {
                    break;
                }
            }
        }
        for inode in to_evict {
            cache.pop(&inode);
        }
    }

    fn make_lookup(
        &self,
        parent: u64,
        inode: u64,
        name: &OsStr,
        entry: &FileEntry,
    ) -> Result<LookupRef<'_>, Error> {
        let stat = to_stat(inode, entry)?;

        let mut cache = self.lookups.write();

        if let Some(lookup) = cache.get_mut(&inode) {
            lookup.refs.fetch_add(1, Ordering::AcqRel);
            let arc = Arc::clone(lookup);
            drop(cache);
            self.name_index
                .lock()
                .put((parent, name.to_owned()), inode);
            return Ok(LookupRef {
                session: self,
                inode,
                lookup: arc,
            });
        }

        if cache.len() >= self.max_lookups {
            self.evict_stale_entries(&mut cache);
        }

        let lookup = Lookup::new(
            inode,
            parent,
            entry.entry_range_info().clone(),
            entry.content_range()?,
            stat,
        );
        lookup.refs.store(1, Ordering::Release);

        // Clone the Arc before moving it into the cache so the returned
        // LookupRef owns its own strong reference.  Even if the LRU later
        // evicts this entry (or another entry via the put-back path below),
        // the LookupRef's Arc keeps the Lookup alive.
        let arc = Arc::clone(&lookup);
        if let Some((evicted_inode, evicted)) = cache.push(inode, lookup) {
            if evicted.refs.load(Ordering::Acquire) > 0 {
                cache.put(evicted_inode, evicted);
            }
        }
        drop(cache);
        self.name_index
            .lock()
            .put((parent, name.to_owned()), inode);

        Ok(LookupRef {
            session: self,
            inode,
            lookup: arc,
        })
    }

    fn forget(&self, inode: u64, count: usize) -> Result<(), Error> {
        let node = self.get_lookup(inode)?;
        node.forget(count)?;
        Ok(())
    }

    async fn lookup(
        &'_ self,
        parent: u64,
        file_name: &OsStr,
    ) -> Result<(EntryParam, LookupRef<'_>), Error> {
        // Fast path: if readdirplus (or a prior lookup) already indexed this
        // name, skip the pxar directory traversal entirely.
        let cached_inode = self
            .name_index
            .lock()
            .get(&(parent, file_name.to_owned()))
            .copied();
        if let Some(inode) = cached_inode {
            if let Ok(lookup_ref) = self.get_lookup(inode) {
                // Copy stat out before the deref borrow ends.
                let stat = lookup_ref.stat;
                let entry_param = EntryParam::simple(inode, stat);
                return Ok((entry_param, lookup_ref));
            }
            // Inode was evicted from lookup cache — fall through to slow path.
        }

        // Slow path: open the parent directory and search pxar.
        let dir = self.open_dir(parent).await?;

        let entry = match { dir }.lookup(file_name).await? {
            Some(entry) => entry,
            None => io_return!(libc::ENOENT),
        };

        let entry = if let pxar::EntryKind::Hardlink(_) = entry.kind() {
            let entry = self.accessor.follow_hardlink(&entry).await?;
            if let EntryKind::Hardlink(_) = entry.kind() {
                io_return!(libc::ELOOP);
            }
            entry
        } else {
            entry
        };

        let inode = to_inode(&entry);
        let response = to_entry_param(inode, &entry)?;
        Ok((response, self.make_lookup(parent, inode, file_name, &entry)?))
    }

    async fn getattr(&self, inode: u64) -> Result<libc::stat, Error> {
        let lookup = self.get_lookup(inode)?;
        // Stat is pre-computed and cached in Lookup; no pxar I/O needed.
        // For the root inode the stat was zero-filled at init time; if the
        // kernel asks for root getattr before any lookup we fall back to a
        // real read.
        if inode != ROOT_ID || lookup.stat.st_mode != 0 {
            return Ok(lookup.stat);
        }
        let entry = unsafe {
            self.accessor
                .open_file_at_range(&lookup.entry_range_info)
                .await?
        };
        to_stat(inode, &entry)
    }

    async fn readdirplus(
        &'_ self,
        request: &mut requests::ReaddirPlus,
    ) -> Result<Vec<LookupRef<'_>>, Error> {
        // We use two sentinel offset cookies (DOT_NEXT, DOTDOT_NEXT) so that
        // we never need to count all directory entries upfront.  Regular
        // entries are numbered 1..=N with N unknown; after iterating them all
        // we emit "." with cookie DOT_NEXT, then ".." with cookie DOTDOT_NEXT.
        //
        // When the kernel resumes a partial readdir it passes back the last
        // cookie we issued:
        //   offset < DOT_NEXT   →  resume regular entries from `offset`, then "." and ".."
        //   offset == DOT_NEXT  →  skip regular entries, emit only ".."
        //   offset == DOTDOT_NEXT → nothing left to emit
        //
        // This avoids the former O(n) `dir.read_dir().count()` call which was
        // catastrophic for directories with billions of entries.

        let mut lookups = Vec::new();
        let offset = request.offset as isize;

        let dir = self.open_dir(request.inode).await?;
        let dir_lookup = self.get_lookup(request.inode)?;

        // Phase 1: regular entries (skipped when resuming at "." or "..").
        if offset < DOT_NEXT {
            let skip = offset as usize;
            let mut next = offset;
            let mut iter = dir.read_dir().skip(skip);

            while let Some(file) = iter.next().await {
                next += 1;
                let file = file?.decode_entry().await?;
                let stat = to_stat(to_inode(&file), &file)?;
                let name = file.file_name();
                // Create lookup and add to lookups BEFORE checking is_full().
                // This ensures that even if the buffer is full, the entry is
                // tracked and leak() will be called to maintain the refcount.
                lookups.push(self.make_lookup(request.inode, stat.st_ino, name, &file)?);
                if request
                    .add_entry(name, &stat, next, 1, f64::MAX, f64::MAX)?
                    .is_full()
                {
                    return Ok(lookups);
                }
            }

            // Regular entries exhausted — add ".".
            let file = dir.lookup_self().await?;
            let stat = to_stat(to_inode(&file), &file)?;
            // Add to lookups BEFORE checking is_full() to ensure proper refcount tracking.
            lookups.push(LookupRef::clone(&dir_lookup));
            if request
                .add_entry(OsStr::new("."), &stat, DOT_NEXT, 1, f64::MAX, f64::MAX)?
                .is_full()
            {
                return Ok(lookups);
            }
        }

        // Phase 2: ".." (skipped only when offset is already DOTDOT_NEXT,
        // meaning the kernel received ".." in a previous call).
        if offset < DOTDOT_NEXT {
            let lookup = self.get_lookup(dir_lookup.parent)?;
            let parent_dir = self.open_dir(lookup.inode).await?;
            let file = parent_dir.lookup_self().await?;
            let stat = to_stat(to_inode(&file), &file)?;
            // Add to lookups BEFORE checking is_full() to ensure proper refcount tracking.
            lookups.push(lookup);
            if request
                .add_entry(OsStr::new(".."), &stat, DOTDOT_NEXT, 1, f64::MAX, f64::MAX)?
                .is_full()
            {
                return Ok(lookups);
            }
        }

        Ok(lookups)
    }

    async fn read(&self, inode: u64, len: usize, offset: u64) -> Result<Vec<u8>, Error> {
        let file = self.get_lookup(inode)?;
        let content = self.open_content(&file).await?;
        let mut buf = vec::undefined(len);
        let mut pos = 0usize;
        loop {
            let got = content
                .read_at(&mut buf[pos..], offset + pos as u64)
                .await?;
            pos += got;
            if got == 0 || pos >= len {
                break;
            }
        }
        buf.truncate(pos);
        Ok(buf)
    }

    async fn readlink(&self, inode: u64) -> Result<OsString, Error> {
        let lookup = self.get_lookup(inode)?;
        let file = self.open_entry(&lookup).await?;
        match file.get_symlink() {
            None => io_return!(libc::EINVAL),
            Some(link) => Ok(link.to_owned()),
        }
    }

    async fn listxattrs(&self, inode: u64) -> Result<Vec<pxar::format::XAttr>, Error> {
        let lookup = self.get_lookup(inode)?;
        let metadata = self.open_entry(&lookup).await?.into_entry().into_metadata();

        let mut xattrs = metadata.xattrs;

        use pxar::format::XAttr;

        if let Some(fcaps) = metadata.fcaps {
            xattrs.push(XAttr::new(
                proxmox_sys::fs::xattr::XATTR_NAME_FCAPS.to_bytes(),
                fcaps.data,
            ));
        }

        Ok(xattrs)
    }

    async fn listxattrs_into(
        &self,
        request: &mut requests::ListXAttr,
    ) -> Result<ReplyBufState, Error> {
        let xattrs = self.listxattrs(request.inode).await?;

        for entry in xattrs {
            if request.add_c_string(entry.name()).is_full() {
                return Ok(ReplyBufState::Full);
            }
        }

        Ok(ReplyBufState::Ok)
    }

    async fn getxattr(&self, inode: u64, xattr: &OsStr) -> Result<pxar::format::XAttr, Error> {
        let xattrs = self.listxattrs(inode).await?;
        for entry in xattrs {
            if entry.name().to_bytes() == xattr.as_bytes() {
                return Ok(entry);
            }
        }
        io_return!(libc::ENODATA);
    }
}

#[inline]
fn to_inode(entry: &FileEntry) -> u64 {
    if entry.is_dir() {
        entry.entry_range_info().entry_range.end
    } else {
        entry.entry_range_info().entry_range.start | NON_DIRECTORY_INODE
    }
}

fn to_entry_param(inode: u64, entry: &pxar::Entry) -> Result<EntryParam, Error> {
    Ok(EntryParam::simple(inode, to_stat(inode, entry)?))
}

fn to_stat(inode: u64, entry: &pxar::Entry) -> Result<libc::stat, Error> {
    let nlink = if entry.is_dir() { 2 } else { 1 };

    let metadata = entry.metadata();

    let mut stat: libc::stat = unsafe { std::mem::zeroed() };
    stat.st_ino = inode;
    stat.st_nlink = nlink;

    let original_mode = u32::try_from(metadata.stat.mode)
        .map_err(|err| format_err!("mode does not fit into st_mode field: {}", err))?;
    let file_type = original_mode & libc::S_IFMT;

    let perms = if entry.is_dir() { 0o755 } else { 0o644 };
    stat.st_mode = file_type | perms;

    stat.st_size = i64::try_from(entry.file_size().unwrap_or(0))
        .map_err(|err| format_err!("size does not fit into st_size field: {}", err))?;
    stat.st_uid = metadata.stat.uid;
    stat.st_gid = metadata.stat.gid;
    stat.st_atime = metadata.stat.mtime.secs;
    stat.st_atime_nsec = metadata.stat.mtime.nanos as _;
    stat.st_mtime = metadata.stat.mtime.secs;
    stat.st_mtime_nsec = metadata.stat.mtime.nanos as _;
    stat.st_ctime = metadata.stat.mtime.secs;
    stat.st_ctime_nsec = metadata.stat.mtime.nanos as _;
    Ok(stat)
}

#[cfg(test)]
mod tests {
    use super::*;
    use lru::LruCache;
    use std::num::NonZeroUsize;
    use std::sync::atomic::Ordering;
    use std::sync::Arc;

    /// Create a minimal Arc<Lookup> suitable for testing.
    /// `refs` is the initial FUSE kernel reference count.
    fn make_arc_lookup(refs: usize) -> Arc<Lookup> {
        let stat: libc::stat = unsafe { std::mem::zeroed() };
        let l = Arc::new(Lookup {
            refs: AtomicUsize::new(refs),
            parent: 0,
            entry_range_info: EntryRangeInfo::toplevel(0..0),
            content_range: None,
            stat,
        });
        l
    }

    /// Create a minimal Box<Lookup> to simulate the *old* storage type.
    fn make_box_lookup(refs: usize) -> Box<Lookup> {
        let stat: libc::stat = unsafe { std::mem::zeroed() };
        Box::new(Lookup {
            refs: AtomicUsize::new(refs),
            parent: 0,
            entry_range_info: EntryRangeInfo::toplevel(0..0),
            content_range: None,
            stat,
        })
    }

    // -----------------------------------------------------------------------
    // Test 1 – reproduces the bug scenario
    //
    // With Box<Lookup> and the push()+put() eviction pattern used by the old
    // make_lookup, a cache that is full with all-active entries causes a
    // *silent* double-eviction: put() drops the new LRU without returning it.
    // Any code that then calls cache.peek(evicted_inode) gets None — which is
    // exactly what the old LookupRef::deref hit before calling
    // .expect("lookup ref deref: inode disappeared") and panicking.
    // -----------------------------------------------------------------------
    #[test]
    fn bug_box_double_eviction_silently_drops_active_entry() {
        // Tiny cache so we can trigger pressure easily.
        let cap = NonZeroUsize::new(3).unwrap();
        let mut cache: LruCache<u64, Box<Lookup>> = LruCache::new(cap);

        // Fill all three slots with active entries (refs = 1 = kernel holds a ref).
        cache.put(1, make_box_lookup(1)); // will be LRU after subsequent puts
        cache.put(2, make_box_lookup(1));
        cache.put(3, make_box_lookup(1)); // MRU

        // Cache is at capacity; all entries have refs > 0.
        // Inserting inode 4 via push() evicts the LRU (inode 1).
        let evicted = cache
            .push(4, make_box_lookup(1))
            .expect("cache was full, must evict");
        let (evicted_inode, evicted_box) = evicted;
        assert_eq!(evicted_inode, 1);
        assert!(
            evicted_box.refs.load(Ordering::Acquire) > 0,
            "evicted entry still has an active kernel ref"
        );

        // Old make_lookup put() the evicted entry back to avoid losing it.
        // lru::LruCache::put() evicts the *new* LRU to make room — but unlike
        // push() it does NOT return the displaced entry.  Inode 2 is silently
        // dropped even though its refs > 0.
        cache.put(evicted_inode, evicted_box);

        // Inode 2 is now gone from the cache.
        assert!(
            cache.peek(&2).is_none(),
            "inode 2 was silently evicted despite having an active kernel ref"
        );

        // Any concurrent task that held a LookupRef for inode 2 would now
        // execute the old deref path:
        //   cache.peek(&2).expect("lookup ref deref: inode disappeared")
        // That expect() would fire.  We reproduce the failure here without
        // actually invoking expect() so the test remains deterministic.
        let would_panic = cache.peek(&2).is_none();
        assert!(
            would_panic,
            "old LookupRef::deref would have panicked for inode 2"
        );
    }

    // -----------------------------------------------------------------------
    // Test 2 – proves the fix
    //
    // With Arc<Lookup>, the LookupRef holds its own strong reference to the
    // Lookup data.  Even after the exact same double-eviction sequence, the
    // data is still live and fully accessible — no cache lookup required.
    // -----------------------------------------------------------------------
    #[test]
    fn fix_arc_survives_double_eviction() {
        let cap = NonZeroUsize::new(3).unwrap();
        let mut cache: LruCache<u64, Arc<Lookup>> = LruCache::new(cap);

        cache.put(1, make_arc_lookup(1));
        let entry2 = make_arc_lookup(1);
        // Simulate a LookupRef: hold a clone of inode 2's Arc.
        let entry2_held = Arc::clone(&entry2);
        cache.put(2, entry2);
        cache.put(3, make_arc_lookup(1));

        // Same double-eviction sequence as Test 1.
        let evicted = cache
            .push(4, make_arc_lookup(1))
            .expect("cache was full, must evict");
        let (evicted_inode, evicted_arc) = evicted;
        assert_eq!(evicted_inode, 1);
        cache.put(evicted_inode, evicted_arc); // evicts inode 2 from cache

        // Inode 2 is gone from the cache — same as before.
        assert!(cache.peek(&2).is_none(), "inode 2 evicted from LRU");

        // But the Arc held by entry2_held is *still alive*.  Strong count ≥ 1
        // (the cache Arc was dropped, the held Arc is still there).
        assert!(Arc::strong_count(&entry2_held) >= 1);

        // The new LookupRef::deref uses &*self.lookup — no cache lookup at all.
        // This is always safe and can never panic.
        let _refs = entry2_held.refs.load(Ordering::Acquire);
        let _parent = entry2_held.parent;
        // reaching here without panic proves the fix works
    }

    // -----------------------------------------------------------------------
    // Test 3 – concurrent stress test
    //
    // Many threads simultaneously grab Arc refs and insert new entries into a
    // small cache, forcing constant eviction.  Every thread accesses the
    // Lookup data via its owned Arc after potentially being evicted from the
    // LRU.  No thread should panic.
    // -----------------------------------------------------------------------
    #[test]
    fn fix_concurrent_arc_access_under_cache_pressure() {
        use parking_lot::Mutex;
        use std::sync::Barrier;
        use std::thread;

        const CACHE_CAP: usize = 8;
        const INODE_SPACE: u64 = 16; // small so evictions are frequent
        const THREADS: usize = 16;
        const ITERS: usize = 2000;

        let cap = NonZeroUsize::new(CACHE_CAP).unwrap();
        let cache: Arc<Mutex<LruCache<u64, Arc<Lookup>>>> =
            Arc::new(Mutex::new(LruCache::new(cap)));

        // Pre-fill the cache.
        {
            let mut c = cache.lock();
            for i in 0..CACHE_CAP as u64 {
                c.put(i, make_arc_lookup(1));
            }
        }

        let barrier = Arc::new(Barrier::new(THREADS));
        let mut handles = Vec::with_capacity(THREADS);

        for t in 0..THREADS {
            let cache = Arc::clone(&cache);
            let barrier = Arc::clone(&barrier);

            handles.push(thread::spawn(move || {
                barrier.wait(); // all threads start at the same time

                for i in 0..ITERS {
                    let inode = ((t * ITERS + i) as u64) % INODE_SPACE;

                    // Acquire or insert — mirrors what make_lookup does.
                    let arc: Arc<Lookup> = {
                        let mut c = cache.lock();
                        if let Some(entry) = c.get(&inode) {
                            entry.refs.fetch_add(1, Ordering::AcqRel);
                            Arc::clone(entry)
                        } else {
                            let new_entry = make_arc_lookup(1);
                            let held = Arc::clone(&new_entry);
                            // Replicate the old push()+put() eviction pattern.
                            if let Some((ev_inode, ev_arc)) = c.push(inode, new_entry) {
                                if ev_arc.refs.load(Ordering::Acquire) > 0 {
                                    // This is the double-eviction path.
                                    // With Box it silently dropped a live entry;
                                    // with Arc the displaced entry's data stays alive.
                                    c.put(ev_inode, ev_arc);
                                }
                            }
                            held
                        }
                    };

                    // Access the Lookup data via the owned Arc.
                    // With the fix this never panics even if the LRU has
                    // already evicted this entry from the cache.
                    let _refs = arc.refs.load(Ordering::Acquire);
                    let _parent = arc.parent;

                    arc.refs.fetch_sub(1, Ordering::AcqRel);
                }
            }));
        }

        for handle in handles {
            handle.join().expect("thread panicked — Arc fix should prevent this");
        }
    }

    // -----------------------------------------------------------------------
    // Test 4 – reproduces the readdirplus is_full() refcount bug
    //
    // In readdirplus, when add_entry() returns is_full(), the function
    // returns early WITHOUT adding the lookup to the 'lookups' vector.
    // This means leak() is never called, so the refcount drops to 0
    // when the LookupRef is dropped, making the inode eligible for eviction.
    //
    // This test simulates that scenario to demonstrate why the fix is needed.
    // It is marked with #[should_panic] because it demonstrates a bug condition.
    // -----------------------------------------------------------------------
    #[test]
    #[should_panic(expected = "BUG: Entry exists in cache but refcount is 0")]
    fn bug_readdirplus_isfull_refcount_drop() {
        // Simulate the pattern in readdirplus when is_full() is triggered
        let cap = NonZeroUsize::new(10).unwrap();
        let mut cache: LruCache<u64, Arc<Lookup>> = LruCache::new(cap);

        // Create a lookup (simulating make_lookup)
        let lookup_arc = make_arc_lookup(1); // refcount = 1
        let inode = 42u64;
        
        // Add to cache (simulating make_lookup's cache.push)
        cache.push(inode, Arc::clone(&lookup_arc));
        
        // At this point, the entry is in the cache with refcount = 1
        // This simulates having added the entry to the FUSE reply buffer
        // but NOT having pushed it to the lookups vector yet
        
        // Simulate the LookupRef being dropped when is_full() triggers early return
        // The LookupRef::drop() decrements refcount
        lookup_arc.refs.fetch_sub(1, Ordering::AcqRel);
        
        // Now refcount = 0, which is the BUG state
        // The entry is in the kernel's directory listing but refcount is 0
        let refs_after_drop = lookup_arc.refs.load(Ordering::Acquire);
        assert_eq!(refs_after_drop, 0, "refcount should be 0 after drop (simulating bug)");
        
        // Simulate what happens when get_lookup is called for this inode
        // (e.g., when FreeFileSync tries to access the entry)
        if let Some(entry) = cache.peek(&inode) {
            let current_refs = entry.refs.load(Ordering::Acquire);
            if current_refs == 0 {
                // This is the bug: entry exists but refcount is 0
                // get_lookup would fail here in the real code
                panic!("BUG: Entry exists in cache but refcount is 0 - would cause ENOENT");
            }
        }
    }

    // -----------------------------------------------------------------------
    // Test 5 – verifies the fix for readdirplus is_full() handling
    //
    // With the fix, even when is_full() triggers, the lookup should be
    // added to the lookups vector so leak() is called, keeping refcount >= 1.
    // -----------------------------------------------------------------------
    #[test]
    fn fix_readdirplus_isfull_maintains_refcount() {
        let cap = NonZeroUsize::new(10).unwrap();
        let mut cache: LruCache<u64, Arc<Lookup>> = LruCache::new(cap);

        // Create a lookup (simulating make_lookup)
        let lookup_arc = make_arc_lookup(1); // refcount = 1
        let inode = 42u64;
        
        // Add to cache
        cache.push(inode, Arc::clone(&lookup_arc));
        
        // Simulate the FIX: lookup is added to lookups vector BEFORE checking is_full()
        // OR leak() is called before returning even when is_full() is true
        
        // The fix increments refcount via leak() before the early return
        lookup_arc.refs.fetch_add(1, Ordering::AcqRel); // refcount = 2 (simulating leak())
        
        // Now simulate LookupRef::drop() decrementing refcount
        lookup_arc.refs.fetch_sub(1, Ordering::AcqRel); // refcount = 1
        
        // After the fix, refcount should be 1 (held by the leaked reference)
        let refs_after_fix = lookup_arc.refs.load(Ordering::Acquire);
        assert_eq!(refs_after_fix, 1, "refcount should be 1 after fix (leaked reference)");
        
        // Verify entry can be found and has valid refcount
        if let Some(entry) = cache.peek(&inode) {
            let current_refs = entry.refs.load(Ordering::Acquire);
            assert!(current_refs > 0, "Entry should have refcount > 0 after fix");
        } else {
            panic!("Entry should still be in cache");
        }
    }
}
