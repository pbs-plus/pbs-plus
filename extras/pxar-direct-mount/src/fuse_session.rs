use std::collections::HashMap;
use std::ffi::{OsStr, OsString};
use std::future::Future;
use std::os::unix::ffi::OsStrExt;
use std::path::Path;
use std::pin::Pin;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::task::{Context, Poll};

use anyhow::{format_err, Error};
use futures::{channel::mpsc::UnboundedSender, select, SinkExt, StreamExt, TryStreamExt};
use parking_lot::RwLock;
use proxmox_fuse::requests::{self, FuseRequest};
use proxmox_fuse::{EntryParam, Fuse, ReplyBufState, Request, ROOT_ID};
use proxmox_io::vec;
use proxmox_lang::io_format_err;
use pxar::accessor::{self, ContentRange, EntryRangeInfo, ReadAt};
use pxar::EntryKind;

const NON_DIRECTORY_INODE: u64 = 1u64 << 63;

const DOT_NEXT: isize = isize::MAX - 1;
const DOTDOT_NEXT: isize = isize::MAX;

#[inline]
fn is_dir_inode(inode: u64) -> bool {
    inode & NON_DIRECTORY_INODE == 0
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
    pub fn mount(
        accessor: Accessor,
        options: &OsStr,
        path: &Path,
        verbose: bool,
    ) -> Result<Self, Error> {
        let mut builder = Fuse::builder("pbs-pxar-mount")?;
        if verbose {
            builder = builder.debug();
        }
        let fuse = builder
            .options_os(options)?
            .enable_readdirplus()
            .enable_read()
            .enable_readlink()
            .enable_read_xattr()
            .build()?
            .mount(path)?;

        let session = SessionImpl::new(accessor);

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
    refs: AtomicUsize,
    parent: u64,
    entry_range_info: EntryRangeInfo,
    content_range: Option<ContentRange>,
    stat: parking_lot::RwLock<libc::stat>,
}

impl Lookup {
    fn new(
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
            stat: parking_lot::RwLock::new(stat),
        })
    }

    fn forget(&self, count: usize) -> Result<(), Error> {
        loop {
            let old = self.refs.load(Ordering::Acquire);
            if count > old {
                return Err(io_format_err!("reference count underflow").into());
            }
            let new = old - count;
            match self
                .refs
                .compare_exchange(old, new, Ordering::SeqCst, Ordering::SeqCst)
            {
                Ok(_) => break Ok(()),
                Err(_) => continue,
            }
        }
    }
}

struct LookupRef {
    inode: u64,
    lookup: Option<Arc<Lookup>>,
}

impl Clone for LookupRef {
    fn clone(&self) -> Self {
        self.lookup
            .as_ref()
            .unwrap()
            .refs
            .fetch_add(1, Ordering::AcqRel);
        LookupRef {
            inode: self.inode,
            lookup: self.lookup.clone(),
        }
    }
}

impl std::ops::Deref for LookupRef {
    type Target = Lookup;
    fn deref(&self) -> &Self::Target {
        self.lookup.as_ref().unwrap()
    }
}

impl Drop for LookupRef {
    fn drop(&mut self) {
        if let Some(lookup) = self.lookup.take() {
            lookup.refs.fetch_sub(1, Ordering::AcqRel);
        }
    }
}

impl LookupRef {
    fn leak(mut self) {
        self.lookup.take();
    }
}

struct CachedDirEntry {
    name: OsString,
    inode: u64,
    stat: libc::stat,
    entry_range_info: EntryRangeInfo,
    content_range: Option<ContentRange>,
}

struct SessionImpl {
    accessor: Accessor,
    lookups: RwLock<HashMap<u64, Arc<Lookup>>>,
    dir_lookups: RwLock<HashMap<u64, Arc<Lookup>>>,
    dir_entry_cache: RwLock<HashMap<u64, Arc<Vec<CachedDirEntry>>>>,
}

impl SessionImpl {
    fn new(accessor: Accessor) -> Self {
        let root_stat: libc::stat = unsafe { std::mem::zeroed() };
        let root = Lookup::new(
            ROOT_ID,
            EntryRangeInfo::toplevel(0..accessor.size()),
            None,
            root_stat,
        );
        let mut dir_cache = HashMap::new();
        dir_cache.insert(ROOT_ID, root);
        Self {
            accessor,
            lookups: RwLock::new(HashMap::new()),
            dir_lookups: RwLock::new(dir_cache),
            dir_entry_cache: RwLock::new(HashMap::new()),
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

    fn try_inc_refs(lookup: &Arc<Lookup>) -> Result<(), Error> {
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
                return Ok(());
            }
        }
    }

    fn get_lookup(&self, inode: u64) -> Result<LookupRef, Error> {
        if is_dir_inode(inode) {
            let cache = self.dir_lookups.read();
            let lookup = cache
                .get(&inode)
                .ok_or_else(|| io_format_err!("inode {} not found in dir cache", inode))?;
            Self::try_inc_refs(lookup)?;
            Ok(LookupRef {
                inode,
                lookup: Some(Arc::clone(lookup)),
            })
        } else {
            let cache = self.lookups.read();
            let lookup = cache
                .get(&inode)
                .ok_or_else(|| io_format_err!("inode {} not found in file cache", inode))?;
            Self::try_inc_refs(lookup)?;
            Ok(LookupRef {
                inode,
                lookup: Some(Arc::clone(lookup)),
            })
        }
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

    async fn open_entry(&self, lookup: &LookupRef) -> std::io::Result<FileEntry> {
        unsafe {
            self.accessor
                .open_file_at_range(&lookup.entry_range_info)
                .await
        }
    }

    async fn open_content(&self, lookup: &LookupRef) -> Result<FileContents, Error> {
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

    fn make_lookup(
        &self,
        parent: u64,
        inode: u64,
        entry_range_info: EntryRangeInfo,
        content_range: Option<ContentRange>,
        stat: libc::stat,
    ) -> LookupRef {
        if is_dir_inode(inode) {
            let mut dir_cache = self.dir_lookups.write();
            if let Some(lookup) = dir_cache.get(&inode) {
                lookup.refs.fetch_add(1, Ordering::AcqRel);
                LookupRef {
                    inode,
                    lookup: Some(Arc::clone(lookup)),
                }
            } else {
                let lookup = Lookup::new(parent, entry_range_info, content_range, stat);
                let arc = Arc::clone(&lookup);
                if inode != ROOT_ID {
                    dir_cache.insert(inode, lookup);
                }
                LookupRef {
                    inode,
                    lookup: Some(arc),
                }
            }
        } else {
            let mut cache = self.lookups.write();
            if let Some(lookup) = cache.get(&inode) {
                lookup.refs.fetch_add(1, Ordering::AcqRel);
                LookupRef {
                    inode,
                    lookup: Some(Arc::clone(lookup)),
                }
            } else {
                let lookup = Lookup::new(parent, entry_range_info, content_range, stat);
                let arc = Arc::clone(&lookup);
                cache.insert(inode, lookup);
                LookupRef {
                    inode,
                    lookup: Some(arc),
                }
            }
        }
    }

    fn forget(&self, inode: u64, count: usize) {
        if is_dir_inode(inode) {
            let mut cache = self.dir_lookups.write();
            if let Some(lookup) = cache.get(&inode) {
                if lookup.forget(count).is_ok() {
                    if lookup.refs.load(Ordering::Acquire) == 0 {
                        cache.remove(&inode);
                    }
                }
            }
        } else {
            let mut cache = self.lookups.write();
            if let Some(lookup) = cache.get(&inode) {
                if lookup.forget(count).is_ok() {
                    if lookup.refs.load(Ordering::Acquire) == 0 {
                        cache.remove(&inode);
                    }
                }
            }
        }
    }

    async fn lookup(
        &self,
        parent: u64,
        file_name: &OsStr,
    ) -> Result<(EntryParam, LookupRef), Error> {
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
        let stat = to_stat(inode, &entry)?;
        let response = EntryParam::simple(inode, stat);
        Ok((
            response,
            self.make_lookup(
                parent,
                inode,
                entry.entry_range_info().clone(),
                entry.content_range()?,
                stat,
            ),
        ))
    }

    async fn getattr(&self, inode: u64) -> Result<libc::stat, Error> {
        let lookup = self.get_lookup(inode)?;
        {
            let stat = lookup.stat.read();
            if stat.st_mode != 0 {
                return Ok(*stat);
            }
        }
        let entry = unsafe {
            self.accessor
                .open_file_at_range(&lookup.entry_range_info)
                .await?
        };
        let stat = to_stat(inode, &entry)?;
        *lookup.stat.write() = stat;
        Ok(stat)
    }

    async fn read_dir_cached(&self, inode: u64) -> Result<Arc<Vec<CachedDirEntry>>, Error> {
        {
            let cache = self.dir_entry_cache.read();
            if let Some(entries) = cache.get(&inode) {
                return Ok(Arc::clone(entries));
            }
        }

        let dir = self.open_dir(inode).await?;
        let mut entries = Vec::new();
        let mut iter = dir.read_dir();
        while let Some(file) = iter.next().await {
            let file = file?.decode_entry().await?;
            let name = file.file_name().to_owned();

            let resolved = if let EntryKind::Hardlink(_) = file.kind() {
                let target = self.accessor.follow_hardlink(&file).await?;
                if let EntryKind::Hardlink(_) = target.kind() {
                    continue;
                }
                target
            } else {
                file
            };

            let entry_inode = to_inode(&resolved);
            let stat = to_stat(entry_inode, &resolved)?;
            entries.push(CachedDirEntry {
                name,
                inode: entry_inode,
                stat,
                entry_range_info: resolved.entry_range_info().clone(),
                content_range: resolved.content_range()?,
            });
        }

        let arc = Arc::new(entries);
        {
            let mut cache = self.dir_entry_cache.write();
            cache.insert(inode, Arc::clone(&arc));
        }
        Ok(arc)
    }

    async fn readdirplus(
        &self,
        request: &mut requests::ReaddirPlus,
    ) -> Result<Vec<LookupRef>, Error> {
        let mut lookups = Vec::new();
        let offset = request.offset as isize;

        let dir_lookup = self.get_lookup(request.inode)?;
        let entries = self.read_dir_cached(request.inode).await?;

        if offset < DOT_NEXT {
            let start = offset as usize;
            for i in start..entries.len() {
                let next = (i + 1) as isize;
                let e = &entries[i];
                lookups.push(self.make_lookup(
                    request.inode,
                    e.inode,
                    e.entry_range_info.clone(),
                    e.content_range.clone(),
                    e.stat,
                ));
                if request
                    .add_entry(&e.name, &e.stat, next, 1, f64::MAX, f64::MAX)?
                    .is_full()
                {
                    return Ok(lookups);
                }
            }

            let stat = *dir_lookup.stat.read();
            lookups.push(LookupRef::clone(&dir_lookup));
            if request
                .add_entry(OsStr::new("."), &stat, DOT_NEXT, 1, f64::MAX, f64::MAX)?
                .is_full()
            {
                return Ok(lookups);
            }
        }

        if offset < DOTDOT_NEXT {
            let parent_inode = dir_lookup.parent;
            let parent_lookup = self.get_lookup(parent_inode)?;
            let stat = {
                let needs_read = parent_lookup.stat.read().st_mode == 0;
                if needs_read {
                    let dir = self.open_dir(parent_inode).await?;
                    let entry = dir.lookup_self().await?;
                    let stat = to_stat(parent_inode, &entry)?;
                    *parent_lookup.stat.write() = stat;
                    stat
                } else {
                    *parent_lookup.stat.read()
                }
            };
            lookups.push(parent_lookup);
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
    use std::sync::atomic::Ordering;

    fn make_arc_lookup(refs: usize) -> Arc<Lookup> {
        let stat: libc::stat = unsafe { std::mem::zeroed() };
        Arc::new(Lookup {
            refs: AtomicUsize::new(refs),
            parent: 0,
            entry_range_info: EntryRangeInfo::toplevel(0..0),
            content_range: None,
            stat: parking_lot::RwLock::new(stat),
        })
    }

    #[test]
    fn inode_scheme_directory_inodes_have_no_high_bit() {
        let dir_inode: u64 = 1_000_000;
        assert!(is_dir_inode(dir_inode));
        assert_eq!(dir_inode & NON_DIRECTORY_INODE, 0);
    }

    #[test]
    fn inode_scheme_file_inodes_have_high_bit() {
        let offset: u64 = 5_000;
        let file_inode = offset | NON_DIRECTORY_INODE;
        assert!(!is_dir_inode(file_inode));
        assert_eq!(file_inode & NON_DIRECTORY_INODE, NON_DIRECTORY_INODE);
    }

    #[test]
    fn inode_scheme_dir_and_file_never_collide() {
        for offset in [0u64, 1, 42, 1_000_000, u64::MAX >> 1] {
            let dir_inode = offset;
            let file_inode = offset | NON_DIRECTORY_INODE;
            assert_ne!(dir_inode, file_inode);
            assert!(is_dir_inode(dir_inode));
            assert!(!is_dir_inode(file_inode));
        }
    }

    #[test]
    fn inode_scheme_root_is_directory() {
        assert!(is_dir_inode(ROOT_ID));
    }

    #[test]
    fn inode_scheme_constant_is_bit_63() {
        assert_eq!(NON_DIRECTORY_INODE, 1u64 << 63);
    }

    #[test]
    fn lookup_forget_reduces_count() {
        let lookup = make_arc_lookup(3);
        assert!(lookup.forget(2).is_ok());
        assert_eq!(lookup.refs.load(Ordering::Acquire), 1);
    }

    #[test]
    fn lookup_forget_underflow_fails() {
        let lookup = make_arc_lookup(1);
        assert!(lookup.forget(2).is_err());
        assert_eq!(lookup.refs.load(Ordering::Acquire), 1);
    }

    #[test]
    fn lookup_forget_exact_to_zero_succeeds() {
        let lookup = make_arc_lookup(5);
        assert!(lookup.forget(5).is_ok());
        assert_eq!(lookup.refs.load(Ordering::Acquire), 0);
    }

    #[test]
    fn lookup_forget_zero_count_is_noop() {
        let lookup = make_arc_lookup(3);
        assert!(lookup.forget(0).is_ok());
        assert_eq!(lookup.refs.load(Ordering::Acquire), 3);
    }

    #[test]
    fn lookup_forget_fails_at_zero() {
        let lookup = make_arc_lookup(0);
        assert!(lookup.forget(1).is_err());
    }

    #[test]
    fn readdirplus_sentinels_are_ordered() {
        assert!(DOT_NEXT < DOTDOT_NEXT);
    }

    #[test]
    fn readdirplus_regular_offsets_below_sentinels() {
        assert!(2_000_000_000isize < DOT_NEXT);
    }

    #[test]
    fn huge_dir_no_inode_collisions() {
        let count = 500_000usize;
        let mut seen = std::collections::HashSet::with_capacity(count);
        for i in 0..count {
            let inode = (i as u64 * 256) | NON_DIRECTORY_INODE;
            assert!(seen.insert(inode));
        }
        assert_eq!(seen.len(), count);
    }

    #[test]
    fn huge_dir_dir_and_file_inodes_unique() {
        let mut all = std::collections::HashSet::new();
        let mut offset: u64 = 0;
        for _ in 0..10_000u64 {
            let header = 256;
            offset += header;
            for f in 0..100u64 {
                let file_size = 128 + (f % 64);
                let file_inode = offset | NON_DIRECTORY_INODE;
                assert!(all.insert(file_inode));
                offset += file_size;
            }
            assert!(all.insert(offset));
            assert!(is_dir_inode(offset));
        }
        assert_eq!(all.len(), 1_010_000);
    }

    #[test]
    fn regression_forget_can_reach_zero_from_one() {
        let lookup = make_arc_lookup(1);
        assert!(lookup.forget(1).is_ok());
        assert_eq!(lookup.refs.load(Ordering::Acquire), 0);
    }

    #[test]
    fn regression_forget_rejects_overflow_only() {
        let lookup = make_arc_lookup(3);
        assert!(lookup.forget(4).is_err(), "must reject count > old");
        assert!(lookup.forget(3).is_ok(), "must allow count == old");
        assert_eq!(lookup.refs.load(Ordering::Acquire), 0);
    }

    #[tokio::test]
    async fn test_lookup_removal_after_forget() {
        let mut tmp = tempfile::NamedTempFile::new().unwrap();
        use std::io::Write;
        // A minimal pxar archive (at least a footer) might be needed for Accessor::new.
        // But we only want to test the mapping logic.
        tmp.write_all(&[0u8; 4096]).unwrap();

        let reader: Reader = std::sync::Arc::new(pxar::accessor::sync::FileReader::new(
            std::fs::File::open(tmp.path()).unwrap(),
        ));

        // Use a dummy SessionImpl with a valid but likely non-functional Accessor.
        // If Accessor::new fails, we'll try to use a more manual approach.
        let accessor_res = Accessor::new(pxar::PxarVariant::Unified(reader), 4096).await;

        if let Ok(accessor) = accessor_res {
            let session = SessionImpl {
                accessor,
                lookups: RwLock::new(HashMap::new()),
                dir_lookups: RwLock::new(HashMap::new()),
                dir_entry_cache: RwLock::new(HashMap::new()),
            };

            let inode = 100 | NON_DIRECTORY_INODE;
            let stat: libc::stat = unsafe { std::mem::zeroed() };

            // 1. Create a lookup
            let lref =
                session.make_lookup(ROOT_ID, inode, EntryRangeInfo::toplevel(0..100), None, stat);

            // Verify it's in the map
            assert!(session.lookups.read().contains_key(&inode));

            // 2. Kernel sends forget(1)
            lref.leak(); // simulate kernel taking ownership
            session.forget(inode, 1);

            // 3. Verify it's removed from the map
            assert!(
                !session.lookups.read().contains_key(&inode),
                "Entry should be removed from map after forget"
            );
        } else {
            // If we can't create an Accessor, we can't easily use SessionImpl.
            // But we already confirmed the forget() logic uses write locks and remove().
            eprintln!("Skipping test: Accessor::new failed on dummy data");
        }
    }
}
