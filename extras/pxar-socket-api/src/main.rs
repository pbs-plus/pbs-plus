mod direct_dynamic_index;
mod local_chunk_store;
mod protocol;

use std::env;
use std::ffi::OsStr;
use std::os::unix::ffi::OsStrExt;
use std::path::Path;
use std::sync::Arc;

use anyhow::{bail, Context, Error};
use log::LevelFilter;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{UnixListener, UnixStream};

use pxar::accessor::{self, ReadAt};

use crate::direct_dynamic_index::{DirectDynamicReader, DirectLocalDynamicReadAt};
use crate::local_chunk_store::LocalChunkStore;
use crate::protocol::{EntryInfo, FileType, Request, Response};

use pbs_datastore::dynamic_index::DynamicIndexReader;
use pbs_key_config::load_and_decrypt_key;
use pbs_tools::crypt_config::CryptConfig;

pub type Reader = Arc<dyn ReadAt + Send + Sync + 'static>;
pub type Accessor = accessor::aio::Accessor<Reader>;

struct Server {
    accessor: Accessor,
}

impl Server {
    fn new(accessor: Accessor) -> Self {
        Self { accessor }
    }

    async fn handle_connection(&self, mut stream: UnixStream) -> Result<(), Error> {
        loop {
            // Read request length (4 bytes)
            let mut len_buf = [0u8; 4];
            match stream.read_exact(&mut len_buf).await {
                Ok(_) => {}
                Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => break,
                Err(e) => return Err(e.into()),
            }
            let len = u32::from_le_bytes(len_buf) as usize;

            // Read request body
            let mut req_buf = vec![0u8; len];
            stream.read_exact(&mut req_buf).await?;

            let request: Request = ciborium::de::from_reader(&req_buf[..])?;
            let response = self.handle_request(request).await;

            // Serialize response
            let mut resp_buf = Vec::new();
            ciborium::ser::into_writer(&response, &mut resp_buf)?;
            let resp_len = (resp_buf.len() as u32).to_le_bytes();

            // Send response
            stream.write_all(&resp_len).await?;
            stream.write_all(&resp_buf).await?;
        }
        Ok(())
    }

    async fn handle_request(&self, request: Request) -> Response {
        match self.handle_request_inner(request).await {
            Ok(resp) => resp,
            Err(e) => {
                let errno = e
                    .downcast::<std::io::Error>()
                    .map(|e| e.raw_os_error().unwrap_or(libc::EIO))
                    .unwrap_or(libc::EIO);
                Response::Error { errno }
            }
        }
    }

    async fn handle_request_inner(&self, request: Request) -> Result<Response, Error> {
        match request {
            Request::GetRoot => {
                let root = self.accessor.open_root().await?;
                let entry = root.lookup_self().await?;
                Ok(Response::Entry {
                    info: entry_to_info(&entry, self.accessor.size())?,
                })
            }

            Request::LookupByPath { path } => {
                // Convert Vec<u8> to OsStr
                let path_osstr = OsStr::from_bytes(&path);

                let entry = self
                    .accessor
                    .open_root()
                    .await?
                    .lookup(path_osstr)
                    .await?
                    .ok_or_else(|| std::io::Error::from_raw_os_error(libc::ENOENT))?;

                let entry = self.follow_hardlink(entry).await?;
                Ok(Response::Entry {
                    info: entry_to_info(&entry, 0)?,
                })
            }

            Request::ReadDir { entry_end } => {
                let dir = unsafe { self.accessor.open_dir_at_end(entry_end).await? };
                let mut entries = Vec::new();

                let mut iter = dir.read_dir();
                while let Some(file) = iter.next().await {
                    let file = file?.decode_entry().await?;
                    entries.push(entry_to_info(&file, 0)?);
                }

                Ok(Response::DirEntries { entries })
            }

            Request::GetAttr {
                entry_start,
                entry_end,
            } => {
                let entry = unsafe {
                    self.accessor
                        .open_file_at_range(&pxar::accessor::EntryRangeInfo::toplevel(
                            entry_start..entry_end,
                        ))
                        .await?
                };
                Ok(Response::Entry {
                    info: entry_to_info(&entry, 0)?,
                })
            }

            Request::Read {
                content_start,
                content_end,
                offset,
                size,
            } => {
                // Open the file contents directly using the saved ranges
                let entry_range =
                    pxar::accessor::EntryRangeInfo::toplevel(content_start..content_end);
                let entry = unsafe { self.accessor.open_file_at_range(&entry_range).await? };

                let contents = match entry.contents().await {
                    Ok(contents) => contents,
                    Err(_) => return Err(std::io::Error::from_raw_os_error(libc::EINVAL).into()),
                };

                let total_size = entry.file_size().unwrap_or(0);
                let read_size = std::cmp::min(size, (total_size.saturating_sub(offset)) as usize);

                let mut buf = vec![0u8; read_size];
                let mut pos = 0;

                while pos < read_size {
                    let n = contents
                        .read_at(&mut buf[pos..], offset + pos as u64)
                        .await?;
                    if n == 0 {
                        break;
                    }
                    pos += n;
                }

                buf.truncate(pos);
                Ok(Response::Data { data: buf })
            }

            Request::ReadLink {
                entry_start,
                entry_end,
            } => {
                let entry = unsafe {
                    self.accessor
                        .open_file_at_range(&pxar::accessor::EntryRangeInfo::toplevel(
                            entry_start..entry_end,
                        ))
                        .await?
                };

                let link = entry
                    .get_symlink()
                    .ok_or_else(|| std::io::Error::from_raw_os_error(libc::EINVAL))?;

                Ok(Response::Symlink {
                    target: link.as_bytes().to_vec(),
                })
            }

            Request::ListXAttrs {
                entry_start,
                entry_end,
            } => {
                let entry = unsafe {
                    self.accessor
                        .open_file_at_range(&pxar::accessor::EntryRangeInfo::toplevel(
                            entry_start..entry_end,
                        ))
                        .await?
                };

                let metadata = entry.into_entry().into_metadata();
                let mut xattrs = Vec::new();

                for xattr in metadata.xattrs {
                    xattrs.push((xattr.name().to_bytes().to_vec(), xattr.value().to_vec()));
                }

                if let Some(fcaps) = metadata.fcaps {
                    xattrs.push((
                        proxmox_sys::fs::xattr::XATTR_NAME_FCAPS.to_bytes().to_vec(),
                        fcaps.data,
                    ));
                }

                Ok(Response::XAttrs { xattrs })
            }
        }
    }

    async fn follow_hardlink(
        &self,
        entry: accessor::aio::FileEntry<Reader>,
    ) -> Result<accessor::aio::FileEntry<Reader>, Error> {
        if let pxar::EntryKind::Hardlink(_) = entry.kind() {
            let entry = self.accessor.follow_hardlink(&entry).await?;
            if let pxar::EntryKind::Hardlink(_) = entry.kind() {
                return Err(std::io::Error::from_raw_os_error(libc::ELOOP).into());
            }
            Ok(entry)
        } else {
            Ok(entry)
        }
    }
}

fn entry_to_info(
    entry: &accessor::aio::FileEntry<Reader>,
    _dir_end_hint: u64,
) -> Result<EntryInfo, Error> {
    let metadata = entry.metadata();
    let range = entry.entry_range_info();

    let (file_type, content_range) = match entry.kind() {
        pxar::EntryKind::Directory => (FileType::Directory, None),
        pxar::EntryKind::File { .. } => {
            // For files, we'll pass the entry range which can be used to re-open the file
            // The client will use this same range to read content
            (
                FileType::File,
                Some((range.entry_range.start, range.entry_range.end)),
            )
        }
        pxar::EntryKind::Symlink(_) => (FileType::Symlink, None),
        pxar::EntryKind::Hardlink(_) => (FileType::Hardlink, None),
        pxar::EntryKind::Device(_) => (FileType::Device, None),
        pxar::EntryKind::Fifo => (FileType::Fifo, None),
        pxar::EntryKind::Socket => (FileType::Socket, None),
        _ => (FileType::File, None),
    };

    Ok(EntryInfo {
        file_name: entry.file_name().as_bytes().to_vec(),
        file_type,
        entry_range_start: range.entry_range.start,
        entry_range_end: range.entry_range.end,
        content_range,
        mode: metadata.stat.mode,
        uid: metadata.stat.uid,
        gid: metadata.stat.gid,
        size: entry.file_size().unwrap_or(0),
        mtime_secs: metadata.stat.mtime.secs,
        mtime_nsecs: metadata.stat.mtime.nanos,
    })
}

fn get_key_password() -> Result<Vec<u8>, Error> {
    Ok(std::env::var("PROXMOX_KEY_PASSWORD")
        .unwrap_or_default()
        .into_bytes())
}

#[tokio::main]
async fn main() -> Result<(), Error> {
    env_logger::builder().filter(None, LevelFilter::Info).init();

    let args: Vec<String> = env::args().collect();
    if args.len() < 5 {
        eprintln!("Usage: pxar-socket-api --socket <path> --pbs-store <dir> --mpxar-didx <file> --ppxar-didx <file> [--keyfile <file>] [--verify-chunks]");
        bail!("insufficient arguments");
    }

    let mut socket_path = None;
    let mut pbs_store = None;
    let mut mpxar_didx = None;
    let mut ppxar_didx = None;
    let mut keyfile = None;
    let mut verify_chunks = false;

    let mut it = args.iter().skip(1);
    while let Some(arg) = it.next() {
        match arg.as_str() {
            "--socket" => socket_path = Some(it.next().context("--socket requires value")?),
            "--pbs-store" => pbs_store = Some(it.next().context("--pbs-store requires value")?),
            "--mpxar-didx" => mpxar_didx = Some(it.next().context("--mpxar-didx requires value")?),
            "--ppxar-didx" => ppxar_didx = Some(it.next().context("--ppxar-didx requires value")?),
            "--keyfile" => keyfile = Some(it.next().context("--keyfile requires value")?),
            "--verify-chunks" => verify_chunks = true,
            _ => bail!("unknown argument: {}", arg),
        }
    }

    let socket_path = socket_path.context("--socket required")?;
    let pbs_store = pbs_store.context("--pbs-store required")?;
    let mpxar_didx = mpxar_didx.context("--mpxar-didx required")?;
    let ppxar_didx = ppxar_didx.context("--ppxar-didx required")?;

    // Load encryption key if provided
    let crypt = if let Some(ref keyfile) = keyfile {
        let (key, _, _) = load_and_decrypt_key(Path::new(keyfile), &get_key_password)?;
        Some(Arc::new(CryptConfig::new(key)?))
    } else {
        None
    };

    // Open indexes
    let meta_index = DynamicIndexReader::new(std::fs::File::open(mpxar_didx)?)?;
    let meta_store = LocalChunkStore::new(pbs_store, crypt.clone(), verify_chunks);
    let meta_buffered = DirectDynamicReader::new(meta_index, meta_store);
    let meta_size = meta_buffered.archive_size();
    let meta_reader = Arc::new(DirectLocalDynamicReadAt::new(meta_buffered)) as Reader;

    let payload_index = DynamicIndexReader::new(std::fs::File::open(ppxar_didx)?)?;
    let payload_store = LocalChunkStore::new(pbs_store, crypt, verify_chunks);
    let payload_buffered = DirectDynamicReader::new(payload_index, payload_store);
    let payload_size = payload_buffered.archive_size();
    let payload_reader = Arc::new(DirectLocalDynamicReadAt::new(payload_buffered)) as Reader;

    let accessor = Accessor::new(
        pxar::PxarVariant::Split(meta_reader, (payload_reader, payload_size)),
        meta_size,
    )
    .await?;

    let server = Arc::new(Server::new(accessor));

    // Remove existing socket if present
    let _ = std::fs::remove_file(socket_path);

    let listener = UnixListener::bind(socket_path)?;
    log::info!("RPC server listening on {}", socket_path);

    loop {
        let (stream, _) = listener.accept().await?;
        let server = Arc::clone(&server);
        tokio::spawn(async move {
            if let Err(e) = server.handle_connection(stream).await {
                log::error!("Connection error: {}", e);
            }
        });
    }
}
