use serde::{Deserialize, Serialize};
use serde_repr::{Deserialize_repr, Serialize_repr};

#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "PascalCase")]
pub enum Request {
    GetRoot,
    LookupByPath {
        #[serde(with = "serde_bytes")]
        path: Vec<u8>,
    },
    ReadDir {
        entry_end: u64,
    },
    GetAttr {
        entry_start: u64,
        entry_end: u64,
    },
    Read {
        content_start: u64,
        content_end: u64,
        offset: u64,
        size: usize,
        buf_capacity: usize,
    },
    ReadLink {
        entry_start: u64,
        entry_end: u64,
    },
    ListXAttrs {
        entry_start: u64,
        entry_end: u64,
    },
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "PascalCase")]
pub enum Response {
    Entry {
        info: EntryInfo,
    },
    DirEntries {
        entries: Vec<EntryInfo>,
    },
    Data {
        #[serde(with = "serde_bytes")]
        data: Vec<u8>,
    },
    Symlink {
        #[serde(with = "serde_bytes")]
        target: Vec<u8>,
    },
    XAttrs {
        xattrs: Vec<(Vec<u8>, Vec<u8>)>,
    },
    Error {
        errno: i32,
    },
}

#[derive(Debug, Clone, Copy, Serialize_repr, Deserialize_repr, PartialEq)]
#[repr(u8)]
pub enum FileType {
    File = 0,
    Directory = 1,
    Symlink = 2,
    Hardlink = 3,
    Device = 4,
    Fifo = 5,
    Socket = 6,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EntryInfo {
    #[serde(with = "serde_bytes")]
    pub file_name: Vec<u8>,
    pub file_type: FileType,
    pub entry_range_start: u64,
    pub entry_range_end: u64,
    pub content_range: Option<(u64, u64)>,
    pub mode: u64,
    pub uid: u32,
    pub gid: u32,
    pub size: u64,
    pub mtime_secs: i64,
    pub mtime_nsecs: u32,
}
