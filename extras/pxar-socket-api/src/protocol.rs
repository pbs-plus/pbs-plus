use serde::{Deserialize, Serialize};

#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "PascalCase")]
pub enum Request {
    GetRoot,
    LookupByPath {
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
    Entry { info: EntryInfo },
    DirEntries { entries: Vec<EntryInfo> },
    Data { data: Vec<u8> },
    Symlink { target: Vec<u8> },
    XAttrs { xattrs: Vec<(Vec<u8>, Vec<u8>)> },
    Error { errno: i32 },
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EntryInfo {
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

#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
pub enum FileType {
    File,
    Directory,
    Symlink,
    Hardlink,
    Device,
    Fifo,
    Socket,
}
