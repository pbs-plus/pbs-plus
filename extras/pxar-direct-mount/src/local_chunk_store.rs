use std::fs::File;
use std::path::PathBuf;
use std::sync::Arc;

use anyhow::Error;
use pbs_datastore::data_blob::DataBlob;
use pbs_datastore::read_chunk::ReadChunk;
use pbs_tools::crypt_config::CryptConfig;
use sha2::{Digest, Sha256};

const HEX_TABLE: &[u8; 16] = b"0123456789abcdef";

pub type ChunkKey = [u8; 32];

#[inline]
fn hex_encode(digest: &[u8; 32]) -> [u8; 64] {
    let mut out = [0u8; 64];
    for (i, &byte) in digest.iter().enumerate() {
        out[i * 2] = HEX_TABLE[(byte >> 4) as usize];
        out[i * 2 + 1] = HEX_TABLE[(byte & 0xf) as usize];
    }
    out
}

#[derive(Clone)]
pub struct LocalChunkStore {
    chunks_root: PathBuf,
    crypt: Option<Arc<CryptConfig>>,
    verify: bool,
}

impl LocalChunkStore {
    pub fn new(root: impl Into<PathBuf>, crypt: Option<Arc<CryptConfig>>, verify: bool) -> Self {
        let root = root.into();
        let chunks_root = root.join(".chunks");
        Self {
            chunks_root,
            crypt,
            verify,
        }
    }

    fn chunk_path(&self, digest: &ChunkKey) -> PathBuf {
        let hex = hex_encode(digest);
        let hex_str = std::str::from_utf8(&hex).unwrap();
        let mut path = PathBuf::with_capacity(self.chunks_root.as_os_str().len() + 70);
        path.push(&self.chunks_root);
        path.push(&hex_str[..4]);
        path.push(hex_str);
        path
    }

    fn load_and_decode(&self, digest: &ChunkKey) -> Result<Vec<u8>, Error> {
        let path = self.chunk_path(digest);
        let mut file = File::open(&path)
            .map_err(|e| anyhow::anyhow!("open chunk {} failed: {e}", path.display()))?;

        let blob = DataBlob::load_from_reader(&mut file)?;
        let data = blob.decode(self.crypt.as_deref(), None)?;

        if self.verify {
            let mut hasher = Sha256::new();
            hasher.update(&data);
            let got = hasher.finalize();
            if got[..] != digest[..] {
                return Err(anyhow::anyhow!(
                    "chunk digest mismatch for {}",
                    path.display()
                ));
            }
        }

        Ok(data)
    }
}

impl ReadChunk for LocalChunkStore {
    fn read_chunk(&self, digest: &ChunkKey) -> Result<Vec<u8>, Error> {
        self.load_and_decode(digest)
    }

    fn read_raw_chunk(&self, digest: &ChunkKey) -> Result<DataBlob, Error> {
        let path = self.chunk_path(digest);
        let mut file = File::open(&path)
            .map_err(|e| anyhow::anyhow!("open chunk {} failed: {e}", path.display()))?;
        DataBlob::load_from_reader(&mut file)
    }
}
