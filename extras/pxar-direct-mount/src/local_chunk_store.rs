use std::fs::File;
use std::path::PathBuf;

use anyhow::Error;
use pbs_datastore::data_blob::DataBlob;
use pbs_datastore::read_chunk::ReadChunk;
use pbs_tools::crypt_config::CryptConfig;
use sha2::{Digest, Sha256};

/// Local store reading chunks from <datastore>/.chunks/xx/<hexdigest>
pub struct LocalChunkStore {
    root: PathBuf,
    crypt: Option<std::sync::Arc<CryptConfig>>,
    verify: bool,
}

impl LocalChunkStore {
    pub fn new(
        root: impl Into<PathBuf>,
        crypt: Option<std::sync::Arc<CryptConfig>>,
        verify: bool,
    ) -> Self {
        Self {
            root: root.into(),
            crypt,
            verify,
        }
    }

    fn chunk_path(&self, digest: &[u8; 32]) -> PathBuf {
        let hex = hex::encode(digest);
        let (dir4, _) = hex.split_at(4);
        self.root.join(".chunks").join(dir4).join(hex)
    }
}

impl ReadChunk for LocalChunkStore {
    fn read_chunk(&self, digest: &[u8; 32]) -> Result<Vec<u8>, Error> {
        let path = self.chunk_path(digest);
        let mut file = File::open(&path)
            .map_err(|e| anyhow::anyhow!("open chunk {} failed: {e}", path.display()))?;

        // Load on-disk DataBlob (may be compressed/encrypted)
        let blob = DataBlob::load_from_reader(&mut file)?;

        // Decode to plaintext payload
        let data = blob.decode(self.crypt.as_deref(), None)?;

        // Optional verify: hash plaintext and compare to index digest
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

    fn read_raw_chunk(&self, digest: &[u8; 32]) -> Result<DataBlob, Error> {
        let path = self.chunk_path(digest);
        let mut file = File::open(&path)
            .map_err(|e| anyhow::anyhow!("open chunk {} failed: {e}", path.display()))?;
        let blob = DataBlob::load_from_reader(&mut file)?;
        Ok(blob)
    }
}
