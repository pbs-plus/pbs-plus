use anyhow::{bail, Error, Result};
use sha2::{Digest, Sha256};
use siphasher::sip::SipHasher24;
use std::env;
use std::fs::File;
use std::io::{self, Read, Write};
use std::os::unix::ffi::OsStrExt;
use std::path::Path;

use pbs_datastore::data_blob::{DataBlob, DataChunkBuilder};

const PXAR_HASH_KEY_1: u64 = 0x83ac3f1cfbb450db;
const PXAR_HASH_KEY_2: u64 = 0xaa4f1b6879369fbd;

fn main() -> Result<()> {
    let args: Vec<String> = env::args().collect();

    if args.len() < 2 {
        eprintln!("Usage: pxar-test-util <command> [args...]");
        eprintln!("Commands:");
        eprintln!("  decode-blob <file>       - Decode a data blob and output raw content");
        eprintln!("  sha256 <file>            - Compute SHA256 hash of file");
        eprintln!("  siphash24 <file>         - Compute SipHash24 hash of file");
        eprintln!("  read-didx <file>         - Read dynamic index entries");
        eprintln!("  parse-header <file>      - Parse pxar header");
        eprintln!("  create-blob <in> <out>   - Create a compressed blob");
        eprintln!("  verify-blob <file>       - Verify blob CRC and structure");
        std::process::exit(1);
    }

    match args[1].as_str() {
        "decode-blob" => {
            if args.len() < 3 {
                bail!("Usage: decode-blob <file>");
            }
            decode_blob(&args[2])
        }
        "sha256" => {
            if args.len() < 3 {
                bail!("Usage: sha256 <file>");
            }
            compute_sha256(&args[2])
        }
        "siphash24" => {
            if args.len() < 3 {
                bail!("Usage: siphash24 <file>");
            }
            compute_siphash24(&args[2])
        }
        "read-didx" => {
            if args.len() < 3 {
                bail!("Usage: read-didx <file>");
            }
            read_didx(&args[2])
        }
        "parse-header" => {
            if args.len() < 3 {
                bail!("Usage: parse-header <file>");
            }
            parse_header(&args[2])
        }
        "create-blob" => {
            if args.len() < 4 {
                bail!("Usage: create-blob <input> <output>");
            }
            create_blob(&args[2], &args[3])
        }
        "verify-blob" => {
            if args.len() < 3 {
                bail!("Usage: verify-blob <file>");
            }
            verify_blob(&args[2])
        }
        _ => {
            bail!("Unknown command: {}", args[1]);
        }
    }
}

fn decode_blob(path: &str) -> Result<()> {
    let mut file = File::open(path)?;
    let mut data = Vec::new();
    file.read_to_end(&mut data)?;

    let blob = DataBlob::load_from_reader(&mut io::Cursor::new(&data))?;
    let decoded = blob.decode(None, None)?;

    io::stdout().write_all(&decoded)?;
    Ok(())
}

fn compute_sha256(path: &str) -> Result<()> {
    let mut file = File::open(path)?;
    let mut hasher = Sha256::new();
    let mut buffer = [0u8; 8192];

    loop {
        let n = file.read(&mut buffer)?;
        if n == 0 {
            break;
        }
        hasher.update(&buffer[..n]);
    }

    let result = hasher.finalize();
    io::stdout().write_all(&result)?;
    Ok(())
}

fn compute_siphash24(path: &str) -> Result<()> {
    use std::hash::Hasher;

    let mut file = File::open(path)?;
    let mut hasher = SipHasher24::new_with_keys(PXAR_HASH_KEY_1, PXAR_HASH_KEY_2);
    let mut buffer = [0u8; 8192];

    loop {
        let n = file.read(&mut buffer)?;
        if n == 0 {
            break;
        }
        hasher.write(&buffer[..n]);
    }

    let result = hasher.finish();
    io::stdout().write_all(&result.to_le_bytes())?;
    Ok(())
}

fn read_didx(path: &str) -> Result<()> {
    use pbs_datastore::dynamic_index::DynamicIndexReader;
    use pbs_datastore::index::IndexFile;

    let file = File::open(path)?;
    let index = DynamicIndexReader::new(file)?;

    let num_entries = index.index_count();
    io::stdout().write_all(&(num_entries as u32).to_le_bytes())?;

    for i in 0..num_entries {
        let start = if i > 0 { index.chunk_end(i - 1) } else { 0 };
        let end = index.chunk_end(i);
        let digest = index.chunk_digest(i);

        io::stdout().write_all(&start.to_le_bytes())?;
        io::stdout().write_all(&end.to_le_bytes())?;
        io::stdout().write_all(digest)?;
    }

    Ok(())
}

fn parse_header(path: &str) -> Result<()> {
    let mut file = File::open(path)?;
    let mut header = [0u8; 16];
    file.read_exact(&mut header)?;

    let htype = u64::from_le_bytes(header[0..8].try_into()?);
    let full_size = u64::from_le_bytes(header[8..16].try_into()?);

    io::stdout().write_all(&htype.to_le_bytes())?;
    io::stdout().write_all(&full_size.to_le_bytes())?;
    Ok(())
}

fn create_blob(input: &str, output: &str) -> Result<()> {
    let mut input_file = File::open(input)?;
    let mut data = Vec::new();
    input_file.read_to_end(&mut data)?;

    let chunk = DataChunkBuilder::new(&data).compress(true).build()?;

    let mut output_file = File::create(output)?;
    chunk.raw_data().save_to_writer(&mut output_file)?;
    Ok(())
}

fn verify_blob(path: &str) -> Result<()> {
    let mut file = File::open(path)?;
    let mut data = Vec::new();
    file.read_to_end(&mut data)?;

    let blob = DataBlob::load_from_reader(&mut io::Cursor::new(&data))?;
    blob.verify_crc()?;

    let magic = blob.magic();
    eprintln!("Magic: {:02x?}", magic);
    eprintln!("Is compressed: {}", blob.is_compressed());
    eprintln!("Is encrypted: {}", blob.is_encrypted());
    eprintln!("CRC verified successfully");

    Ok(())
}
