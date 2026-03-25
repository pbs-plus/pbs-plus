mod direct_dynamic_index;
mod fuse_session;
mod local_chunk_store;

use std::env;
use std::ffi::OsStr;
use std::path::Path;

use anyhow::{bail, Context, Error};
use env_logger;
use futures::future::FutureExt;
use futures::select;
use log::LevelFilter;
use tokio::signal::unix::{signal, SignalKind};

use pxar::accessor;

use crate::direct_dynamic_index::ConcurrentLocalReader;
use crate::fuse_session::{Accessor, Reader, Session};
use crate::local_chunk_store::LocalChunkStore;

use pbs_datastore::dynamic_index::DynamicIndexReader;
use pbs_datastore::index::IndexFile;
use pbs_key_config::load_and_decrypt_key;
use pbs_tools::crypt_config::CryptConfig;

const DEFAULT_LOOKUP_CACHE_SIZE: usize = 131072;
const DEFAULT_CHUNK_CACHE_SIZE: usize = 4096;
const DEFAULT_CHUNK_TTL_SECS: u64 = 300;

struct Args {
    mountpoint: String,
    verbose: bool,
    pbs_store: String,
    mpxar_didx: String,
    ppxar_didx: String,
    keyfile: Option<String>,
    verify_chunks: bool,
    options: String,
    lookup_cache_size: usize,
    chunk_cache_size: usize,
    chunk_ttl_secs: u64,
}

fn print_usage() {
    eprintln!("Usage (datastore-only, no local .mpxar needed):");
    eprintln!("  pxar-direct-mount --pbs-store <datastore> --mpxar-didx <path/to/mpxar.didx> --ppxar-didx <path/to/ppxar.didx> <mountpoint> [options]");
    eprintln!();
    eprintln!("Required options:");
    eprintln!("  --pbs-store <DIR>        PBS datastore root (contains .chunks)");
    eprintln!("  --mpxar-didx <FILE>      Path to metadata dynamic index (mpxar.didx)");
    eprintln!("  --ppxar-didx <FILE>      Path to payload dynamic index (ppxar.didx)");
    eprintln!();
    eprintln!("Optional options:");
    eprintln!("  --keyfile <FILE>         Path to encryption key (if backup is encrypted)");
    eprintln!("  --verify-chunks          Verify chunk SHA256 on read");
    eprintln!("  --options <STR>          FUSE options (default: ro,default_permissions)");
    eprintln!("  --lookup-cache <N>       Max cached inodes (default: 131072)");
    eprintln!("  --chunk-cache <N>        Max cached chunks (default: 4096)");
    eprintln!("  --chunk-ttl <SECS>       Chunk cache TTL in seconds (default: 300)");
    eprintln!("  --verbose                Verbose logging/foreground");
    eprintln!();
    eprintln!("Legacy modes:");
    eprintln!("  Unified pxar: pbs-pxar-mount <archive.pxar> <mountpoint>");
    eprintln!("  Split local files: pbs-pxar-mount <archive.mpxar> <mountpoint> --payload-input <archive.ppxar>");
}

fn parse_args() -> Result<(Option<String>, Option<String>, Args), Error> {
    let mut it = env::args().skip(1);

    let mut positional: Vec<String> = Vec::new();
    let mut verbose = false;
    let mut payload_input: Option<String> = None;

    let mut pbs_store: Option<String> = None;
    let mut mpxar_didx: Option<String> = None;
    let mut ppxar_didx: Option<String> = None;
    let mut keyfile: Option<String> = None;
    let mut verify_chunks = false;
    let mut options = "ro,default_permissions".to_string();
    let mut lookup_cache_size = DEFAULT_LOOKUP_CACHE_SIZE;
    let mut chunk_cache_size = DEFAULT_CHUNK_CACHE_SIZE;
    let mut chunk_ttl_secs = DEFAULT_CHUNK_TTL_SECS;

    while let Some(arg) = it.next() {
        match arg.as_str() {
            "--verbose" => verbose = true,
            "--verify-chunks" => verify_chunks = true,
            "--payload-input" => {
                payload_input = Some(
                    it.next()
                        .ok_or_else(|| Error::msg("--payload-input requires value"))?,
                );
            }
            "--pbs-store" => {
                pbs_store = Some(
                    it.next()
                        .ok_or_else(|| Error::msg("--pbs-store requires value"))?,
                );
            }
            "--mpxar-didx" => {
                mpxar_didx = Some(
                    it.next()
                        .ok_or_else(|| Error::msg("--mpxar-didx requires value"))?,
                );
            }
            "--ppxar-didx" => {
                ppxar_didx = Some(
                    it.next()
                        .ok_or_else(|| Error::msg("--ppxar-didx requires value"))?,
                );
            }
            "--keyfile" => {
                keyfile = Some(
                    it.next()
                        .ok_or_else(|| Error::msg("--keyfile requires value"))?,
                );
            }
            "--options" => {
                options = it
                    .next()
                    .ok_or_else(|| Error::msg("--options requires value"))?;
            }
            "--lookup-cache" => {
                lookup_cache_size = it
                    .next()
                    .ok_or_else(|| Error::msg("--lookup-cache requires value"))?
                    .parse()
                    .context("--lookup-cache must be a number")?;
                if lookup_cache_size < 1024 {
                    bail!("--lookup-cache must be at least 1024");
                }
            }
            "--chunk-cache" => {
                chunk_cache_size = it
                    .next()
                    .ok_or_else(|| Error::msg("--chunk-cache requires value"))?
                    .parse()
                    .context("--chunk-cache must be a number")?;
                if chunk_cache_size < 64 {
                    bail!("--chunk-cache must be at least 64");
                }
            }
            "--chunk-ttl" => {
                chunk_ttl_secs = it
                    .next()
                    .ok_or_else(|| Error::msg("--chunk-ttl requires value"))?
                    .parse()
                    .context("--chunk-ttl must be a number (seconds)")?;
            }
            _ => positional.push(arg),
        }
    }

    let ds_mode = pbs_store.is_some() || mpxar_didx.is_some() || ppxar_didx.is_some();

    if ds_mode {
        if pbs_store.is_none() || mpxar_didx.is_none() || ppxar_didx.is_none() {
            print_usage();
            bail!("datastore mode requires --pbs-store, --mpxar-didx and --ppxar-didx");
        }
        if positional.len() != 1 {
            print_usage();
            bail!("datastore mode requires exactly one positional argument: <mountpoint>");
        }
        let mountpoint = positional.remove(0);
        let args = Args {
            mountpoint,
            verbose,
            pbs_store: pbs_store.unwrap(),
            mpxar_didx: mpxar_didx.unwrap(),
            ppxar_didx: ppxar_didx.unwrap(),
            keyfile,
            verify_chunks,
            options,
            lookup_cache_size,
            chunk_cache_size,
            chunk_ttl_secs,
        };
        return Ok((None, payload_input, args));
    }

    if positional.len() < 2 {
        print_usage();
        bail!("missing required positional arguments");
    }
    let archive = positional.remove(0);
    let mountpoint = positional.remove(0);

    let args = Args {
        mountpoint,
        verbose,
        pbs_store: String::new(),
        mpxar_didx: String::new(),
        ppxar_didx: String::new(),
        keyfile,
        verify_chunks,
        options,
        lookup_cache_size,
        chunk_cache_size,
        chunk_ttl_secs,
    };

    Ok((Some(archive), payload_input, args))
}

fn get_key_password() -> Result<Vec<u8>, Error> {
    Ok(std::env::var("PROXMOX_KEY_PASSWORD")
        .unwrap_or_default()
        .into_bytes())
}

#[tokio::main(flavor = "multi_thread")]
async fn main() -> Result<(), Error> {
    env_logger::builder().filter(None, LevelFilter::Info).init();

    let (archive_opt, payload_input, args) = parse_args()?;

    let mountpoint_path = Path::new(&args.mountpoint);
    let fuse_opts = OsStr::new(&args.options);

    let accessor: Accessor = if let Some(archive_path) = archive_opt {
        let meta_file = std::fs::File::open(&archive_path)
            .with_context(|| format!("failed to open archive {}", &archive_path))?;
        let meta_size = meta_file.metadata()?.len();
        let meta_reader: Reader = std::sync::Arc::new(accessor::sync::FileReader::new(meta_file));

        if let Some(ppxar) = payload_input {
            let pfile = std::fs::File::open(&ppxar)
                .with_context(|| format!("failed to open payload {}", ppxar))?;
            let psize = pfile.metadata()?.len();
            let p_reader: Reader = std::sync::Arc::new(accessor::sync::FileReader::new(pfile));
            Accessor::new(
                pxar::PxarVariant::Split(meta_reader, (p_reader, psize)),
                meta_size,
            )
            .await?
        } else {
            Accessor::new(pxar::PxarVariant::Unified(meta_reader), meta_size).await?
        }
    } else {
        let crypt = if let Some(ref keyfile) = args.keyfile {
            let (key, _, _fp) = load_and_decrypt_key(Path::new(keyfile), &get_key_password)?;
            Some(std::sync::Arc::new(CryptConfig::new(key)?))
        } else {
            None
        };

        let open_index = |path: &str| -> Result<DynamicIndexReader, Error> {
            let file = std::fs::File::open(path)
                .with_context(|| format!("failed to open didx {}", path))?;
            let index = DynamicIndexReader::new(file)
                .map_err(|e| Error::msg(format!("unable to read dynamic index '{}': {e}", path)))?;
            Ok(index)
        };

        use crate::local_chunk_store::ChunkCacheConfig;
        use std::time::Duration;

        let chunk_ttl = Duration::from_secs(args.chunk_ttl_secs);
        // Use half the sweep interval of the TTL, capped at 60 s.
        let sweep_interval = Duration::from_secs((args.chunk_ttl_secs / 2).max(10).min(60));

        let meta_index = open_index(&args.mpxar_didx)?;
        let meta_store = LocalChunkStore::with_cache_config(
            &args.pbs_store,
            crypt.clone(),
            args.verify_chunks,
            ChunkCacheConfig {
                max_entries: args.chunk_cache_size / 2,
                ttl: chunk_ttl,
            },
        );

        let payload_index = open_index(&args.ppxar_didx)?;
        let payload_store = LocalChunkStore::with_cache_config(
            &args.pbs_store,
            crypt.clone(),
            args.verify_chunks,
            ChunkCacheConfig {
                max_entries: args.chunk_cache_size / 2,
                ttl: chunk_ttl,
            },
        );

        // Clone handles for cleanup tasks before moving stores into readers.
        let meta_cleanup = meta_store.clone();
        let payload_cleanup = payload_store.clone();
        tokio::spawn(meta_cleanup.run_cleanup(sweep_interval));
        tokio::spawn(payload_cleanup.run_cleanup(sweep_interval));

        let meta_size = meta_index.index_bytes();
        let meta_reader: Reader =
            std::sync::Arc::new(ConcurrentLocalReader::new(meta_index, meta_store));

        let payload_size = payload_index.index_bytes();
        let payload_reader: Reader =
            std::sync::Arc::new(ConcurrentLocalReader::new(payload_index, payload_store));

        Accessor::new(
            pxar::PxarVariant::Split(meta_reader, (payload_reader, payload_size)),
            meta_size,
        )
        .await?
    };

    let session = Session::mount_with_cache(
        accessor,
        fuse_opts,
        args.verbose,
        mountpoint_path,
        args.lookup_cache_size,
    )
    .map_err(|err| anyhow::format_err!("pxar mount failed: {}", err))?;

    let mut interrupt = signal(SignalKind::interrupt())?;
    select! {
        res = session.fuse() => res?,
        _ = interrupt.recv().fuse() => {
            log::debug!("interrupted");
        }
    }

    Ok(())
}