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

use crate::direct_dynamic_index::{DirectDynamicReader, DirectLocalDynamicReadAt};
use crate::fuse_session::{Accessor, Reader, Session};
use crate::local_chunk_store::LocalChunkStore;

use pbs_datastore::dynamic_index::DynamicIndexReader;
use pbs_key_config::load_and_decrypt_key;
use pbs_tools::crypt_config::CryptConfig;

struct Args {
    mountpoint: String,
    verbose: bool,
    // Datastore + two dynamic indexes:
    pbs_store: String,
    mpxar_didx: String,
    ppxar_didx: String,
    keyfile: Option<String>,
    verify_chunks: bool,
    options: String,
}

fn print_usage() {
    eprintln!("Usage (datastore-only, no local .mpxar needed):");
    eprintln!("  pxar-direct-mount --pbs-store <datastore> --mpxar-didx <path/to/mpxar.didx> --ppxar-didx <path/to/ppxar.didx> <mountpoint> [--keyfile <path>] [--verify-chunks] [--options <opts>] [--verbose]");
    eprintln!("Options:");
    eprintln!("  --pbs-store <DIR>        PBS datastore root (contains .chunks)");
    eprintln!("  --mpxar-didx <FILE>      Path to metadata dynamic index (mpxar.didx)");
    eprintln!("  --ppxar-didx <FILE>      Path to payload dynamic index (ppxar.didx)");
    eprintln!("  --keyfile <FILE>         Path to encryption key (if backup is encrypted)");
    eprintln!("  --verify-chunks          Verify chunk SHA256 on read");
    eprintln!("  --options <STR>          FUSE options (default: ro,default_permissions)");
    eprintln!("  --verbose                Verbose logging/foreground");
    eprintln!();
    eprintln!("Legacy modes:");
    eprintln!("  Unified pxar: pbs-pxar-mount <archive.pxar> <mountpoint>");
    eprintln!("  Split local files: pbs-pxar-mount <archive.mpxar> <mountpoint> --payload-input <archive.ppxar>");
}

fn parse_args() -> Result<(Option<String>, Option<String>, Args), Error> {
    // We support both legacy positional modes and datastore-only mode.
    // Modes:
    //  A) Unified pxar: <archive.pxar> <mountpoint>
    //  B) Split local:  <archive.mpxar> <mountpoint> --payload-input <ppxar>
    //  C) Datastore:    --pbs-store <dir> --mpxar-didx <file> --ppxar-didx <file> <mountpoint>
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
            _ => positional.push(arg),
        }
    }

    // Determine mode by presence of flags
    let ds_mode = pbs_store.is_some() || mpxar_didx.is_some() || ppxar_didx.is_some();

    if ds_mode {
        // Datastore mode: require pbs_store, mpxar_didx, ppxar_didx and 1 positional (mountpoint)
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
        };
        // Return None for legacy archive path, and payload_input for split-local may be None
        return Ok((None, payload_input, args));
    }

    // Legacy modes
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
        verify_chunks, // ignored in legacy path
        options,
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

    // If archive_opt is Some => legacy modes
    // If archive_opt is None => datastore mode
    let mountpoint_path = Path::new(&args.mountpoint);
    let fuse_opts = OsStr::new(&args.options);

    // Build pxar accessor
    let accessor: Accessor = if let Some(archive_path) = archive_opt {
        // Legacy: unified or split local
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
        // Datastore mode: both streams from didx + .chunks
        // Load key if provided
        let crypt = if let Some(ref keyfile) = args.keyfile {
            let (key, _, _fp) = load_and_decrypt_key(Path::new(keyfile), &get_key_password)?;
            Some(std::sync::Arc::new(CryptConfig::new(key)?))
        } else {
            None
        };

        // Helper to decode a didx blob into a tempfile and parse the DynamicIndexReader
        let open_index = |path: &str| -> Result<DynamicIndexReader, Error> {
            let file = std::fs::File::open(path)
                .with_context(|| format!("failed to open didx {}", path))?;
            let index = DynamicIndexReader::new(file)
                .map_err(|e| Error::msg(format!("unable to read dynamic index '{}': {e}", path)))?;
            Ok(index)
        };

        // Build metadata reader from mpxar.didx
        let meta_index = open_index(&args.mpxar_didx)?;
        let meta_store = LocalChunkStore::new(&args.pbs_store, crypt.clone(), args.verify_chunks);
        let meta_buffered = DirectDynamicReader::new(meta_index, meta_store);
        let meta_size = meta_buffered.archive_size();
        let meta_reader = DirectLocalDynamicReadAt::new(meta_buffered);
        let meta_reader: Reader = std::sync::Arc::new(meta_reader);

        // Build payload reader from ppxar.didx
        let payload_index = open_index(&args.ppxar_didx)?;
        let payload_store =
            LocalChunkStore::new(&args.pbs_store, crypt.clone(), args.verify_chunks);
        let payload_buffered = DirectDynamicReader::new(payload_index, payload_store);
        let payload_size = payload_buffered.archive_size();
        let payload_reader = DirectLocalDynamicReadAt::new(payload_buffered);
        let payload_reader: Reader = std::sync::Arc::new(payload_reader);

        // Split pxar from two dynamic readers
        Accessor::new(
            pxar::PxarVariant::Split(meta_reader, (payload_reader, payload_size)),
            meta_size,
        )
        .await?
    };

    let session = Session::mount(accessor, fuse_opts, args.verbose, mountpoint_path)
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
