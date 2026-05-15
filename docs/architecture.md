# Architecture

PBS Plus extends a stock Proxmox Backup Server with file-level backup/restore capabilities and a read/write FUSE mount for pxar archives. It runs as a sidecar service on the PBS host and communicates with agents on target machines.

## Components

| Component | Binary | Runs on |
|---|---|---|
| PBS Plus server | `pbs-plus` | PBS host (Linux) |
| Agent (Windows) | `pbs-plus-agent.exe` | Target workstations |
| Agent (Linux) | `pbs-plus-agent` | Target workstations / containers |
| Kubernetes operator | `pbs-plus-operator` | Kubernetes cluster |
| pxar-mount | `pxar-mount` | PBS host (bundled with server) |

## Network Topology

```
┌─────────────────────────────────────────────────┐
│               PBS Plus Server                    │
│                                                  │
│  TCP/8007 ── Modified PBS Web UI (proxied)       │
│  TCP/8017 ── PBS Plus API (jobs, targets, etc.)  │
│  TCP/8018 ── Agent HTTP (bootstrap, renewal)     │
│  UDP/8008 ── aRPC control plane (QUIC + mTLS)   │
│  TCP/8008 ── aRPC data plane (mTLS + smux)      │
└────────────┬──────────────────────────┬──────────┘
             │ QUIC (persistent)        │ TCP (on-demand)
             │ status, commands,        │ backup/restore
             │ file-tree browsing       │ binary streams
     ┌───────▼────────┐        ┌───────▼────────┐
     │  Agent (Linux)  │        │ Agent (Windows) │
     └────────────────┘        └────────────────┘
```

### Ports

| Port | Protocol | Purpose |
|---|---|---|
| 8007 | HTTPS | Proxied PBS Web UI with Disk Backup UI injected |
| 8017 | HTTPS | PBS Plus REST API — job CRUD, token management, metrics |
| 8018 | HTTPS | Agent HTTP endpoint — bootstrap, certificate renewal, drive status |
| 8008 | QUIC (UDP) | aRPC control plane — persistent mTLS connection for status, commands, file-tree browsing |
| 8008 | TCP+mTLS | aRPC data plane — temporary smux streams for backup/restore data |

## How Backup Works

1. User creates a **backup job** via the Disk Backup UI (or API) specifying a target, datastore, schedule, and optional exclusion rules.
2. The **scheduler** fires the job. The server sends a backup command to the agent over the QUIC control plane.
3. The agent **forks a subprocess** that establishes a TCP+mTLS+smux data connection to the server.
4. The server mounts the agent's filesystem via **FUSE over aRPC** (`arpcfs`), making the remote files appear as a local directory.
5. The server runs `proxmox-backup-client` against the FUSE mount to create a standard PBS snapshot in the datastore.
6. On completion, the FUSE mount is torn down and the agent subprocess exits.

## How Restore Works

1. User creates a **restore job** specifying snapshot, source path, and destination target.
2. The server sends a restore command over QUIC. The agent forks a subprocess.
3. A TCP+mTLS+smux data connection is established.
4. The agent's restore subprocess pulls file data from the server and writes it to the destination path on the agent host.
5. Integrity is verified via sha256 checksums.

## Internal Packages

```
internal/
├── agent/          # Agent binary code
│   ├── agentfs/    # Filesystem enumeration (Windows drives, Unix root)
│   ├── cli/        # Command-line modes (backup, restore, file-tree)
│   ├── lifecycle/  # Service lifecycle management
│   ├── migration/  # Registry format migrations
│   ├── registry/   # Config storage (TOML on Unix, registry on Windows)
│   ├── snapshots/  # Block-level snapshot support (Windows VSS)
│   └── sync/       # Volume status synchronization
├── arpc/           # Agent RPC — QUIC control plane + TCP data plane
├── calendar/       # Schedule expression parser
├── conf/           # Environment config singleton
├── mtls/           # mTLS certificate management
├── operator/       # Kubernetes operator controller
├── pxar/           # Pxar format reader, client, restore logic
├── pxarmount/      # FUSE filesystem + journal overlay + commit engine
├── safemap/        # Thread-safe generic map
├── server/         # Server binary code
│   ├── application/  # Business logic services (backup, restore, targets, etc.)
│   ├── backup/       # Backup orchestration and FUSE mount management
│   ├── database/     # SQLite database (migrations, queries, sqlc generated code)
│   ├── jobs/         # Job scheduling and execution
│   ├── mount/        # pxar-mount management
│   ├── proxmox/      # PBS integration (datastore ops, UPID parsing)
│   ├── restore/      # Restore orchestration
│   ├── rpc/          # Server-side aRPC handlers
│   ├── scheduler/    # Cron-like scheduler
│   ├── store/        # Central store (DB + services + agent manager)
│   └── vfs/          # Virtual filesystem layers
│       ├── arpcfs/     # FUSE filesystem over aRPC (mounts remote agent FS)
│       ├── s3fs/       # S3-compatible backup target filesystem
│       └── sessions/   # smux session management
├── syslog/         # Structured logging
└── validate/       # Input validation helpers
```

## Database

The server uses SQLite (via `modernc.org/sqlite` — no CGo) for job, target, token, exclusion, and script storage. Migrations live in `internal/server/database/migrations/`. Query code is generated by sqlc (`sqlc.yaml`).

## Web UI

PBS Plus injects custom JavaScript into the stock PBS web interface:

- **Pre-load scripts** (`views/pre/`): initialization, utilities, log viewer, task viewer
- **Custom scripts** (`views/custom/`): navigation, Disk Backup UI, snapshot mount UI, data models
- **Panels/Windows**: job editors, target selectors, backup/restore dialogs

The injection works by proxying port 8007 and prepending the custom JS to PBS responses. The proxy is torn down when `pbs-plus` is stopped.
