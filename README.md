# Proxmox Backup Server (PBS) Plus

A Proxmox Backup Server (PBS) "extension" designed to add advanced backup features, positioning PBS as a robust alternative to Veeam.

> [!WARNING]  
> This repo is currently in heavy development. Expect major changes on every release until the first stable release, `1.0.0`.
> Do not expect it to work perfectly (or at all) in your specific setup as I have yet to build any tests for this project yet.
> However, feel free to post issues if you think it will be helpful for the development of this project.

## Table of Contents
- [Introduction](#introduction)
- [Features](#features)
- [Installation](#installation)
- [Usage](#usage)
- [Contributing](#contributing)
- [License](#license)

## Introduction
PBS Plus is a project focused on extending Proxmox Backup Server (PBS) with advanced features to create a more competitive backup solution, aiming to make PBS a viable alternative to Veeam. Among these enhancements is remote file-level backup, integrated directly within the PBS Web UI, allowing for streamlined configuration and management of backups of bare-metal workstations without requiring external cron jobs or additional scripts.

## How does it work?
![image](https://github.com/user-attachments/assets/e9005288-b95e-44e7-b5d8-211907cfab10)


## Planned Features/Roadmap
- [x] Execute remote backups directly from Proxmox Backup Server Web UI
- [x] File backup from bare-metal workstations with agent
- [ ] File restore to bare-metal workstations with agent
- [x] File-level exclusions for backups with agent
- [x] Windows agent support for workstations
- [x] Linux agent support for workstations
- [ ] Containerized agent support for Docker/Kubernetes
- [ ] Mac agent support for workstations 
- [x] MySQL database backup/restore support (use pre-backup hook scripts to dump databases)
- [x] PostgreSQL database backup/restore support (use pre-backup hook scripts to dump databases)
- [x] Active Directory/LDAP backup/restore support (use pre-backup hook scripts to dump databases)

## Installation
To install PBS Plus:
### PBS Plus
- Install the `.deb` package in the release and install it in your Proxmox Backup Server machine.
- This will "mount" a new self-signed certificate (and custom JS files) on top of the current one. It gets "unmounted" whenever `pbs-plus` service is stopped.
- When upgrading your `proxmox-backup-server`, don't forget to stop the `pbs-plus` service first before doing so.
- You should see a modified Web UI on `https://<pbs>:8007` if installation was successful.

### Windows Agent
- In the Agent Bootstrap menu under Disk Backup, click on an existing valid token or generate a new one.
- Click on Deploy With Token while the valid token is selected. That should give you a PowerShell command. Executing that command in an elevated PowerShell should install and bootstrap the agent properly.
- Windows agents store configuration in the Windows Registry and use DPAPI for secrets. They do not use environment variables for initial configuration.
- As soon as the script finishes, you should be able to see the client as Reachable in the Targets tab. If so, you should be good to go.

### Linux Agent
- Install one of the `pbs-plus-agent` Linux packages from the Releases page and install it on your machine, or use the containerized agent below.
- In the Agent Bootstrap menu under Disk Backup, click on an existing valid token or generate a new one.
- Click on Copy Token while the valid token is selected. That should give you the token you need for the agent to establish connection.
- Configure initial settings on the agent host using environment variables (Linux uses env vars for initial config; Windows uses the registry):
  - `PBS_PLUS_INIT_SERVER_URL` — e.g. `https://<pbs-server>:8008`
  - `PBS_PLUS_INIT_BOOTSTRAP_TOKEN` — the copied token
- Restart the `pbs-plus-agent` service so it reads the env vars and initializes:
  - `systemctl restart pbs-plus-agent` (if using systemd)
- As soon as the agent starts, it should persist config to `/etc/pbs-plus-agent/registry`, bootstrap mTLS, and you should see the client as Reachable in the Targets tab. If so, you should be good to go.

### Containerized Agent (Docker/Podman/Kubernetes)
- You can run the agent as a container instead of installing a native package.
- Provide initial configuration via environment variables (Linux-style):
  - `PBS_PLUS_INIT_SERVER_URL` — e.g. `https://<pbs-server>:8008`
  - `PBS_PLUS_INIT_BOOTSTRAP_TOKEN` — the copied token
- Mount persistent volumes so state, logs, and registry survive restarts:
  - `/var/lib/pbs-plus-agent` (state)
  - `/var/log/pbs-plus-agent` (logs)
  - `/etc/pbs-plus-agent` (registry/config)
- Example (Docker):
  ```bash
  docker run -d --name pbs-plus-agent \
    --restart=unless-stopped \
    -e PBS_PLUS_INIT_SERVER_URL="https://<pbs-server>:8008" \
    -e PBS_PLUS_INIT_BOOTSTRAP_TOKEN="<your-bootstrap-token>" \
    -v /srv/pbs-plus-agent/lib:/var/lib/pbs-plus-agent \
    -v /srv/pbs-plus-agent/log:/var/log/pbs-plus-agent \
    -v /srv/pbs-plus-agent/etc:/etc/pbs-plus-agent \
    ghcr.io/pbs-plus/pbs-plus-agent:latest
  ```
- After startup, the agent bootstraps with the server and should appear as Reachable in the Targets tab.

## Usage
PBS Plus currently consists of two main components: the server and the agent. The server should be installed on the PBS machine, while agents are installed on client workstations.

### Server
- The server hosts an API server for its services on port `8017` to enable enhanced functionality.
- The server hosts another endpoint solely for agent communications with mTLS on port `8008`.
- All new features, including remote file-level backups, can be managed through the "Disk Backup" page.

### Agent
- Currently, Windows and Linux agents are supported.
- Linux agents **do not** support snapshots on backup yet.
- The agent registers with the server on initialization, exchanging public keys for communication.
- The agent acts as a service, using a custom RPC (`aRPC`/Agent RPC) using [QUIC](https://github.com/quic-go/quic-go) with mTLS to communicate with the server. For backups, the server communicates with the agent over `aRPC` to deploy a `FUSE`-based filesystem, mounts the volume to PBS, and runs `proxmox-backup-client` on the server side to perform the actual backup.

### S3-compatible backup target
> [!WARNING]  
> This is a very early implementation of S3 as backup target. This has not been optimized to lessen access fees and has only been tested on local S3-compatible implementations (Ceph, MinIO, etc.)
- It should be as simple as adding a target with the following format as path: `<scheme>://<access key>@<endpoint>/<bucket>`
- Afterwards, you can set the secret key via the `Set S3 Secret Key` button while having the newly created target selected.

### Hook scripts (Pre/Post scripts)
#### Overview

- PreScript: runs before the backup. Can validate prerequisites and optionally emit overrides (e.g., namespace). If it exits non‑zero, the backup is aborted.
- PostScript: runs after the backup (success or failure). Does not change the result; useful for notifications and cleanup.
- Communication: all job fields are provided as env vars; PreScript can emit `KEY=VALUE` overrides via stdout.

Reminders:
- Scripts run on the PBS server, not on the agent.
- Paths and dependencies must exist on the PBS server.
- Scripts must be executable and return promptly; long scripts delay the job.
- Use stdout only for `KEY=VALUE` outputs; send human-readable logs to stderr.
- PreScript failure cancels the backup; PostScript never changes the outcome.
- Network calls from scripts should handle timeouts/retries to avoid blocking the job.

#### Environment Variables

Input to scripts

All job fields are exposed as `PBS_PLUS__<FieldName>`, for example:

- `PBS_PLUS__JOB_ID`
- `PBS_PLUS__COMMENT`
- `PBS_PLUS__SOURCE_MODE`
- `PBS_PLUS__TARGET`
- `PBS_PLUS__NAMESPACE`
- …and more.

Additional to PostScript:

- `PBS_PLUS__JOB_SUCCESS` — `true` or `false`
- `PBS_PLUS__JOB_WARNINGS` — integer string count

Output from PreScript

Print overrides to stdout as `KEY=VALUE` lines:

- `PBS_PLUS__NAMESPACE` — updates the job’s namespace before the backup starts

Notes:
- Use stdout strictly for `KEY=VALUE` when emitting overrides; write human logs to stderr.
- If PreScript exits non‑zero, the backup does not proceed.

#### Sample PreScript

Require a maintenance window and set a namespace:

```bash
#!/usr/bin/env bash
# Fail outside 22:00–05:59; set a time-stamped namespace.
HOUR="$(date +%H)"
if [ "$HOUR" -lt 22 ] && [ "$HOUR" -gt 5 ]; then
  echo "Backups allowed only 22:00–05:59" >&2
  exit 1
fi

SAFE_TGT="${PBS_PLUS__TARGET// /_}"
TS="$(date +%Y%m%d%H%M%S)"
echo "PBS_PLUS__NAMESPACE=Maint/${SAFE_TGT}/${TS}"
exit 0
```

Other common “required” conditions for PreScript:
- Snapshot/quiesce gating (exit non‑zero if app quiesce fails)
- Free space/health checks (exit if below threshold)
- Access control/time windows (as above)

#### Sample PostScript

Notify result (always runs; does not change outcome):

```bash
#!/usr/bin/env bash
STATUS="${PBS_PLUS__JOB_SUCCESS:-false}"
WARN="${PBS_PLUS__JOB_WARNINGS:-0}"
JOB="${PBS_PLUS__JOB_ID:-unknown}"

MSG="Job ${JOB} completed: success=${STATUS}, warnings=${WARN}"
logger -t pbs-plus "$MSG"
exit 0
```

### aRPC QUIC window auto-tuning (server)

The server auto-sizes QUIC receive windows from system RAM and enforces sane bounds while honoring quic-go’s defaults as the minimum base.

Key env vars (sizes accept `4194304`, `4MB`, `4MiB`, etc.):

- `PBS_PLUS_ARPC_CONN_TARGET`
  - Default: `100`
  - Expected concurrent active connections used to divide the RAM budget.

- `PBS_PLUS_ARPC_RAM_FRACTION`
  - Default: `0.10` (10%), max `0.50`
  - Fraction of total RAM reserved for worst-case QUIC receive buffering across all active connections.

Floors and ceilings:

- `PBS_PLUS_ARPC_MIN_CONN_WIN`
  - Default: `15MiB` (quic-go default `MaxConnectionReceiveWindow`)
  - Minimum per-connection max window.

- `PBS_PLUS_ARPC_MIN_STREAM_WIN`
  - Default: `6MiB` (quic-go default `MaxStreamReceiveWindow`)
  - Minimum per-stream max window.

- `PBS_PLUS_ARPC_ABS_MAX_CONN_WIN`
  - Default: `256MiB`
  - Hard upper bound for per-connection max window.

- `PBS_PLUS_ARPC_ABS_MAX_STREAM_WIN`
  - Default: `64MiB`
  - Hard upper bound for per-stream max window.

Explicit overrides (optional):

- `PBS_PLUS_ARPC_MAX_CONN_WIN`
  - Force `MaxConnectionReceiveWindow` (still clamped and related to stream max).

- `PBS_PLUS_ARPC_MAX_STREAM_WIN`
  - Force `MaxStreamReceiveWindow`.

- `PBS_PLUS_ARPC_INIT_CONN_WIN`
  - Force `InitialConnectionReceiveWindow` (otherwise ≈ `min(4MiB, maxConn/4)`, ≥ `512KiB`).

- `PBS_PLUS_ARPC_INIT_STREAM_WIN`
  - Force `InitialStreamReceiveWindow` (otherwise ≈ `min(2MiB, maxStream/4)`, ≥ `512KiB`).

How sizing works (simplified):

- `totalBudget = totalRAM × PBS_PLUS_ARPC_RAM_FRACTION`
- `per-connection cap = floor(totalBudget / PBS_PLUS_ARPC_CONN_TARGET)`, clamped to `[MIN_CONN, ABS_MAX_CONN]`
- `per-stream cap = per-connection cap / 4`, clamped to `[MIN_STREAM, ABS_MAX_STREAM]`, and `≤ per-connection cap / 2`
- Initial windows derived from maxima unless explicitly set
- Relationships enforced: `Max ≥ Initial`; `MaxConn ≥ 2 × MaxStream`

Effects:

- Lower `PBS_PLUS_ARPC_CONN_TARGET` or higher `PBS_PLUS_ARPC_RAM_FRACTION` ⇒ larger per-connection caps (more throughput headroom, higher worst-case buffering).
- Increase `PBS_PLUS_ARPC_ABS_MAX_*` to allow higher ceilings; increase `PBS_PLUS_ARPC_MIN_*` to raise the minimum base.
- If `totalRAM / PBS_PLUS_ARPC_CONN_TARGET` is small, values will clamp to the minimum base (`15MiB` conn, `6MiB` stream).

## Contributing
Contributions are welcome! Please fork the repository and create a pull request with your changes. Ensure code style consistency and include tests for any new features or bug fixes.

## License
This project is licensed under the MIT License. See the [LICENSE](LICENSE) file for more details.
