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
- In the `Agent Bootstrap` menu under `Disk Backup`, click on an existing valid token or generate a new one.
- Click on `Deploy With Token` while the valid token is selected. That should give you a Powershell command. Executing that command in an elevated Powershell should install the agent properly.
- If you're not seeing the `Deploy With Token` button, try doing hard refresh (shift + refresh button on Chromium-based browsers) as it's probably using a cached version of the page.
- As soon as the script finishes, you should be able to see the client as "Reachable" in the `Targets` tab. If so, then you should be good to go.

### Linux Agent
- Install one of the pbs-plus-agent Linux package in the release and install it in your machine.
- In the `Agent Bootstrap` menu under `Disk Backup`, click on an existing valid token or generate a new one.
- Click on `Copy Token` while the valid token is selected. That should give you the token you need for the agent to establish connection.
- In your target machine, modify `/etc/pbs-plus-agent/registry/software/pbsplus/config.json`.
- In the `config.json` file, place the copied token under `BootstrapToken` and place your server URL under `ServerURL` with port `8008`. (e.g. `https://10.1.0.2:8008`)
- Do a restart of the `pbs-plus-agent` service (`systemctl restart pbs-plus-agent` if you're using systemd).

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

## Hook scripts (Pre/Post scripts)

### Overview

- **PreScript**: runs before the backup. Can inspect job fields and emit overrides (e.g. namespace).
- **PostScript**: runs after the backup. Can perform cleanup or external calls using data set during PreScript.
- **Communication**: via environment variables and a temporary “env file” whose path is passed as the first argument to every script.

### Environment Variables

#### Input to Scripts

All job fields are exposed as `PBS_PLUS__<FieldName>`:

- `PBS_PLUS__JOB_ID`  
- `PBS_PLUS__COMMENT`  
- `PBS_PLUS__SOURCE_MODE`  
- `PBS_PLUS__TARGET`  
- …and more.

#### Output from Scripts

Scripts write to the temp-env file one `KEY=VALUE` per line. Common override:

- `PBS_PLUS__NAMESPACE` — changes where the manager updates the job’s namespace.

### Sample PreScript

```bash
#!/usr/bin/env bash
#
# Pre-backup hook: override the job’s namespace
# $1 = path to temp-env file

ENV_FILE="$1"

# Read the comment field; default to "N/A"
JOB_COMMENT="${PBS_PLUS__COMMENT:-N/A}"

# Build namespace (e.g. "Comment/Hello-World")
SAFE_COMMENT="${JOB_COMMENT// /_}"       # replace spaces with underscores
NSPACE="Comment/${SAFE_COMMENT}"

# Optionally, stamp a timestamp:
TS="$(date +%Y%m%d%H%M%S)"
NSPACE="${NSPACE}/${TS}"

# Emit override
echo "PBS_PLUS__NAMESPACE=${NSPACE}" > "$ENV_FILE"

exit 0
```

### Sample PostScript

```bash
#!/usr/bin/env bash
#
# Post-backup hook: log completion
# $1 = path to temp-env file (ignored here)

LOG_DIR="/var/log/pbs-plus"
mkdir -p "$LOG_DIR"

# Read vars set by Go
JOB_ID="${PBS_PLUS__JOB_ID:-unknown}"
NSPACE="${PBS_PLUS__NAMESPACE:-not-set}"
END_TS="$(date '+%Y-%m-%d %H:%M:%S')"

# Append to log
echo "${END_TS}: job ${JOB_ID} finished in namespace ${NSPACE}" \
    >> "${LOG_DIR}/backup.log"

exit 0
```

## Contributing
Contributions are welcome! Please fork the repository and create a pull request with your changes. Ensure code style consistency and include tests for any new features or bug fixes.

## License
This project is licensed under the MIT License. See the [LICENSE](LICENSE) file for more details.
