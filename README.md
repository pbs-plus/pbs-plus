# Proxmox Backup Server (PBS) Plus

A Proxmox Backup Server (PBS) "overlay" proxy server designed to add advanced backup features, positioning PBS as a robust alternative to Veeam.

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

## Planned Features/Roadmap
- [x] Execute remote backups directly from Proxmox Backup Server Web UI
- [x] File backup from bare-metal workstations with agent
- [ ] File restore to bare-metal workstations with agent
- [x] File-level exclusions for backups with agent
- [ ] Pipelining backup jobs from Web UI
- [x] Windows agent support for workstations
- [ ] Linux agent support for workstations
- [ ] Containerized agent support for Docker/Kubernetes
- [ ] Mac agent support for workstations 
- [ ] MySQL database backup/restore support
- [ ] PostgreSQL database backup/restore support
- [ ] Active Directory/LDAP backup/restore support

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

## Usage
PBS Plus currently consists of two main components: the server and the agent. The server should be installed on the PBS machine, while agents are installed on client workstations.

### Server
- The server hosts an API server for its services on port `8008` to enable enhanced functionality.
- All new features, including remote file-level backups, can be managed through the "Disk Backup" page.

### Agent
- Currently, only Windows agents are supported.
- The agent registers with the server on initialization, exchanging public keys for communication.
- The agent acts as a service, using a custom RPC (`aRPC`/Agent RPC) using [smux](https://github.com/xtaci/smux) with mTLS to communicate with the server. For backups, the server communicates with the agent over `aRPC` to deploy a `FUSE`-based filesystem, mounts the volume to PBS, and runs `proxmox-backup-client` on the server side to perform the actual backup.

## Contributing
Contributions are welcome! Please fork the repository and create a pull request with your changes. Ensure code style consistency and include tests for any new features or bug fixes.

## License
This project is licensed under the MIT License. See the [LICENSE](LICENSE) file for more details.
