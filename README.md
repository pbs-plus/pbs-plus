# Proxmox Backup Server (PBS) Plus

A Proxmox Backup Server (PBS) "overlay" proxy server designed to add advanced backup features, positioning PBS as a robust alternative to Veeam.

> [!WARNING]  
> This repo is currently in heavy development. Expect major changes on every release until the first stable release, `1.0.0`.
> Currently, this project is not meant to be used by others but you're free to try and do so.

## Table of Contents
- [Introduction](#introduction)
- [Features](#features)
- [Installation](#installation)
- [Usage](#usage)
- [Contributing](#contributing)
- [License](#license)

## Introduction
PBS Plus is a project focused on extending Proxmox Backup Server (PBS) with advanced features to create a more competitive backup solution, aiming to make PBS a viable alternative to Veeam. Among these enhancements is disk-to-disk file backup, integrated directly within the PBS Web UI, allowing for streamlined configuration and management of backups without requiring external cron jobs or additional scripts. Ultimately, this project aims to support bare-metal workstations with agent support for multiple operating systems, including Windows.

## Features
- Comprehensive disk-to-disk file backup as part of PBS's feature set.
- Integration with the Proxmox Backup Server Web UI, with an additional "Disk Backup" page.
- User-friendly interface and configuration.
- Windows agent support for workstations.

## Installation
To install PBS Plus:
- Install scripts for various platforms are under development.

## Usage
PBS Plus currently consists of two main components: the server and the agent. The server should be installed on the PBS machine, while agents are installed on client workstations.

### Server
- The server hosts a modified version of the PBS Web UI on port `8008` (default), featuring JavaScript modifications to add a "Disk Backup" page and additional API paths to enable enhanced functionality.
- All new features, including disk-to-disk backups, can be managed through the "Disk Backup" page.

### Agent
- Currently, only Windows agents are supported.
- The agent registers with the server on initialization, exchanging public keys for secure SSH/SFTP communication.
- The agent acts as a service, using SSH/SFTP to communicate with the server. For backups, the server opens an SFTP connection to the agent, mounts the volume to PBS via rclone, and runs `proxmox-backup-client` on the server side to perform the actual backup.

## Contributing
Contributions are welcome! Please fork the repository and create a pull request with your changes. Ensure code style consistency and include tests for any new features or bug fixes.

## License
This project is licensed under the MIT License. See the [LICENSE](LICENSE) file for more details.
