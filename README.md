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
- Windows sample batch script:
```
@echo off
setlocal

REM Run as administrator check
net session >nul 2>&1
if %errorLevel% neq 0 (
    echo This script requires administrator privileges.
    echo Please run as administrator.
    pause
    exit /b 1
)

REM Set paths
set "SOURCE_PATH=\\source here\PBS Plus Agent"
set "INSTALL_DIR=%ProgramFiles(x86)%\PBS Plus Agent"

REM Test network connectivity
echo Testing network connectivity...
if not exist "%SOURCE_PATH%" (
    echo Cannot access network share at %SOURCE_PATH%
    echo Please check network connectivity and share permissions.
    pause
    exit /b 1
)

REM Create installation directory
echo Creating installation directory...
if not exist "%INSTALL_DIR%" mkdir "%INSTALL_DIR%"

REM Check and stop existing services if binaries exist
if exist "%INSTALL_DIR%\pbs-plus-agent.exe" (
    echo Stopping existing PBS Plus Agent service...
    sc stop "PBSPlusAgent" >nul 2>&1
    timeout /t 2 /nobreak >nul
)

if exist "%INSTALL_DIR%\pbs-plus-updater.exe" (
    echo Stopping existing PBS Plus Updater service...
    sc stop "PBSPlusUpdater" >nul 2>&1
    timeout /t 2 /nobreak >nul
)

REM Copy binary files with retries
echo Copying application files...
set "RETRY_COUNT=3"
set "RETRY_DELAY=5"

:COPY_AGENT
copy /Y "%SOURCE_PATH%\pbs-plus-agent.exe" "%INSTALL_DIR%" >nul 2>&1
if %errorLevel% neq 0 (
    set /a RETRY_COUNT-=1
    if %RETRY_COUNT% gtr 0 (
        echo Retrying copy of pbs-plus-agent.exe in %RETRY_DELAY% seconds...
        timeout /t %RETRY_DELAY% /nobreak >nul
        goto COPY_AGENT
    )
    echo Failed to copy pbs-plus-agent.exe after multiple attempts
    pause
    exit /b 1
)

set "RETRY_COUNT=3"
:COPY_UPDATER
copy /Y "%SOURCE_PATH%\pbs-plus-updater.exe" "%INSTALL_DIR%" >nul 2>&1
if %errorLevel% neq 0 (
    set /a RETRY_COUNT-=1
    if %RETRY_COUNT% gtr 0 (
        echo Retrying copy of pbs-plus-updater.exe in %RETRY_DELAY% seconds...
        timeout /t %RETRY_DELAY% /nobreak >nul
        goto COPY_UPDATER
    )
    echo Failed to copy pbs-plus-updater.exe after multiple attempts
    pause
    exit /b 1
)

REM Verify files were copied correctly
if not exist "%INSTALL_DIR%\pbs-plus-agent.exe" (
    echo Failed to verify pbs-plus-agent.exe
    pause
    exit /b 1
)
if not exist "%INSTALL_DIR%\pbs-plus-updater.exe" (
    echo Failed to verify pbs-plus-updater.exe
    pause
    exit /b 1
)

REM Change to installation directory
cd /d "%INSTALL_DIR%"

REM Check and install services
echo Checking PBS Plus Agent service...
sc query "PBSPlusAgent" >nul 2>&1
if %errorLevel% equ 0 (
    echo PBS Plus Agent service is already installed
) else (
    echo Installing PBS Plus Agent service...
    start "" "%INSTALL_DIR%\pbs-plus-agent.exe" install
)

echo Checking PBS Plus Updater service...
sc query "PBSPlusUpdater" >nul 2>&1
if %errorLevel% equ 0 (
    echo PBS Plus Updater service is already installed
) else (
    echo Installing PBS Plus Updater service...
    start "" "%INSTALL_DIR%\pbs-plus-updater.exe" install
)

REM Start services
echo Starting PBS Plus Agent service...
sc start "PBSPlusAgent" >nul 2>&1
if %errorLevel% equ 0 (
    echo PBS Plus Agent service started
) else (
    echo Failed to start PBS Plus Agent service, start it manually.
)

echo Starting PBS Plus Updater service...
sc start "PBSPlusUpdater" >nul 2>&1
if %errorLevel% equ 0 (
    echo PBS Plus Updater service started
) else (
    echo Failed to start PBS Plus Updater service, start it manually.
)

echo Installation completed successfully.
pause
exit /b 0
```
- Windows registry setup (.reg)
```
Windows Registry Editor Version 5.00

[HKEY_LOCAL_MACHINE\SOFTWARE\PBSPlus\Config]
"ServerURL"="https://<server ip here>:8008"
"BootstrapToken"="<token here>"
```

- **Windows installer (.msi) is available but is unreliable/untested.** I suggest using manual installation as shown above instead for now.

## Usage
PBS Plus currently consists of two main components: the server and the agent. The server should be installed on the PBS machine, while agents are installed on client workstations.

### Server
- The server hosts an API server for its services on port `8008` to enable enhanced functionality.
- All new features, including remote file-level backups, can be managed through the "Disk Backup" page.

### Agent
- Currently, only Windows agents are supported.
- The agent registers with the server on initialization, exchanging public keys for communication.
- The agent acts as a service, using a custom RPC (`aRPC`/Agent RPC) using [smux](https://github.com/xtaci/smux) with mTLS and NFS to communicate with the server. For backups, the server opens an NFS connection to the agent, mounts the volume to PBS, and runs `proxmox-backup-client` on the server side to perform the actual backup.

## Contributing
Contributions are welcome! Please fork the repository and create a pull request with your changes. Ensure code style consistency and include tests for any new features or bug fixes.

## License
This project is licensed under the MIT License. See the [LICENSE](LICENSE) file for more details.
