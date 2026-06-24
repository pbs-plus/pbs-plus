# Installation

## Server

### Requirements

- Proxmox Backup Server (Debian-based)
- The `pbs-plus` binary or `.deb` package from [Releases](https://github.com/pbs-plus/pbs-plus/releases)

### Steps

1. Install the `.deb` package on your PBS machine.

2. Configure the hostname in `/etc/proxmox-backup/pbs-plus/pbs-plus.env`:

```bash
PBS_PLUS_HOSTNAME=pbs.example.com
```

3. Restart the service:

```bash
systemctl restart pbs-plus
```

`pxar-mount` is bundled with the server binary — no separate install needed.

### What it does

- Proxies port 8007, injecting custom JS into the PBS Web UI.
- Starts the PBS Plus API on port 8017.
- Starts the agent HTTP endpoint on port 8018.
- Listens for aRPC connections on port 8008 (both UDP/QUIC and TCP).

### Upgrading PBS

Always stop `pbs-plus` before upgrading `proxmox-backup-server`:

```bash
systemctl stop pbs-plus
apt upgrade proxmox-backup-server
systemctl start pbs-plus
```

---

## Windows Agent

1. Open the PBS Plus Web UI → **Disk Backup** → **Agent Bootstrap**.
2. Generate a new token or select an existing one.
3. Click **Show Fingerprint** to view the server CA certificate fingerprint — note this for Linux/container/K8s deployments.
4. Click **Deploy With Token** — this gives you a PowerShell command that includes the CA fingerprint automatically.
5. Run the command in an elevated PowerShell.

Windows agents store config in the Windows Registry and encrypt secrets with DPAPI. The only required env var is:

- `PBS_PLUS_HOSTNAME` — unique hostname/FQDN for mTLS (set before first start).

After install, the agent should appear as **Reachable** in the Targets tab.

---

## Linux Agent

### Package install

1. Install the `pbs-plus-agent` package from [Releases](https://github.com/pbs-plus/pbs-plus/releases).

2. Configure initial connection via env vars:

```bash
# /etc/default/pbs-plus-agent or your service override
PBS_PLUS_INIT_SERVER_URL="https://pbs.example.com:8008"
PBS_PLUS_INIT_BOOTSTRAP_TOKEN="<token>"
PBS_PLUS_INIT_SERVER_CA_FINGERPRINT="<sha256-hex-fingerprint>"
PBS_PLUS_HOSTNAME="$(hostname -f)"
```

3. Restart:

```bash
systemctl restart pbs-plus-agent
```

On first start, the agent:
- Persists config to `/etc/pbs-plus-agent/registry.toml`
- Bootstraps mTLS with the server
- Should appear as **Reachable** in the Targets tab

### Registry

Linux agents store all config in `/etc/pbs-plus-agent/registry.toml`. Secrets are encrypted at rest with AES-GCM using a key stored at `/etc/pbs-plus-agent/.registry.key`.

Initial env vars (`PBS_PLUS_INIT_SERVER_URL`, `PBS_PLUS_INIT_BOOTSTRAP_TOKEN`, `PBS_PLUS_INIT_SERVER_CA_FINGERPRINT`) are consumed on first start only — after that, config is read from the TOML file.

To obtain the CA fingerprint, click **Show Fingerprint** in the Agent Bootstrap panel, or run:

```bash
openssl x509 -in /etc/proxmox-backup/pbs-plus/ca.pem -noout -fingerprint -sha256 | cut -d= -f2
```

---

## Containerized Agent (Docker / Podman)

```bash
docker run -d --name pbs-plus-agent \
  --cap-add=DAC_READ_SEARCH \
  -e PBS_PLUS_INIT_SERVER_URL="https://pbs.example.com:8008" \
  -e PBS_PLUS_INIT_BOOTSTRAP_TOKEN="<token>" \
  -e PBS_PLUS_INIT_SERVER_CA_FINGERPRINT="<sha256-hex-fingerprint>" \
  -e PBS_PLUS_HOSTNAME="my-host" \
  -v /srv/pbs-plus-agent/lib:/var/lib/pbs-plus-agent \
  -v /srv/pbs-plus-agent/log:/var/log/pbs-plus-agent \
  -v /srv/pbs-plus-agent/etc:/etc/pbs-plus-agent \
  ghcr.io/pbs-plus/pbs-plus-agent:latest
```

### Volumes

| Path | Purpose |
|---|---|
| `/var/lib/pbs-plus-agent` | State (mTLS certs, runtime data) |
| `/var/log/pbs-plus-agent` | Logs |
| `/etc/pbs-plus-agent` | Registry/config |

### Capabilities

- `DAC_READ_SEARCH` — may be required for read access to the host filesystem.
- `SYS_ADMIN` + `/dev/fuse` — required if you need FUSE support inside the container.

### Environment Variables

| Variable | Required | Description |
|---|---|---|
| `PBS_PLUS_INIT_SERVER_URL` | Yes | Server URL, e.g. `https://pbs:8008` |
| `PBS_PLUS_INIT_BOOTSTRAP_TOKEN` | Yes | Bootstrap token from the UI |
| `PBS_PLUS_INIT_SERVER_CA_FINGERPRINT` | No | SHA-256 fingerprint of the server CA (hex). Pins the server certificate during bootstrap, preventing MITM attacks. If unset, bootstrap falls back to insecure TLS (not recommended). |
| `PBS_PLUS_HOSTNAME` | Yes | Unique hostname for this agent |
| `PBS_PLUS_DISABLE_AUTO_UPDATE` | No | Set to `true` in containers |
| `PBS_PLUS__I_AM_INSIDE_CONTAINER` | No | Set to `true` in containers (auto-set in official image) |
| `PUID` / `PGID` | No | Run agent as specific UID/GID |

---

## Kubernetes Operator

See [Kubernetes Operator](kubernetes-operator.md) for full details.
