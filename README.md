# Proxmox Backup Server (PBS) Plus

Advanced backup features for [Proxmox Backup Server](https://pbs.proxmox.com/) — file-level backup/restore, FUSE archive mounting with journal overlays, and a Kubernetes operator.

> [!WARNING]
> Pre-1.0. Expect breaking changes on every release.

## Quick Links

| Topic | Document |
|---|---|
| How it works, architecture, ports | [Architecture](docs/architecture.md) |
| Server, agent, container, operator install | [Installation](docs/installation.md) |
| Backup/restore jobs, S3 targets, hook scripts, database dumps | [Usage](docs/usage.md) |
| Read/write FUSE mount for PBS archives | [pxar-mount](docs/pxar-mount.md) |
| K8s operator for annotated PVCs | [Kubernetes Operator](docs/kubernetes-operator.md) |
| CI workflows and E2E testing | [Development](docs/development.md) |

## Installation

**Server** — install the `.deb` on your PBS machine, then:

```bash
echo "PBS_PLUS_HOSTNAME=$(hostname -f)" >> /etc/proxmox-backup/pbs-plus/pbs-plus.env
systemctl restart pbs-plus
```

**Agent (Linux)** — install the package, set env vars, restart:

```bash
echo "PBS_PLUS_INIT_SERVER_URL=https://<pbs>:8008" >> /etc/pbs-plus-agent/registry.toml
echo "PBS_PLUS_INIT_BOOTSTRAP_TOKEN=<token>" >> /etc/pbs-plus-agent/registry.toml
echo "PBS_PLUS_HOSTNAME=$(hostname -f)" >> /etc/default/pbs-plus-agent
systemctl restart pbs-plus-agent
```

**Agent (Docker)**:

```bash
docker run -d --name pbs-plus-agent \
  --cap-add=DAC_READ_SEARCH \
  -e PBS_PLUS_INIT_SERVER_URL="https://<pbs>:8008" \
  -e PBS_PLUS_INIT_BOOTSTRAP_TOKEN="<token>" \
  -e PBS_PLUS_HOSTNAME="$(hostname -f)" \
  -v /srv/pbs-plus-agent/lib:/var/lib/pbs-plus-agent \
  -v /srv/pbs-plus-agent/log:/var/log/pbs-plus-agent \
  -v /srv/pbs-plus-agent/etc:/etc/pbs-plus-agent \
  ghcr.io/pbs-plus/pbs-plus-agent:latest
```

See [Installation](docs/installation.md) for Windows agent, Kubernetes operator, and details.

## Contributing

PRs welcome. See [Development](docs/development.md) for CI and test details.

## License

[MIT](LICENSE)
