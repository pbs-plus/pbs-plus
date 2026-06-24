# Kubernetes Operator

The PBS Plus Kubernetes Operator automates PVC backups by watching for annotated PVCs and creating backup agent pods.

## Prerequisites

- Kubernetes 1.24+
- [VolumeSnapshot CRD](https://kubernetes.io/docs/concepts/storage/volume-snapshots/) installed (for snapshot-based backups)
- PBS Plus server running and accessible from the cluster

## Installation

### Deploy the operator

```bash
kubectl apply -f deploy/kubernetes/operator.yaml
```

### Create a bootstrap secret

```bash
kubectl create secret generic pbs-plus-bootstrap \
  --from-literal=server-url='https://<pbs-server>:8008' \
  --from-literal=bootstrap-token='<your-bootstrap-token>' \
  --from-literal=ca-fingerprint='<sha256-hex-fingerprint>' \
  -n pbs-plus-operator
```

## Usage

### Annotate PVCs

Add `pbs-plus.io/backup` to any PVC you want backed up:

```bash
kubectl annotate pvc my-pvc pbs-plus.io/backup=true
```

The operator creates a backup agent pod that mounts the PVC and connects to PBS Plus.

### Snapshot Mode

For **ReadWriteOnce (RWO)** volumes, the operator automatically:
1. Creates a VolumeSnapshot
2. Restores it to a temporary PVC
3. Mounts the temporary PVC to the backup agent

For **ReadWriteMany (RWX)** volumes, direct mount is used by default. Enable snapshot mode explicitly:

```bash
kubectl annotate pvc my-rwx-pvc pbs-plus.io/backup=true pbs-plus.io/snapshot=true
```

### Cleanup

When a PVC's backup annotation is removed, the operator automatically:
1. Deletes the backup agent pod
2. Cleans up VolumeSnapshots and restored PVCs

## Configuration

### PVC Annotations

| Annotation | Description |
|---|---|
| `pbs-plus.io/backup=true` | Required — enables backup for this PVC |
| `pbs-plus.io/snapshot=true` | Optional — force snapshot mode (default for RWO, optional for RWX) |

### Secret Keys

| Key | Description |
|---|---|
| `server-url` | PBS Plus server URL (e.g. `https://pbs.example.com:8008`) |
| `bootstrap-token` | Bootstrap token from PBS Plus server |
| `ca-fingerprint` | SHA-256 fingerprint of server CA (hex). Pins the server certificate during bootstrap. If unset, bootstrap uses insecure TLS. |

### Operator Flags

| Flag | Default | Description |
|---|---|---|
| `--server-url` | *(required)* | PBS Plus server URL |
| `--bootstrap-token-secret` | `pbs-plus-bootstrap` | Secret containing bootstrap credentials |
| `--namespace` | `""` (all) | Namespace to watch (empty for all namespaces) |
| `--agent-image` | `ghcr.io/pbs-plus/pbs-plus-agent:latest` | Agent container image |
| `--snapshot-class` | `""` (auto) | Default VolumeSnapshotClass |
| `--metrics-addr` | `:8080` | Metrics endpoint bind address |
| `--enable-leader-election` | `false` | Enable leader election for HA |
