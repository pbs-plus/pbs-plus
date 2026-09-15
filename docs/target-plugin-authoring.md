# Target plugin authoring, hosting, and recovery

Operational companion to `docs/target-plugins.md`, which holds the architecture and the phase plan. This document covers what a publisher and an administrator actually have to do: write a plugin, sign it, host a repository, test an install, and recover a broken one.

## Writing a plugin

A plugin is a normal executable. The host starts one process per operation and hands it an already-connected Unix socket on file descriptor 3, named by `PBS_PLUS_PLUGIN_FD` (`internal/targetplugin/protocol.go:32`). There is no listener, no port, and no config file.

Go plugins use the helper in `internal/targetplugin/serve_linux.go:25`:

```go
func main() {
	if err := targetplugin.Serve(context.Background(), myplugin.Descriptor(), myplugin.Handlers()); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
```

`Serve` answers `plugin.describe` itself, rejects host protocol mismatches, and returns nil when the host closes the socket, so a clean shutdown exits 0. Handlers receive canonical CBOR payloads; `targetplugin.Request[T]` decodes and validates one. Reverse host calls go through `targetplugin.CallHost` and are limited to `host.event`, `host.scratch`, `host.agent_backup_mount`, `host.agent_restore`, and `host.lease_close`. Line-oriented tool output becomes task-log lines through `targetplugin.NewJobEventLog` (writes the section marker) or `targetplugin.NewHostEventWriter` (chooses the event level); `targetplugin.NewLeaseToken` mints the lease cleanup token.

Other languages implement the same wire format directly: smux over the inherited socket, aRPC request and response envelopes, canonical CBOR payloads bounded at 4 MiB. The Go package is a convenience, not the contract.

Required methods are `plugin.describe`, `plugin.health`, `target.validate`, and `backup.open`. Everything else in `internal/targetplugin/protocol.go:13` is optional and only called when the plugin advertises the matching behavior.

`plugin.health` answers whether the plugin process itself works. Missing external tooling is environmental and belongs in the health message, not in `Healthy: false`, because the installer refuses to install an unhealthy plugin. The first-party database plugins follow this rule (`internal/targetplugin/postgresql/plugin_linux.go:104`).

### Forms

The plugin declares three `FormSchema` values in its descriptor: target, backup options, restore options (`internal/targetplugin/schema.go:36`). The host renders them with no plugin JavaScript. Controls are `text`, `secret`, `integer`, `boolean`, `select`, `path`, `certificate_path`, `status`, and `group`. Secrets never leave the host except inside an operation payload.

Changing any schema changes the schema digest, so bump the schema version and provide the matching `target.migrate`, `backup.migrate_options`, or `restore.migrate_options` handler. Activation dry-runs migrations and keeps the old version active if any record fails.

## Signing

Everything is ECDSA P-256 over SHA-256, with base64-encoded ASN.1 signatures, matching the existing agent update path.

Generate a publisher key and print its public half:

```sh
openssl ecparam -name prime256v1 -genkey -noout -out publisher.pem
ECDSA_PRIVATE_KEY=publisher.pem go run ./cmd/signer ecdsa-pubkey
```

Sign the index and every artifact with the same tool (`cmd/signer/main.go:22`):

```sh
ECDSA_PRIVATE_KEY=publisher.pem go run ./cmd/signer ecdsa-sign index.toml index.toml.sig
ECDSA_PRIVATE_KEY=publisher.pem go run ./cmd/signer ecdsa-sign plugin-linux-amd64 plugin-linux-amd64.sig
```

The index signature covers the exact index bytes, verified before any TOML decoding (`internal/targetplugin/repository.go:236`). Reformatting a signed index invalidates it. Index bytes are capped at 4 MiB and a signature at 1024 bytes.

`publisher_key_fingerprint` is the hex SHA-256 of the publisher public key. Artifact verification requires the fingerprint to match the key the host resolved, then checks declared size, digest, and signature while streaming (`internal/targetplugin/artifact.go:20`).

## Hosting a repository

A repository is a static tree, hostable from any HTTPS location including a plain
public git repository such as GitHub. Build it with the publisher tool instead of
hand-writing the interlocked digests:

```sh
go build ./cmd/plugin-filesystem -o plugin-filesystem
openssl ecparam -name prime256v1 -genkey -noout -out publisher-key.pem
go run ./cmd/plugin-publish \
  -key publisher-key.pem -id com.example.plugins -publisher "Example Ltd" \
  -out repo-tree/ plugin-filesystem
git -C repo-tree init && git -C repo-tree add -A && git -C repo-tree commit -m publish
```

`plugin-publish` runs each binary's `plugin.describe`, writes per-release
`manifest.toml`, computes and signs digests, assembles and signs `index.toml`,
then round-trips the tree through the host's own verifier. On GitHub, push the
tree and register `https://raw.githubusercontent.com/<owner>/<repo>/main/index.toml`
as the repository URL; all artifact URLs are relative, so the tree works unchanged
at any hosting root. Do not track artifacts with Git LFS (raw hosts serve the LFS
pointer, not the binary), and pin a tag rather than `main` when immutability matters.

A repository is two static files per index plus the artifacts. No server software is required.

`index.toml`:

```toml
format_version = 1
repository_id = "com.example.plugins"

[[release]]
plugin_id = "com.example.redis"
version = "1.2.0"
publisher = "Example Ltd"
publisher_key_fingerprint = "2f1c...<64 hex chars>"
minimum_host_version = "1.3.0"
maximum_host_version = "2.0.0"
protocol = 1
target_types = ["redis"]
manifest_url = "https://plugins.example.com/redis/1.2.0/manifest.toml"
manifest_sha256 = "9ab3...<64 hex chars>"
channel = "stable"

[[release.artifact]]
os = "linux"
arch = "amd64"
url = "https://plugins.example.com/redis/1.2.0/plugin-linux-amd64"
size = 8123456
sha256 = "c41d...<64 hex chars>"
signature = "MEUCIQ...<base64 ASN.1>"
```

`index.toml.sig` holds the base64 signature of `index.toml`.

`manifest.toml` per release carries identity and the three schemas, and its SHA-256 must equal `manifest_sha256` (`internal/targetplugin/manifest.go:23`):

```toml
format_version = 1
protocol = 1
plugin_id = "com.example.redis"
version = "1.2.0"
target_types = ["redis"]
schema_sha256 = "7d20...<64 hex chars>"

[target_schema]
version = 1

[[target_schema.field]]
key = "host"
label = "Host"
control = "text"
required = true
```

`schema_sha256` is the digest the plugin itself reports, so generate it from the descriptor rather than by hand:

```go
digest, err := targetplugin.SchemaDigest(myplugin.Descriptor())
```

Unknown fields are rejected on decode, URLs must be HTTPS or relative to the index, and duplicate `plugin_id@version` entries or duplicate platforms fail validation. To withdraw a release, set `revoked = true` with a `revocation_reason` rather than deleting the entry, so hosts can explain the state.

## Installing

The first-party repository (`org.pbs-plus.plugins`, served from
`https://raw.githubusercontent.com/pbs-plus/plugins/main/index.toml`) is enabled by
default: on boot the server installs its newest compatible releases in the background
and imports existing targets, so a fresh installation needs network access to
raw.githubusercontent.com on first boot. Offline installs can stage plugin binaries
under `/usr/lib/pbs-plus/plugins`, which the same boot path registers as a local
repository; the release package itself ships no plugin binaries.

Administrators add further repositories, confirm the publisher fingerprint out of band, then install a release. The HTTP surface is under `/api2/extjs/config` (`internal/server/web/server.go:77`):

| Route                                              | Purpose                                   |
| -------------------------------------------------- | ----------------------------------------- |
| `d2d-plugin-repository`                            | list and add repositories                 |
| `d2d-plugin-repository/{repository}/refresh`       | fetch and verify the index, list releases |
| `d2d-plugin-install`                               | install one release                       |
| `d2d-installed-plugin`                             | list installed plugins and versions       |
| `d2d-installed-plugin/{plugin}/{version}/activate` | switch the active version                 |

Adding a repository only pins the key when the administrator-supplied fingerprint matches the downloaded key, so confirm it through a separate trusted channel.

Installation is atomic. The artifact is staged, verified, started, asked to describe itself and report health, and only then promoted with `RENAME_NOREPLACE` into `/var/lib/pbs-plus/plugins/<plugin-id>/<version>/` holding `plugin` and `manifest.toml` (`internal/targetplugin/install_linux.go:48`). A version directory is immutable; reinstalling the same version fails with `ErrVersionInstalled`.

First-party plugins skip the network entirely: the server package ships them in `/usr/lib/pbs-plus/plugins` and bootstrap promotes and activates them on first boot (`internal/server/plugins/builtin_linux.go:50`). Their trust anchor is the signed operating system package.

## Testing

Package tests cover the protocol, the supervisor, and the lifecycle. For a real host, `cmd/plugin-verify` installs bundled artifacts into a throwaway state directory, health-checks every plugin, creates and probes a target, opens a backup lease, and optionally writes and reads back a real snapshot:

```sh
plugin-verify \
  -artifacts /usr/lib/pbs-plus/plugins \
  -state /root/plugin-verify-state \
  -source /srv/some-directory \
  -repository 'user@pbs!token@localhost:datastore' \
  -backup-id plugin-verify
```

With `-repository` set it runs `proxmox-backup-client`, then restores the `pbs-plus-target-plugin` archive and compares the decoded snapshot metadata with the lease metadata. Run it under `unshare -m` with a tmpfs over `/var/lib/pbs-plus` to keep production state out of reach.

Plugin stderr is discarded by default. Set `PBS_PLUS_PLUGIN_STDERR=true` on the host process to forward it while debugging (`internal/targetplugin/protocol.go:35`).

## Recovery

- A failing plugin only fails its own operation. The host reaps the process, releases brokered mounts, and removes the workspace even after a crash or a hang.
- Disable a plugin to stop new operations without deleting anything. Targets and jobs stay, and their errors name the disabled plugin.
- Roll back by activating an older installed version. Activation dry-runs record migrations first and leaves the previous version active if any record fails.
- Uninstall refuses to remove the active version or to orphan installed plugins by removing their repository.
- Restoring a snapshot whose plugin version is missing produces `RestorePluginUnavailableError` naming the plugin, required version, and target type, plus the install route (`internal/server/plugins/restore_linux.go:25`).
- Pre-plugin snapshots have no metadata archive. The host maps them with fixed first-party recognizers (`internal/server/plugins/legacy_linux.go:19`); third-party plugins cannot claim them.
- If a version directory is damaged, uninstall that version and reinstall it. Nothing reads a partially promoted directory, because promotion is the last step.
