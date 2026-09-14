# Target plugin system plan

## Decision

Target plugins will be signed, out-of-process executables. They will not be linked into `pbs-plus` and will not use Go's `plugin` package.

The plugin wire protocol will reuse the existing aRPC stack instead of introducing JSON:

- aRPC request and response envelopes, method routing, errors, and raw streams
- CBOR payloads
- smux multiplexing
- an inherited Unix socket between the host and each plugin process
- TOML for signed repository indexes and install manifests
- the existing form-encoded and ExtJS API conventions at the browser boundary

The existing aRPC request and response types already carry CBOR payloads and support raw streams (`internal/arpc/call.go:11-202`). Its router dispatches named methods over CBOR (`internal/arpc/router.go:18-86`), and its transport already multiplexes calls with smux (`internal/arpc/pipe.go:17-259`). Reusing those pieces keeps one RPC model in the project while avoiding a network listener or a new serialization dependency.

Plugins are trusted administrator-installed code in version 1. Signature verification establishes publisher and artifact integrity. It does not make a plugin safe. OS sandboxing can be added later without changing the contract.

## Goals

1. Install a target type without rebuilding or restarting `pbs-plus`.
2. Express every current target through the same public plugin contract.
3. Keep target configuration, secrets, status probes, backup options, restore options, archive compatibility, cancellation, and cleanup extensible.
4. Permit repositories to be added and plugins to be browsed, installed, updated, disabled, rolled back, and removed from the management UI.
5. Keep plugins crash-isolated and make interrupted installs and operations recoverable.
6. Preserve existing targets, jobs, snapshots, and API compatibility during migration.

## Non-goals for version 1

- Running untrusted plugins.
- Plugin-supplied JavaScript or HTML.
- In-process Go plugins.
- A general extension system for unrelated PBS Plus features.
- Automatic installation or automatic major-version upgrades.
- A second agent plugin system.

## Why the boundary must include jobs and execution

A target is not only a persisted record today:

- Target normalization and kind dispatch are centralized in `internal/server/coredb/target.go:17-347`.
- Target detail storage branches by kind in `internal/server/coredb/target.go:189-347`.
- Status probing branches between agent, local, S3, and database behavior in `internal/server/application/services.go:229-289`.
- Backup source preparation branches between staged database data, staged Dovecot data, agent mounts, and S3 mounts in `internal/server/backup/source.go:92-252`.
- Restore dispatch selects database, Dovecot, agent, local, or unsupported S3 behavior in `internal/server/restore/job.go:56-77`.
- Backup and restore records contain database-specific and Dovecot-specific fields in `internal/server/coredb/backup.go:750-784` and `internal/server/coredb/restore.go:578-604`.
- Their persistence code validates exact target kinds in `internal/server/coredb/backup.go:787-837` and `internal/server/coredb/restore.go:606-650`.
- Target edit windows are selected from hard-coded kind maps in `internal/server/web/ui/management/target_panel.go:56-63` and `internal/server/web/ui/management/target_panel.go:150-157`.
- Backup and restore forms branch on exact kinds in `internal/server/web/ui/management/backup_window.go:71-102` and `internal/server/web/ui/management/restore_window.go:76-116`.

A CRUD-only plugin API would leave the hard part compiled into the host. The contract therefore owns the complete target lifecycle.

## Architecture

```text
Browser
  |
  | existing PBS/ExtJS HTTP API
  v
Plugin manager -------------------------------- Repository client
  |                                                    |
  | resolves target type, version, schema              | signed TOML index
  |                                                    | signed artifacts
  v                                                    v
Operation supervisor                              Plugin registry
  |
  | spawn one process per operation
  | inherited Unix socket + smux + aRPC/CBOR
  v
Plugin executable <---------------------------> Scoped host broker
  |                                                    |
  | creates a source/sink lease                        | scratch paths
  | stages or consumes target data                     | agent session bridge
  | emits logs/progress                                | cancellation
  v                                                    v
Local path or raw stream ----------------------> Existing backup/restore engine
```

The host remains responsible for scheduling, PBS credentials, snapshot creation, task logs, retries, notifications, and job state. A plugin is responsible for target-specific validation, probing, source preparation, destination application, and its own temporary resources.

Backup data should use local paths or aRPC raw streams, not CBOR values. The current aRPC protocol already has a raw-stream status and handshake (`internal/arpc/call.go:35-65`).

## Identity and compatibility

Each plugin declares:

- `plugin_id`: reverse-DNS publisher identity, such as `org.pbs-plus.filesystem`
- `version`: semantic version of the executable package
- `protocol`: plugin protocol major version
- one or more stable `target_type` values
- configuration schema version
- archive type and supported archive format versions
- capabilities and requested host permissions
- supported operating system and architecture artifacts

Third-party target types must be namespaced, such as `com.example.oracle`. First-party types retain `filesystem`, `s3`, `postgresql`, `mysql`, `ldap`, and `dovecot` so existing API clients and database rows continue to work.

A target records the plugin ID, the plugin version that understands its stored configuration, and the configuration schema version. A job execution records the exact plugin version it launched. A snapshot records the plugin ID, target type, and archive format version. Runtime selection must never guess from a display name.

Protocol compatibility and archive compatibility are separate:

- Protocol compatibility decides whether the host can launch the executable.
- Configuration compatibility decides whether the executable can read a target record.
- Archive compatibility decides whether a plugin can restore a snapshot.

## Local process transport

For each validate, probe, backup, restore, or migration operation, the supervisor will:

1. Resolve one exact installed plugin version.
2. Create a Unix socket pair with close-on-exec enabled.
3. Pass one socket to the child as an inherited file descriptor. No filesystem socket and no listening port are created.
4. Start smux over the socket and wrap it in an aRPC `StreamPipe`.
5. Send protocol and operation identity in the first method request; do not add a separate runtime description handshake.
6. Apply a deadline and operation-specific payload limits.
7. Close the aRPC session when the operation finishes.
8. On cancellation, request graceful cancellation, close the session, send `SIGTERM`, then send `SIGKILL` after a fixed grace period.
9. Reap the child and release every host-owned lease even if the plugin crashes.

The inherited descriptor number and protocol version may be passed through environment variables. Target configuration and secrets must not be placed in arguments or environment variables.

The aRPC package needs a local constructor around an existing `net.Conn` and smux session. It should reuse `newStreamPipe` rather than duplicate the request, response, and routing code (`internal/arpc/pipe.go:132-174`). Plugin decoding must use conservative limits instead of the current broad array limit used by the general stream pipe (`internal/arpc/pipe.go:132-157`).

One process per operation is deliberate. It avoids cross-job state, makes upgrades atomic for new operations, and turns a crashed plugin into a failed job rather than a failed server.

### Hot-path latency

The plugin layer adds one local aRPC request and response to each short operation. `plugin.describe` runs only during install, activation, or explicit metadata refresh; `plugin.health` runs only during install or an explicit health check. Neither method runs before normal validate, probe, backup, or restore calls. The first operation request carries the protocol version, complete operation envelope, normalized configuration, secret values, and job options needed by that method.

`backup.open` and `restore.open` exchange control metadata once, then the host moves bulk data through the returned local path or the existing aRPC raw stream. Structured restore uses one `restore.consume` call after host-side extraction. No file chunk, database row, progress event, or PBS command crosses a CBOR request loop. Reverse broker calls occur only for capabilities that require host-owned resources, such as the existing agent session.

Optional `backup.check` and `restore.check` calls are for explicit preflight workflows. The scheduler does not call them immediately before `backup.open`, `restore.open`, or `restore.consume`; those opening methods perform the same validation in their single request. The host may coalesce concurrent probes and briefly cache successful status results, but it does not reuse a plugin process across unrelated operations.

## Protocol methods

All method payloads and results are typed CBOR maps with integer or stable snake-case string keys. Every runtime request includes the protocol version, operation ID, idempotency key, deadline, plugin version, target type, and schema version where relevant.

### Required methods

| Method            | Purpose                                                                          | Process lifetime |
| ----------------- | -------------------------------------------------------------------------------- | ---------------- |
| `plugin.describe` | Return identity, schemas, capabilities, archive formats, and permission requests | short            |
| `target.validate` | Normalize and validate non-secret config plus secret presence                    | short            |
| `target.probe`    | Return reachability, optional size data, and a bounded diagnostic                | short            |
| `backup.open`     | Prepare and lease a readable source for the host backup engine                   | job length       |
| `restore.open`    | Prepare and lease a writable destination path when path restore is supported     | job length       |
| `restore.consume` | Apply a host-staged structured archive when consumer restore is supported        | job length       |

### Optional methods

| Method                    | Purpose                                                                |
| ------------------------- | ---------------------------------------------------------------------- |
| `target.migrate`          | Convert one stored target schema version to another                    |
| `backup.migrate_options`  | Convert one stored backup option schema version to another             |
| `restore.migrate_options` | Convert one stored restore option schema version to another            |
| `backup.check`            | Cheap preflight before the scheduler starts an expensive job           |
| `restore.check`           | Validate archive metadata and requested restore options before writing |
| `plugin.health`           | Verify runtime dependencies after install or upgrade                   |

Target deletion has no remote cleanup hook in version 1. Current target deletion only removes stored state (`internal/server/coredb/target.go:561-605`). Adding remote side effects to delete would make retries and rollback unsafe without a demonstrated need.

### Reverse host methods

A long-running plugin process can call a small host router over the same bidirectional aRPC session:

| Method                    | Purpose                                                                                  |
| ------------------------- | ---------------------------------------------------------------------------------------- |
| `host.event`              | Emit a structured log, warning, progress update, or user-safe diagnostic                 |
| `host.scratch`            | Request an operation-scoped directory with a size policy                                 |
| `host.agent_backup_mount` | Acquire the existing agent filesystem source lease for the first-party filesystem plugin |
| `host.agent_restore`      | Run the existing agent restore stream for the first-party filesystem plugin              |
| `host.lease_close`        | Release a brokered lease early                                                           |

Broker calls use an unforgeable operation token created after spawn. The token only authorizes the current target and operation. No generic database, shell, file-read, or secret-fetch broker is exposed.

The agent broker is transport infrastructure, not a target-type switch. It is needed because the server already owns persistent agent sessions. Agent backup acquires an aRPC filesystem mount (`internal/server/backup/source.go:161-199`), while agent restore checks the persistent session, starts an agent restore subprocess, waits for its data session, and serves the pxar reader over aRPC (`internal/server/restore/job.go:251-407`).

## Declarative forms

`plugin.describe` returns three declarative form schemas:

1. target configuration
2. backup job options
3. restore job options

The install manifest also contains the same public form metadata in TOML so the repository UI can preview a plugin without executing it. The executable description is authoritative after installation, and installation fails if its identity or schema digest differs from the signed manifest.

Version 1 needs only the controls required by current target types:

- text
- password or secret
- integer
- boolean
- fixed select
- path
- certificate path
- read-only status field
- field group

A field can declare requiredness, default, minimum, maximum, pattern, help text, ordering, and simple equality-based visibility conditions. There is no script expression field.

The browser renders forms from these descriptors and continues submitting normal HTTP form data. The target and job APIs convert the bounded form values to typed CBOR before storage or plugin calls. The host validates field names, types, lengths, and declared bounds. `target.validate` performs target-specific validation.

This replaces the current per-kind edit windows in `internal/server/web/ui/management/windows.go:65-209` and the kind-specific job controls in `internal/server/web/ui/management/backup_window.go:71-146` and `internal/server/web/ui/management/restore_window.go:76-155`.

## Configuration and secrets

Non-secret target configuration and plugin job options are stored as canonical CBOR blobs. Canonical encoding makes equality checks, schema migration tests, and digesting deterministic without adding JSON.

Secret fields are split before storage:

- The config blob contains no secret value.
- Each secret is stored by target name and schema field key.
- API responses return only whether a secret is configured.
- Secrets are decrypted immediately before process spawn, sent through aRPC, and cleared from host-owned buffers after the call where practical.
- Plugins must not return secrets in normalized config, errors, logs, or diagnostics.

This generalizes the existing encrypted secret functions in `internal/server/coredb/secrets_box.go:13-19` and replaces the separate S3 secret and database password accessors currently exposed from `internal/server/coredb/target.go:656-789`.

Unknown config keys are rejected on create and update. A plugin upgrade cannot silently reinterpret stored values. Schema changes require the matching target or job-option migration method and an atomic host-side migration transaction. Secret migrations receive field presence and may return key rename or delete operations, but never receive plaintext solely for migration.

New and old plugin versions can coexist. Installing a version does not move targets or jobs to it. Activation first dry-runs every required migration, saves the prior canonical CBOR values, commits all migrated target and job records in one transaction, and only then changes the active version. Any failure leaves the previous version and records active.

## Backup contract

`backup.open` receives:

- normalized target config
- resolved target secrets
- normalized backup option values
- operation workspace
- job and cancellation identifiers

It returns a lease with:

- source kind: `directory` or `raw_stream`
- source path when the kind is `directory`
- archive type and format version
- supported host features, such as subpath, exclusions, xattrs, or change detection
- cleanup token

For a directory lease, the plugin process stays alive until the host calls close. The host verifies that the returned path is either the assigned workspace, an allowed configured local path, or a brokered mount. It then runs the existing PBS backup pipeline against that path. The current command already reduces a prepared source to a `name.pxar:path` argument (`internal/server/backup/command.go:30-92`).

The host, not the plugin, controls PBS credentials and invokes `proxmox-backup-client`. Plugins cannot choose the datastore, namespace, backup ID, or command-line flags.

A small host-owned metadata archive is added to each new snapshot. It contains canonical CBOR metadata with the plugin ID, plugin version, target type, archive type, archive format version, and schema digests. The implementation spike in phase 1 must confirm the final PBS archive name and retrieval path before the format is frozen.

## Restore contract

Restore has three modes because current targets have three real shapes.

### Path destination

`restore.open` returns a writable directory lease. The host restores the selected pxar content into that path using its existing restore engine. Local filesystem and agent filesystem targets use this mode.

### Structured consumer

The host extracts the plugin-owned archive into an operation workspace, runs `restore.check`, and calls `restore.consume`. The plugin applies the data to the destination service. PostgreSQL, MySQL, LDAP, and Dovecot use this mode today through their dedicated restore paths (`internal/server/restore/database.go:29-52`, `internal/server/restore/dovecot.go:12-36`).

### Brokered stream

A plugin with the `agent_transport` permission can call `host.agent_restore`. The host drives the existing agent session and pxar raw stream, while the plugin supplies the validated agent destination request and remains the owner of the target operation. This preserves the current outbound-agent connection model without compiling agent target selection into restore dispatch (`internal/server/restore/job.go:251-407`).

Before any mode starts, the host compares snapshot archive metadata with the destination plugin's declared compatibility ranges. A missing or disabled compatible version produces an actionable error and an install link. A plugin must validate all structured archive paths and checksums before destructive work.

Existing snapshots have no generic plugin metadata. First-party plugins therefore declare legacy recognizers for their existing archive layouts. The host owns the fixed legacy mapping; third-party plugins cannot claim arbitrary legacy snapshots.

## Persistence changes

Add migrations for the following logical tables. Exact SQL names follow existing singular-domain conventions and are finalized with the migration.

### Plugin registry

- repositories: ID, name, URL, pinned public key, enabled state, refresh metadata
- installed plugins: plugin ID, active version, enabled state, repository ID
- installed versions: plugin ID, version, platform, install path, manifest bytes, SHA-256, install time, health state

### Target data

- plugin target configs: target name, plugin ID, plugin version, config schema version, canonical CBOR config
- plugin target secrets: target name, field key, encrypted value
- plugin target config history: target name, plugin version, schema version, previous CBOR config, migration time

### Job data

- backup plugin options: backup ID, schema version, canonical CBOR options
- restore plugin options: restore ID, schema version, canonical CBOR options
- job plugin executions: job ID, execution ID, plugin ID, exact plugin version, archive format, start and end state

The common `targets` row remains the stable identity and kind record. Existing detail tables remain readable during migration; the architecture already uses one common target row plus per-kind detail rows (`docs/architecture.md:151-166`).

Core database methods move from a broad union-like `Target` payload toward a common target record plus opaque typed plugin config. Legacy response fields remain populated for first-party types until their API deprecation window ends.

## Repository and installation format

### Repository index

A repository serves a versioned TOML index and detached signature. Each release entry includes:

- plugin ID and version
- publisher name and key fingerprint
- minimum and maximum host versions
- protocol major version
- target type IDs
- manifest URL and SHA-256
- per-platform artifact URL, size, SHA-256, and signature
- release channel and replacement or revocation status

The signed bytes are the exact downloaded TOML bytes. Parsing happens only after signature verification.

### Install manifest

A release manifest is bounded to 4 MiB and contains `format_version`, `protocol`, `plugin_id`, `version`, `target_types`, `schema_sha256`, and the `target_schema`, `backup_schema`, and `restore_schema` TOML tables. The manifest bytes are authenticated by the SHA-256 digest in the signed repository index, so they are parsed only after that digest matches.

`schema_sha256` is the hexadecimal SHA-256 of canonical CBOR for a map with `target`, `backup`, and `restore` keys whose values are the three form schemas. Installation validates this digest from the manifest tables, then compares the manifest identity and schema digest with `plugin.describe`. These checks are local install-time work and add no runtime operation round trips.

### Trust model

- The first-party repository key is pinned in the server package.
- Adding any other repository requires an administrator to provide or confirm its public-key fingerprint.
- Repository metadata cannot rotate its own trust root.
- A key change is a separate administrator action.
- Revocation metadata can disable future installs but cannot silently delete a working local version.

Use ECDSA P-256 signatures for the first format to match the current update verifier's preferred algorithm (`internal/agent/updater/updater.go:259-280`). Reuse the existing download discipline: stream to a temporary file, hash during download, verify before rename, and never expose a failed artifact as cached (`internal/server/web/api/agentdist/plus.go:126-210`).

### Atomic install

1. Download the signed manifest and selected platform artifact to an install transaction directory.
2. Enforce configured byte limits before and during download.
3. Verify repository signature, manifest digest, artifact digest, and artifact signature.
4. Verify manifest identity against `plugin.describe` in a time-bounded install process with no target secrets.
5. Run `plugin.health` without target secrets.
6. Rename the completed version directory atomically.
7. Commit the registry row and activate the version.
8. Keep the previous version for rollback.

Install paths are versioned, root-owned, and not writable by the plugin process. Activation changes a registry pointer, not files in place. Running operations keep using the executable they started with.

Disable blocks new operations but does not kill running jobs. Uninstall is blocked while a target config, job, or rollback slot depends on that version. Compatible snapshots produce a warning rather than a hard block because their metadata can direct the administrator to reinstall a compatible plugin later.

## Runtime safety and reliability

Version 1 enforces:

- no shell command construction for plugin launch
- absolute executable paths under the managed install root
- no secrets in argv, environment, repository metadata, API responses, or task logs
- maximum CBOR payload, nesting, map, string, byte-string, and event sizes
- maximum stderr line and total diagnostic size
- operation deadlines and idle timeouts
- bounded concurrent plugin processes globally and per plugin
- process death signal tied to the server on Linux
- deterministic cancellation escalation
- operation-scoped scratch directories with startup garbage collection
- lease cleanup owned by the host
- idempotency keys for retryable methods
- crash counters and temporary quarantine after repeated immediate failures
- atomic registry and target schema migrations

Permission declarations are shown before install and stored with the version. In version 1 they are auditable policy, not a security boundary. The UI must say that plainly.

## Host API and UI

Add management APIs for:

- list, add, refresh, disable, and remove repositories
- browse repository plugins and versions
- install, update, roll back, disable, enable, and uninstall plugins
- retrieve installed target type descriptors
- retrieve target, backup, and restore form descriptors
- inspect plugin health and recent failures

Use a new `pluginapi` package under `internal/server/web/api/`, matching the existing per-domain API package structure (`docs/CONVENTIONS.md:79-85`).

Add a Plugins panel under PBS Plus Configuration with:

- repository list and pinned key fingerprints
- available and installed plugin versions
- publisher, capabilities, permissions, checksums, and signature state
- install, update, rollback, disable, and uninstall controls
- plugin health and dependent targets

The Targets tree becomes descriptor-driven. Installed target types provide labels, icons from an allowlist, grouping, columns, and create/edit forms. No plugin JavaScript is loaded. Current hard-coded grouping and button visibility live in `internal/server/web/ui/management/target_panel.go:271-341` and `internal/server/web/ui/management/target_panel.go:535-592` and should disappear after migration.

## First-party parity plan

First-party plugins live as separate `cmd/target-plugin-*` executables. They may reuse internal implementation packages while they are built from this repository, but their code is not linked into `pbs-plus`.

The aRPC/CBOR wire format and golden fixtures are the public contract. A small Go SDK may wrap them, but installation cannot require that a plugin be written in Go. smux and CBOR interoperability are part of the repository conformance check.

| Plugin        | Target config                                                 | Probe                         | Backup                   | Restore                                      | Special host service               |
| ------------- | ------------------------------------------------------------- | ----------------------------- | ------------------------ | -------------------------------------------- | ---------------------------------- |
| filesystem    | local or agent access, path, agent host                       | statfs or agent               | readable path lease      | writable local path or brokered agent stream | scoped agent backup/restore broker |
| S3            | endpoint, bucket, region, prefix, TLS, addressing, access key | TCP/TLS and bucket check      | S3 FUSE path lease       | unsupported, preserving current behavior     | none                               |
| PostgreSQL    | host, port, username, TLS, CA, client selection               | TCP plus client preflight     | staged dump directory    | structured consumer                          | none                               |
| MySQL/MariaDB | PostgreSQL fields plus server/client family                   | TCP plus client preflight     | staged dump directory    | structured consumer                          | none                               |
| LDAP          | host, port, bind user, TLS, CA, base DN, client selection     | TCP/TLS plus client preflight | staged LDIF directory    | structured consumer                          | none                               |
| Dovecot       | listener, password, CA, client selection                      | TCP/TLS plus client preflight | staged mailbox directory | structured consumer                          | none                               |

The database and Dovecot plugins initially move existing orchestration behind the contract rather than rewrite it. Their current staging implementations already expose archive directories (`internal/server/backup/source.go:111-160`), which matches `backup.open`.

Parity means the existing target-specific switch statements are removed from target persistence, status, backup source preparation, restore dispatch, and job option handling. Fixed legacy decoding and the agent transport broker may remain in the host.

## Implementation phases

### Phase 0: Freeze the contract

- Write an ADR for out-of-process aRPC/CBOR plugins and the trusted-code model.
- Define canonical CBOR request and result structs, method names, limits, error codes, cancellation, and idempotency rules.
- Define TOML manifest and repository index formats plus signature input bytes.
- Spike a local aRPC connection over an inherited Unix socket.
- Confirm the PBS metadata archive name and retrieval path.
- Build a protocol conformance test executable that intentionally times out, crashes, emits oversized data, and leaks a lease.

Exit: wire fixtures round-trip deterministically and failure cleanup tests pass.

### Phase 1: Runtime and registry

- Add local `net.Conn` constructors to `internal/arpc` without changing the agent transport.
- Add the plugin supervisor, bounded process pool, event bridge, cancellation, and cleanup registry.
- Add repository, installed-version, and health persistence.
- Add signed TOML refresh and atomic artifact installation.
- Add CLI or internal admin operations before exposing the UI.
- Ship a minimal test plugin used only by integration tests.

Exit: install, invoke, crash, cancel, upgrade, rollback, disable, and uninstall tests pass without a server crash or leaked process.

### Phase 2: Generic target model and UI

- Add generic target config and secret tables.
- Add target, backup, and restore schema descriptors.
- Add descriptor-driven API parsing and ExtJS form rendering.
- Add plugin repository and installation UI.
- Keep old detail tables and response fields as a compatibility read path.

Exit: an external test plugin can define a target and job forms, store secrets, probe status, and survive a server restart without target-specific host code.

### Phase 3: Backup and restore execution

- Replace source type dispatch with `backup.open` leases.
- Add host-owned snapshot plugin metadata.
- Add path-destination, structured-consumer, and brokered-stream restore modes.
- Add archive compatibility selection and install guidance.
- Add cancellation, retry, task-log, and cleanup failure injection tests.

Exit: a third-party test plugin completes backup and restore through the normal scheduler and task engine.

### Phase 4: First-party migration

Migrate in increasing integration difficulty:

1. local filesystem
2. S3
3. PostgreSQL
4. MySQL/MariaDB
5. LDAP
6. Dovecot
7. agent filesystem

For each plugin:

- add golden descriptor and config migration tests
- import existing target rows without changing public kind values
- compare old and plugin backup artifacts
- restore both old and new snapshots
- switch one type to plugin execution behind a feature flag
- remove its old host dispatch only after parity tests pass

Exit: all existing target types run through external executables and the server contains no execution switch on those kinds.

### Phase 5: Default enablement and cleanup

- Ship signed first-party plugin artifacts in the server package and activate them during package installation or first boot without requiring repository network access.
- Enable plugin execution by default for migrated installations.
- retain legacy field emission for the documented compatibility window
- remove old detail writes after one release with successful migration telemetry
- document repository hosting, signing, development, testing, and recovery

Exit: a fresh installation receives first-party target support only through separately packaged plugin artifacts.

## Acceptance criteria

The system is ready only when all of the following are true:

1. Installing a signed external plugin adds a target type without rebuilding or restarting the server.
2. No plugin artifact is activated before its signature, hash, identity, protocol, and health checks pass.
3. A killed, hung, malformed, or oversized plugin fails only its operation and leaves no process, mount, scratch directory, or active lease.
4. Secrets never appear in target reads, process arguments, environment variables, repository metadata, or logs.
5. Plugin-defined target, backup, and restore forms require no plugin JavaScript.
6. A snapshot records enough metadata to select a compatible restore plugin years later.
7. Upgrade and rollback preserve stored target configuration or fail before activation.
8. Disabled or missing plugins produce actionable target and restore errors rather than corrupting records.
9. Every existing target passes create, edit, secret rotation, probe, backup, cancellation, restore where currently supported, upgrade, rollback, and uninstall-protection tests through the plugin path.
10. The host has no target-kind execution branches after first-party migration, except explicit legacy snapshot recognition and the generic agent transport broker.

## Deliberately deferred

- Enforced per-plugin Linux sandbox profiles. Add when the repository accepts publishers beyond explicitly trusted administrators.
- Agent-side plugin deployment. Add when a target cannot be implemented through server execution or the scoped agent filesystem broker.
- Arbitrary form widgets or scripts. Add only when a real target cannot fit the bounded declarative controls.
- Remote cleanup hooks on target deletion. Add only with an idempotent resource ownership model.
- Cross-host plugin clustering. Add when PBS Plus itself has a clustered control plane.
