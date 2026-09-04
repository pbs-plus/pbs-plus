# Usage

All PBS Plus features are managed through the **PBS Plus** navigation node in the PBS Web UI: PBS Plus Configuration, Backup / Restore, Targets, Snapshots, Data Verification, and MTF Migration.

## Disk Backup

File-level backup features are managed through the **Backup / Restore** page.

### Targets

A **target** is a backup source, managed on the tabbed **Targets** page. Each tab holds one kind:

- **Filesystem**: a registered agent host, or a local path on the PBS server. Agent targets appear automatically once an agent bootstraps with the server. Each reports hostname, OS, IP address, connection status (Reachable / Unreachable), available volumes (drives on Windows, root filesystem on Linux), and volume size (total, used, free).
- **S3**: an S3-compatible bucket (see below).
- **PostgreSQL** and **MySQL / MariaDB**: database servers (see Database Backup below).
- **LDAP / Active Directory**: directory servers backed up as LDIF (see Database Backup below).
- **Dovecot**: remote mailboxes backed up through doveadm over verified TLS (see Dovecot Mailbox Backup and Restore below).

Target size metadata is resolved without scanning target contents: local targets use `statfs`, agent targets use volume metadata reported by the agent.

### Backup Jobs

A backup job defines:

- **Target** - which target to back up
- **Datastore** - which PBS datastore receives the snapshot
- **Namespace** - PBS namespace within the datastore
- **Schedule** - cron-like expression (or empty for manual-only)
- **Exclusions** - file patterns to skip
- **Source mode** - how files are enumerated
- **Pre/Post scripts** - hook scripts run on the server before/after backup
- **Mount script** - script to run when the target filesystem is mounted
- **Max directory entries** - limit directory traversal depth
- **Retry policy** - retry count and interval

For database targets, the job also defines:

- **Scope** - `server` (every database on the server, each dumped to its own file, plus PostgreSQL globals; for LDAP, the entire base DN) or `database` (a single named database; for LDAP, a single subtree DN)
- **Database** - the database name (or subtree DN for LDAP), required for `database` scope. LDAP jobs left empty default to `server` scope.

For Dovecot targets, the job defines a required source username and an optional mailbox name. Leaving the mailbox empty backs up all mailboxes for that user.

Job edits made while a job is running are preserved: the running task is not disturbed and the next run picks up the changes.

### Scheduling

Schedules use a custom calendar expression format (parsed by `internal/calendar/`). Jobs without a schedule can be triggered manually from the UI or API.

### Exclusions

Exclusion rules filter files during backup. Rules are stored in the SQLite database and referenced by job ID. Global exclusions (shared across jobs) are managed on the PBS Plus Configuration page.

## Restore

Filesystem restores copy files from a PBS snapshot back to an agent:

1. Select a snapshot from the datastore
2. Choose source path within the snapshot
3. Choose destination target and path
4. Select restore mode (overwrite, etc.)

The agent forks a restore subprocess that pulls data over the aRPC data plane and writes to the destination.

Database restores load a dump archive back into a database server:

- Select a dump snapshot; the snapshot list is filtered by target engine so only compatible snapshots are offered
- Restore the whole server dump, or pick a single **source database** from it
- Optionally set a **destination database** name to restore under a new name
- Optionally replace existing databases

Dovecot restores select a Dovecot snapshot, source username, optional destination username, and optional mailbox. Additive mode merges the backed-up state into the destination user (the underlying one-way dsync merge is not a conflict-resolution system); **Replace Existing** mirrors the backup into the destination. Replace requires the destination user's mail storage to be fresh or empty (the disaster-recovery case): Dovecot refuses to delete and recreate an existing INBOX, so replacing into a mailbox with divergent index state fails the restore instead of partially applying it. Use additive mode for populated destinations, or clear the destination user's mail storage first.

## S3-Compatible Backup Target

> [!WARNING]
> Early implementation. Not optimized for access costs. Tested with local S3-compatible stores (Ceph, MinIO).

1. Add a target with path format: `<scheme>://<access key>@<endpoint>/<bucket>`
2. Set the secret key via **Set S3 Secret Key** button.

Example: `s3://AKIAIOSFODNN7EXAMPLE@minio.local:9000/backups`

## Hook Scripts

Hook scripts run on the PBS server, not on the agent. Script output is included in the task log.

### PreScript

Runs before backup. Can validate prerequisites and emit overrides. If it exits non-zero, the backup is aborted.

All job fields are exposed as env vars (`PBS_PLUS__<FIELD_NAME>`):

- `PBS_PLUS__JOB_ID`
- `PBS_PLUS__TARGET`
- `PBS_PLUS__NAMESPACE`
- `PBS_PLUS__STORE`
- `PBS_PLUS__COMMENT`
- and more

Output overrides via stdout as `KEY=VALUE` lines:

- `PBS_PLUS__NAMESPACE` - updates the job's namespace

Send human-readable output to stderr, not stdout.

### PostScript

Runs after backup (success or failure). Cannot change the result. Additional env vars:

- `PBS_PLUS__JOB_SUCCESS` - `"true"` or `"false"`
- `PBS_PLUS__JOB_WARNINGS` - count of warnings

### Example: Time-gated backup with namespace override

```bash
#!/usr/bin/env bash
HOUR="$(date +%H)"
if [ "$HOUR" -lt 22 ] && [ "$HOUR" -gt 5 ]; then
  echo "Backups allowed only 22:00-05:59" >&2
  exit 1
fi
SAFE_TGT="${PBS_PLUS__TARGET// /_}"
TS="$(date +%Y%m%d%H%M%S)"
echo "PBS_PLUS__NAMESPACE=Maint/${SAFE_TGT}/${TS}"
exit 0
```

### Example: Notification PostScript

```bash
#!/usr/bin/env bash
STATUS="${PBS_PLUS__JOB_SUCCESS:-false}"
WARN="${PBS_PLUS__JOB_WARNINGS:-0}"
JOB="${PBS_PLUS__JOB_ID:-unknown}"
logger -t pbs-plus "Job ${JOB}: success=${STATUS}, warnings=${WARN}"
```

## Database Backup

PostgreSQL and MySQL/MariaDB servers are backed up natively, without agents or hook scripts.

1. Add a **PostgreSQL** or **MySQL / MariaDB** target on the Targets page: host, port, username, password, TLS mode (and CA certificate if verifying), plus for MySQL the server variant (`mysql` or `mariadb`).
2. Create a backup job with scope `server` (all databases) or `database` (one named database).
3. The server selects the installed dump client (`pg_dump`, `mysqldump`, or `mariadb-dump`) whose version matches the live database server. To force a specific client, set a **default client directory** on the target; otherwise the matching client is picked automatically on every run. There are no per-job client overrides.
4. The dump is staged with a sha256 manifest and written to the datastore as a standard PBS snapshot of split pxar archives. Restore via the normal restore flow (see Restore above).

Notes:

- Server-wide dumps write each database to its own dump file, so a single database can be restored on its own. PostgreSQL server dumps also include separate roles and globals dumps.
- Dump client output is mirrored into the PBS task log.

### LDAP Logical Backup and Restore

LDAP targets capture the entries and readable attributes under a base DN as LDIF, without agents or hook scripts. Scheduled runs, retention, encryption, checksums, and snapshot history use the normal PBS backup pipeline.

1. Add an **LDAP / Active Directory** target: host, port, bind DN, password, base DN, and TLS mode (`disabled`, `starttls`, or `ldaps`). StartTLS and LDAPS require certificate verification; set a CA certificate when the system trust store does not contain the issuer.
2. Dumps use the installed `ldapsearch` client with paged results. Pin a **default client directory** to force a specific ldap-utils installation.
3. Readable user attributes, including binary values and generic LDAP password attributes, are preserved. Server-maintained attributes that cannot be replayed are stripped.
4. Restores use `ldapmodify` in add mode. Existing entries are not overwritten or merged. Entries are replayed parent-first under their original DNs, and a whole-base snapshot can restore one selected subtree.
5. **Replace Existing** first verifies that the selected subtree is present in the checked snapshot, then recursively deletes that destination subtree with `ldapdelete` before replaying it. LDAP restore does not rename DNs.

This is a logical directory-data backup, not an Active Directory disaster-recovery backup. LDAP cannot capture AD password secrets, NTDS.dit, SYSVOL, deleted objects, replication state, domain-wide security state, or every protected attribute. Use supported Windows Server System State/VSS backups for domain or forest recovery. OpenLDAP schema, `cn=config`, ACL configuration, and replication state also require the server's native backup tools when they are outside the selected base DN.

### Dovecot Mailbox Backup and Restore

Dovecot targets preserve one user's messages, folders, flags, keywords, and mailbox metadata through Dovecot's native dsync protocol. They do not require a PBS Plus agent on the mail server.

1. Configure a TLS-enabled doveadm TCP listener on the Dovecot server. Set a strong `doveadm_password`, install a server certificate whose SAN matches the target hostname, and restrict the listener to the PBS Plus host with a firewall. The shared password grants mailbox-level administrative access.
2. Add a **Dovecot** target with the listener host and port, shared doveadm password, an absolute path to the trusted CA certificate on the PBS Plus server, and optionally a pinned Dovecot client directory.
3. Create a backup job with the source username. Leave **Mailbox** empty for every mailbox, or enter one mailbox name for selective backup.
4. Restore to the original username or another destination username. Leave **Mailbox** empty to use the backup's scope. Additive restore uses one-way `doveadm sync` and never deletes destination messages; **Replace Existing** uses `doveadm backup` to mirror the backup into the destination and requires fresh or empty destination mail storage.

The PBS Plus host requires Dovecot 2.4 or newer client tools. Remote Dovecot 2.3 and 2.4 servers are supported through the doveadm protocol. For Dovecot 2.4, a minimal listener includes `doveadm_password`, `ssl_server_cert_file`, `ssl_server_key_file`, and a `service doveadm` inet listener with `ssl = yes`. Dovecot 2.3 uses its corresponding 2.3 SSL setting names. Validate the version-specific configuration with `doveconf -n`, verify the listener with `openssl s_client`, and use the upstream [Dovecot 2.4 doveadm documentation](https://doc.dovecot.org/main/core/admin/doveadm.html) or [Dovecot 2.3 dsync TCP documentation](https://doc.dovecot.org/2.3/configuration_manual/replication/).

This target covers mailbox data only. Dovecot configuration, user databases in SQL or LDAP, TLS private keys, Sieve scripts, quota/accounting systems, replication state, and full-host recovery are excluded and must be backed up separately. Additive dsync merge behavior is not a conflict-resolution system; use **Replace Existing** when the destination must match the snapshot exactly.

### Service Backup via Hook Scripts

Other services (for example OpenLDAP) can still be dumped through hook scripts: the agent's filesystem is mounted on the PBS server via FUSE, so a PreScript or Mount Script can trigger a data dump to a local path before backup begins, and a PostScript can clean up dump files after a successful backup.

Example OpenLDAP dump script:

```bash
#!/bin/bash
DUMP_DIR="/mnt/backups/ldap"
mkdir -p "$DUMP_DIR"
slapcat -l "$DUMP_DIR/config.ldif" -n 0
slapcat -l "$DUMP_DIR/data.ldif" -n 1
exit 0
```

Exit non-zero to abort the backup if the dump fails.

## Snapshot Mounts

The **Snapshots** page manages read/write FUSE mounts of PBS archives on the server (powered by `pxar-mount`, see [pxar-mount](pxar-mount.md) for the full commit workflow):

- **Active Mounts**: running mount sessions, with unmount (keeping or discarding read/write changes) and remount actions for offline sessions
- **Mount Profiles**: batch mount definitions that mount the newest snapshot of every backup group under a parent namespace (root by default), each namespace in its own directory inside one share or local root. Profiles with **auto-mount** enabled reconcile continuously: new namespaces appear, vanished ones are unmounted, and read-only mounts follow the newest snapshot when **Replace on new snapshot** is enabled. Read/write mounts are never auto-replaced. Each profile can define its own calendar-format check schedule; without one, checks run every 5 minutes.
- **Datastore tabs**: browse snapshots per datastore, mount them, or **compose** a new snapshot from selected paths of an existing archive (with optional single-directory flattening)
- **Outposts**: expose mounted snapshots as NFS or SMB shares instead of local mounts (see [Outposts](outposts.md))

Read/write overlay data is stored inside the datastore under `.pbs-plus/mount-overlays/`, a hidden directory PBS group scans skip.

## Data Verification

The **Data Verification** page defines verification jobs that check backed-up file contents against the live files on agent targets (sha256 comparison, with file filters to narrow the selection). Verification runs as a normal PBS task and requires the target agent to be connected.

## MTF Migration

The **MTF Migration** page migrates Windows Backup (BKF) / MTF-format LTO tape contents into PBS snapshots:

- **Inventory** and **Drives**: inspect tape drives and media
- **Changers**: manage SCSI media changers
- **Namespace Mappings**: map tape sets to PBS namespaces
- **Migration Jobs**: run the tape-to-pxar conversion as a scheduled or manual job

## Notifications

- **Notification Batches** (PBS Plus Configuration page): collect results from multiple jobs into a single notification. Results are persisted, so a batch survives a server restart mid-flush. Default wait window is 300 seconds.
- **Alert Settings** (PBS Plus Configuration page): alerts for unconfigured targets, stale backups, and offline targets.
