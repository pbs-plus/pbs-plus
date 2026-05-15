# Usage

## Disk Backup

All file-level backup features are managed through the **Disk Backup** page in the PBS Web UI.

### Targets

A **target** is a registered agent host. Targets appear automatically once an agent bootstraps with the server. Each target reports:
- Hostname, OS, IP address
- Connection status (Reachable / Unreachable)
- Available volumes (drives on Windows, root filesystem on Linux)

### Backup Jobs

A backup job defines:
- **Target** — which agent to back up
- **Datastore** — which PBS datastore receives the snapshot
- **Namespace** — PBS namespace within the datastore
- **Schedule** — cron-like expression (or empty for manual-only)
- **Exclusions** — file patterns to skip
- **Source mode** — how files are enumerated
- **Pre/Post scripts** — hook scripts run on the server before/after backup
- **Mount script** — script to run when the target filesystem is mounted
- **Max directory entries** — limit directory traversal depth
- **Retry policy** — retry count and interval

### Scheduling

Schedules use a custom calendar expression format (parsed by `internal/calendar/`). Jobs without a schedule can be triggered manually from the UI or API.

### Exclusions

Exclusion rules filter files during backup. Rules are stored in the SQLite database and referenced by job ID.

## Restore

Restore jobs copy files from a PBS snapshot back to an agent:

1. Select a snapshot from the datastore
2. Choose source path within the snapshot
3. Choose destination target and path
4. Select restore mode (overwrite, etc.)

The agent forks a restore subprocess that pulls data over the aRPC data plane and writes to the destination.

## S3-Compatible Backup Target

> [!WARNING]
> Early implementation. Not optimized for access costs. Tested with local S3-compatible stores (Ceph, MinIO).

1. Add a target with path format: `<scheme>://<access key>@<endpoint>/<bucket>`
2. Set the secret key via **Set S3 Secret Key** button.

Example: `s3://AKIAIOSFODNN7EXAMPLE@minio.local:9000/backups`

## Hook Scripts

Hook scripts run on the PBS server, not on the agent.

### PreScript

Runs before backup. Can validate prerequisites and emit overrides. If it exits non-zero, the backup is aborted.

All job fields are exposed as env vars (`PBS_PLUS__<FIELD_NAME>`):

- `PBS_PLUS__JOB_ID`
- `PBS_PLUS__TARGET`
- `PBS_PLUS__NAMESPACE`
- `PBS_PLUS__STORE`
- `PBS_PLUS__COMMENT`
- …and more

Output overrides via stdout as `KEY=VALUE` lines:

- `PBS_PLUS__NAMESPACE` — updates the job's namespace

Send human-readable output to stderr, not stdout.

### PostScript

Runs after backup (success or failure). Cannot change the result. Additional env vars:

- `PBS_PLUS__JOB_SUCCESS` — `"true"` or `"false"`
- `PBS_PLUS__JOB_WARNINGS` — count of warnings

### Example: Time-gated backup with namespace override

```bash
#!/usr/bin/env bash
HOUR="$(date +%H)"
if [ "$HOUR" -lt 22 ] && [ "$HOUR" -gt 5 ]; then
  echo "Backups allowed only 22:00–05:59" >&2
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

## Database / Service Backup

PBS Plus can back up databases and directory services using hook scripts. Since the agent's filesystem is mounted on the PBS server via FUSE, a PreScript or Mount Script can trigger a data dump to a local path before backup begins.

### Flow

1. **PreScript** runs on the PBS server
2. Script connects to the database and exports data to a directory
3. PBS Plus backs up that directory as part of the job
4. **PostScript** can clean up dump files after successful backup

### PostgreSQL

```bash
#!/bin/bash
HOST="localhost" PORT="5432" USER="postgres"
export PGPASSWORD="your_password"
DUMP_DIR="/mnt/backups/postgres"
mkdir -p "$DUMP_DIR"
DATABASES=$(psql -h "$HOST" -p "$PORT" -U "$USER" -Atc \
  "SELECT datname FROM pg_database WHERE datistemplate = false AND datname != 'postgres';")
for DB in $DATABASES; do
  pg_dump -h "$HOST" -p "$PORT" -U "$USER" -F c -b -v -f "$DUMP_DIR/${DB}.dump" "$DB"
done
exit 0
```

### MySQL / MariaDB

```bash
#!/bin/bash
HOST="localhost" USER="root" PASS="your_password"
DUMP_DIR="/mnt/backups/mysql"
mkdir -p "$DUMP_DIR"
mysqldump --host="$HOST" --user="$USER" --password="$PASS" \
  --all-databases --single-transaction --quick --lock-tables=false \
  --routines --triggers > "$DUMP_DIR/full_backup.sql"
exit 0
```

### LDAP (OpenLDAP)

```bash
#!/bin/bash
DUMP_DIR="/mnt/backups/ldap"
mkdir -p "$DUMP_DIR"
slapcat -l "$DUMP_DIR/config.ldif" -n 0
slapcat -l "$DUMP_DIR/data.ldif" -n 1
exit 0
```

### Tips

- **Cleanup**: Use a PostScript to delete dump files after successful backup.
- **Error handling**: Exit non-zero to abort the backup if the dump fails.
