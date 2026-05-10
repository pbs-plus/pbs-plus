# Phase 1: Server Consolidation — Complete

## Summary

Consolidated 5 server-related packages (`backend/`, `web/`, `store/`, `tasks/`, `application/`) into a single `internal/server/` domain. All files moved, snake_case filenames renamed to MixedCaps, 6 sub-packages merged with symbol renames, and all import paths updated across the project. Build and vet pass with zero errors.

## File Moves & Renames

- 100+ Go files moved from old locations to `internal/server/` subdirectories
- All non-Go files (SQL migrations, SQL queries, JS views, PS1 scripts) moved
- Snake_case filenames converted to MixedCaps (e.g., `client_logging.go` → `clientLogging.go`)
- Old directories (`internal/application/`, `internal/backend/`, `internal/tasks/`, `internal/web/`, `internal/store/`) removed

## Package Merges

| Old Package            | New Package  | Symbol Renames                            |
| ---------------------- | ------------ | ----------------------------------------- |
| `rpc/job`              | `rpc`        | (no collisions, merged into same package) |
| `vfs/arpcfs/arpcfuse`  | `vfs/arpcfs` | `Mount` → `MountFuse`                     |
| `vfs/arpcfs/arpcmount` | `vfs/arpcfs` | `Mount` → `MountARPC`                     |
| `vfs/s3fs/fuse`        | `vfs/s3fs`   | `Mount` → `MountFuse`                     |
| `vfs/s3fs/s3mount`     | `vfs/s3fs`   | `Mount` → `MountS3`                       |
| `vfs/s3fs/s3url`       | `vfs/s3fs`   | `Parse` → `ParseS3Url`                    |

## Import Cycle Fixes

Two import cycles were discovered and resolved during the merge:

1. **`database` ↔ `s3fs`**: `database/targets.go` imported `s3url` for `ParseS3Url`/`S3Url`; when merged into `s3fs` (which imports `database.Backup`), this created a cycle. **Fix**: Moved `S3Url` type and `ParseS3Url` function from `s3fs/s3url.go` into the `database` package as `database.S3Url` / `database.ParseS3Url`. Removed self-imports from merged `s3fs` files (`fuseServer.go`, `s3mount.go`) and `arpcfs` files (`arpcfuse.go`, `arpcmount.go`).

2. **`jobs/backup` ↔ `mount` ↔ `rpc`**: Merging `rpc/job` into `rpc` created a cycle where `backup` → `mount` → `rpc` → `backup`. **Fix**: Introduced factory function pattern in `rpc/jobService.go` — `BackupJobFactory` and `RestoreJobFactory` are function variables set at initialization time in `server/bootstrap.go`, breaking the compile-time import cycle.

## Additional Fixes

- `cmd/pbs_plus/main.go`: Resolved `net/rpc` vs `internal/server/rpc` package name collision by aliasing the project import as `jobrpc`
- `web/api/filetree.go`, `web/api/mountHandlers.go`, `agent/sync/wire.go`: Restored `backend` import alias that was lost during sed replacement (changed from `backend "internal/backend"` to `backend "internal/server"`)

## Verification

- `go build ./...` — passes with zero errors
- `go vet ./internal/server/...` — clean
- `go mod tidy` — clean
