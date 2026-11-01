# Logging and error conventions

`convention_test.go` enforces these rules over the trees listed in its `enforced`
slice. Packages outside that list predate the convention and are converted tree by
tree; add a tree to `enforced` once it passes.

## Levels

| Level   | Use for                                                      |
| ------- | ------------------------------------------------------------ |
| `Error` | The operation failed and something was lost or skipped.      |
| `Warn`  | The operation continued but an operator should know.         |
| `Info`  | A lifecycle milestone: started, mounted, finished, released. |
| `Debug` | Per-request or per-file detail. Off in production.           |

## Call shape

```go
log.Error(err, "failed to mount the agent source", "agent", name, "mountpoint", path)
log.Info("agent source mounted", "agent", name, "mountpoint", path)
```

The error goes in the `err` argument, never interpolated into the message. Context
goes in trailing key-value pairs, never formatted into the message either.

## Messages

- Lowercase, except proper nouns (`PBS`, `QUIC`, `S3`, `Dovecot`).
- No trailing period.
- No `FunctionName:` prefix. The message says what failed, not where.
- Never empty. `log.Error(err, "")` loses the only human-readable part of the line.
- State the effect, not the call: "failed to remove the stale memcached socket",
  not "os.Remove failed".

## Error strings

- Lowercase, no trailing period, no `FunctionName:` prefix.
- Wrap with `%w`, never `%v`, so `errors.Is` and `errors.As` keep working.
- Describe the operation in progress: `fmt.Errorf("getting the datastore: %w", err)`.

## Job task logs

Job-scoped entries also land in the PBS task log an operator reads in the UI.
`formatJobLogLine` renders them, dropping identity fields already implied by the
task and sorting the rest as `key=value`.

Errors keep the `WARNING: ` prefix because `processPBSProxyLogs` counts that exact
marker; removing it would silently zero the warning count.

A finished job is headed by `reportLines` in `internal/server/backup/diagnosis.go`:

```
Job: DP-D030-D
Datastore: phoenix, namespace DP/DP-D030/D
Source: agent DP-D030 (10.1.24.9), path /C/Data, mode snapshot
Started: 2026-10-31T19:03:38-04:00, ended 2026-10-31T19:18:00-04:00, duration 14m22s
Result: cannot reach agent DP-D030 (10.1.24.9)
Detail: target unreachable
```

`Result` comes from `failureSummary`, which maps each sentinel error to one
unambiguous phrase. Add a case there when you add a sentinel error an operator can
hit; the fallback is a bare "backup failed", which tells them nothing.

## Direct slog

Use the shared `internal/log` package. `slog` is allowed only in `internal/log`,
`internal/conf`, and `internal/proxmox/tasklog`, which run before the shared logger
exists or are imported by it.
