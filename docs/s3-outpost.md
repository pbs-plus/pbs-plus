# S3 Outpost (design)

Status: design, not implemented. Branch `feat/s3-outpost`.

An S3 outpost is a third outpost driver (`s3`, next to `nfs` and `samba`) that
serves an S3-compatible endpoint whose objects are PBS snapshots. It exists for
automated backup systems that can only write to S3: mariadb-operator, CNPG
barman, Velero, pgBackRest, mysqldump sidecars. They PUT a dump, LIST to find
the newest one, GET it back, DELETE for retention. Nothing else.

`internal/server/outpost/outpost.go:5` already names this as the reason drivers
are registered per type.

## Why not pbs-s3gateway

`~/pbs-s3gateway` proves the protocol translation works and its
`keymapper`/`upload` split is the right decomposition. Four things in it do not
survive contact with a real datastore, and they drive this design:

| Defect                                                                                                 | Consequence                                                                                                                                                             |
| ------------------------------------------------------------------------------------------------------ | ----------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| One backup group per object key (`keymapper/mapper.go:44`), backup-id derived by encoding the filename | Group explosion, PBS prune (per group, keep-last N) is useless, no dedup lineage between successive dumps, lossy against `SAFE_ID_REGEX` `[A-Za-z0-9_][A-Za-z0-9._\-]*` |
| Namespace derived from arbitrary key prefixes, auto-created on 404 (`pbs/upload.go`)                   | Namespace explosion, silently breaches `MAX_NAMESPACE_DEPTH` (8), no ACL boundary an operator chose                                                                     |
| Prefix listing decodes IDs without the namespace (`FilterByPrefix`), so it cannot page                 | `ListObjectsV2` is O(datastore) per call and `ContinuationToken` cannot be honoured                                                                                     |
| Talks to PBS over HTTP/2 from outside, holding no datastore lock                                       | Double hop for data that is already local; races against prune, GC, sync and verify                                                                                     |

The fix for all four is the same: bind buckets to real PBS boundaries chosen by
an operator, keep the literal S3 key in the manifest instead of encoding it into
an id, and publish through the same locked local path that mount commits already
use (`internal/server/snapshotmount/compose_publish.go:40`).

## Object model

**Bucket = (datastore, namespace, backup type, group id).** Configured, never
derived from request paths. `ListBuckets` returns exactly the configured set;
`CreateBucket`/`DeleteBucket` return `405`. This is the whole answer to
namespace and group explosion, and it puts PBS ACLs, prune jobs, GC and
verification on a boundary an admin picked.

**Key = a snapshot in that group.** The object write time (UTC, second
granularity) is the snapshot time. The literal key is stored, never encoded:

```json
"unprotected": {
  "pbs-plus-s3": {
    "bucket": "mariadb", "key": "mariadb/backup.2026-01-01T00:00:00Z.sql.gz",
    "etag": "\"...\"", "size": 918273645,
    "content-type": "application/gzip", "user-metadata": {}
  }
}
```

`unprotected` is free-form and excluded from the manifest signature, so this is
inert to PBS. The snapshot's `notes` gets the key too, so the PBS UI is readable
without pbs-plus.

Consequences that fall out for free:

- Successive dumps land in one group, so PBS prune keep-last/keep-daily works
  unchanged and chunk dedup has a lineage (`BackupConfig.PreviousBackup`).
- PUT of an existing key writes a new snapshot; newest wins for GET, older ones
  are versions that prune trims. S3 overwrite semantics hold, and version
  history is a side effect rather than new machinery.
- DELETE destroys the snapshots for that key through the locked prune path.
  `.protected` snapshots return `AccessDenied` naming the marker.

**Group id.** Flat-key clients (mariadb-operator writes `<prefix>/<basename>`,
`pkg/minio/minio.go` `PrefixedFileName`) get one group from bucket config. For
clients that use real key hierarchies, the bucket may opt into
`group_from = first-path-segment`, slugified to `SAFE_ID_REGEX` with a
`-<8 hex of sha256(segment)>` suffix whenever slugification is not injective.
Never more than one namespace level is invented.

## Key index

Listing by reading every manifest is O(snapshots) per request and cannot page.
A SQLite table (sqlc, alongside `coredb`) maps
`(bucket, key) → (datastore, ns, type, group, snapshot_time, size, etag, content_type, deleted)`,
ordered by key so `ContinuationToken` is a real cursor.

The index is a cache, never the truth. The datastore is the truth.

- Written in the same transaction boundary as the publish, after the manifest
  lands.
- Reconciled at outpost start and on the scheduler (reuse `internal/calendar`),
  by walking group manifests: entries whose snapshot vanished (prune, GC,
  manual delete, sync) are dropped, snapshots found with a `pbs-plus-s3`
  manifest section and no row are added.
- A `GET`/`HEAD` miss falls back to a manifest read before answering `404`, so a
  stale index degrades latency, never correctness.

## Component layout

`internal/server/outpost/` holds `.go` files, so per CONVENTIONS it takes no
subpackages. The driver stays there; the protocol and object model live in a
sibling package.

```
internal/server/outpost/s3.go          s3Driver, s3Instance; registers TypeS3
internal/server/objectstore/
  doc.go            package doc: mapping rules, what is deliberately unsupported
  bucket.go         bucket config, binding to datastore/ns/group, ListBuckets
  sigv4.go          SigV4 verification, aws-chunked + trailer decoding
  request.go        router, handlers, Expect: 100-continue, error mapping
  wire.go           XML request/response types
  object.go         key <-> snapshot mapping, manifest section, ETag rules
  index.go          key index queries and reconcile
  writer.go         PUT -> chunked snapshot publish (locks, owner, chown)
  reader.go         GET/HEAD, Range via ParseDynamicIndex + chunk source
  multipart.go      spooled multipart upload, journal, reaper
  credential.go     access key -> secret + PBS auth id + bucket grants
  errors.go         S3 error codes
```

Storage seam: writes go through `backupproxy.RemoteStore`. Local (default) is
`backupproxy.NewDatastoreStore(datastoreDir, snapshotDir, ...)`, which writes
chunks into the shared `.chunks` and publishes index and manifest into the
snapshot dir. An outpost running off the PBS host (`feat/external-outposts`)
substitutes `backupproxy.NewPBSStore` and nothing else changes; both satisfy the
same interface, so this costs one constructor switch, not a second code path.

An object body is uploaded with one `BackupSession.UploadArchive` call, which
buzhash-chunks the stream into a single `.didx`. One index per object is what
makes Range reads and cross-version dedup work.

## Reliability

This is the part that separates this from the prototype.

**Authentication is verified, not parsed.** minio-go always signs SigV4. The
outpost verifies the signature against the secret bound to the access key, with
a ±15 min skew window, and supports `STREAMING-AWS4-HMAC-SHA256-PAYLOAD`,
`STREAMING-UNSIGNED-PAYLOAD-TRAILER` and the trailer variants minio-go ≥7.0.70
sends. `GetBucketLocation` returns the configured region so the client stops
probing. Each access key carries a PBS auth id and per-bucket grants
(`read`, `write`, `delete`); the auth id becomes the group `owner` file, so PBS
ownership checks stay meaningful.

**A snapshot appears only when it is complete.** Order per PUT, mirroring
`compose_publish.go`: register in `/run/proxmox-backup/active-operations` →
ensure group path → group lock in `/run/proxmox-backup/locks` → write `owner`
→ stream chunks into `.chunks` → publish `.didx` → publish `index.json` →
chown `backup:backup` → drop locks. Any failure unlinks the snapshot dir and
the group if we created it. A snapshot without `index.json` is a broken
snapshot to PBS, and this design never produces one.

GC safety is inherited rather than invented: chunks are written with a fresh
atime and PBS GC only sweeps below `now - 24h5m`, so an upload is safe as long
as it finishes inside that window. Uploads are bounded well below it.

**Multipart survives a restart.** Parts spool to
`<state>/objectstore/uploads/<uploadID>/<n>` with an fsynced JSON journal; nothing
is held in memory. `CompleteMultipartUpload` streams the parts in order through
one `UploadArchive`. Out-of-order arrival and part re-upload are legal in S3, so
spooling is the correct shape; the sequential-arrival fast path that chunks
inline is deliberately future work. Abandoned uploads are reaped by age
(default 7d) at start and on schedule.

**Concurrency.** A per-`(bucket, key)` mutex plus the PBS group lock. Snapshot
time collisions increment by one second with a bounded retry, the same rule the
prototype learned (`pbs/upload.go` `retryStartSession`) but bounded by the lock
rather than by parsing error strings. Concurrent chunking sessions per outpost
are capped; chunking is CPU and IO bound and an unbounded S3 endpoint is a
denial-of-service surface.

**ETag is recorded, never recomputed.** SHA-256 of the object for single PUT,
`<md5-of-part-md5s>-<n>` for multipart, matching what clients expect to compare.

**Every mutation is a PBS task.** PUT/DELETE open a UPID task log through
`internal/proxmox/tasklog`, so an operator sees S3 ingest in the PBS task list
next to real backups. Prometheus counters per bucket for requests, bytes,
failures and upload duration.

## Deliberately unsupported

Named, returning correct S3 errors rather than lying:

- Versioning API, lifecycle, replication, tagging, ACL, CORS, object lock →
  `NotImplemented`. Version history exists as snapshots but is not exposed via
  the versioning API.
- `CopyObject` → `NotImplemented` initially; a server-side copy is an index
  insert plus a manifest write and is cheap to add later.
- SSE-C: mariadb-operator supports it (`pkg/minio/minio.go` `getSSEC`). Phase 4
  either honours the header by encrypting the stream with the supplied key, or
  rejects it explicitly. Silently ignoring an encryption header is the one
  failure mode worse than not supporting it.
- Presigned URLs → phase 4.
- Anonymous access → never.

## Phases

Each phase ends on a runnable check, no phase leaves a half-written snapshot
path behind.

1. **Skeleton and contract.** `outpost.Outpost` gains the s3 fields, `TypeS3`
   registers, config persists and the endpoint starts/stops with the others.
   Bucket config, credential store, SigV4 verification, `ListBuckets`,
   `GetBucketLocation`, `HeadBucket`. Unit tests sign real minio-go requests
   against the verifier.
2. **Single-object round trip.** PUT (single part, `aws-chunked` decoded) →
   locked publish → `index.json` with the `pbs-plus-s3` section. GET, HEAD,
   Range. DELETE with `.protected` handling. Key index created and written.
   Check: `mc cp`/`mc cat`/`mc rm` and a Go test using minio-go.
3. **Listing and multipart.** `ListObjectsV2` (prefix, delimiter, max-keys,
   continuation), `ListObjects` v1, `DeleteObjects`. Spooled multipart with
   journal and reaper. Index reconcile on start and schedule.
   Check: a 5 GiB `FPutObject` interrupted and retried.
4. **Integration.** ExtJS panel fields in `outposts_panel.go`, API surface at
   `/api2/extjs/config/d2d-outposts`, task logs, metrics, TLS via existing mtls
   config, SSE-C and presigned decision, docs in `docs/outposts.md`.
   Check: `.github/actions/run-s3-outpost-e2e` runs a real mariadb-operator
   `Backup` and `Restore` against the outpost, plus a minio-go conformance
   subset.

## Open decisions

1. Bucket-to-group binding for hierarchical keys: one group per bucket with keys
   as snapshots (simple, one prune policy per bucket) versus first-path-segment
   groups (finer prune, invents ids). Default is the former; the latter is
   opt-in per bucket.
2. Retention ownership: let clients express retention through DELETE only, or
   also expose a bucket prune policy that PBS enforces. If both act, an
   operator's prune job and a client's `maxRetention` will disagree.
3. External outposts: whether the S3 outpost ships in phase 1 as PBS-host-local
   only (direct datastore writes) or carries the `NewPBSStore` path from the
   start.
