# S3 Outposts

An S3 outpost serves an S3-compatible endpoint whose objects are PBS snapshots.
It targets automated backup systems that can only speak S3 - mariadb-operator,
CNPG barman, Velero, pgBackRest, dump sidecars. They PUT a dump, LIST to find
the newest one, GET it back and DELETE for retention.

Client-facing usage (creating an outpost, buckets, credentials, TLS) is
documented in [outposts.md](outposts.md). This document is the technical
reference: how objects map onto the datastore, what the server guarantees, and
what it deliberately refuses.

## Object model

**A bucket is (datastore, namespace, backup type, backup id).** The binding is
configured by an operator, never derived from request paths. `ListBuckets`
returns exactly the configured set; `CreateBucket` and `DeleteBucket` answer
`405`. This puts PBS ACLs, prune jobs, GC and verification on a boundary an
admin picked, and keeps one prune policy per bucket.

**A key is a snapshot in that group.** The object write time (UTC, one-second
granularity) is the snapshot time. The literal S3 key is stored unencoded in
the manifest's free-form `unprotected` section, which PBS excludes from the
manifest signature:

```json
"unprotected": {
  "pbs-plus-s3": {
    "bucket": "mariadb",
    "key": "mariadb/backup.2026-01-01T00:00:00Z.sql.gz",
    "etag": "\"...\"",
    "size": 918273645,
    "content-type": "application/gzip",
    "user-metadata": {}
  }
}
```

Consequences:

- Successive dumps land in one group, so PBS prune keep-last/keep-daily works
  unchanged and chunk dedup has a lineage between dumps.
- PUT of an existing key writes a new snapshot; newest wins for GET and LIST.
  Older snapshots are versions that prune trims.
- DELETE removes every snapshot of that key through the locked path. A
  `.protected` marker on any snapshot makes that snapshot answer `AccessDenied`
  naming the marker.

## Authentication

minio-go always signs SigV4, so the outpost verifies instead of trusting:
signature against the secret bound to the access key, S3 credential scope,
region and date match, ±15 minute clock skew, constant-time comparison.
Streaming bodies use `STREAMING-AWS4-HMAC-SHA256-PAYLOAD` and its `-TRAILER`
variant with chained per-chunk signatures, `x-amz-decoded-content-length`
enforcement, and signed `x-amz-checksum-crc32c`/`x-amz-checksum-crc64nvme`
trailers. `STREAMING-UNSIGNED-PAYLOAD-TRAILER` and `UNSIGNED-PAYLOAD` are
rejected; clients that need them should use plain HTTP or TLS with signed
payloads, which every dump tool does.

`GetBucketLocation` returns the configured region so clients stop probing.
Each access key carries a PBS auth id and per-bucket `read`/`write`/`delete`
grants; the auth id becomes the group `owner` file, so PBS ownership checks
stay meaningful.

## Publication lifecycle

A snapshot appears only when it is complete. Per PUT, in order: register an
active write under `/run/proxmox-backup/active-operations` -> ensure and chown
the group path -> acquire the group and snapshot locks under
`/run/proxmox-backup/locks` -> write/validate the `owner` file -> take a
shared read lock on `<datastore>/.lock` -> stream the body through the
buzhash chunker into `.chunks` as one `s3-object.didx` -> verify the index ->
publish `index.json.blob` -> drop locks. Any failure removes the snapshot
directory and, if the group was newly created, the group. A snapshot without
its manifest is a broken snapshot to PBS, and this path never produces one.

GC safety is inherited: chunks are written with a fresh atime and PBS GC only
sweeps below `now - 24h5m`, so an upload is safe as long as it finishes inside
that window.

## Multipart

Parts spool to `<state>/objectstore/<outpost>-uploads/<uploadID>/part-N` with
an fsynced JSON journal; nothing is held in memory. Clients upload parts
concurrently, so part data lands in a private temp file first and the rename
plus journal update run under a per-upload `flock`. `CompleteMultipartUpload`
holds the same lock, streams the parts in order through one publish - opening
one part file at a time, so ten-thousand-part uploads cannot exhaust file
descriptors - and removes the spool on success. The multipart ETag is
`<md5-of-concatenated-binary-part-md5s>-<n>`; single PUT records the SHA-256.
Abandoned uploads are reaped after 7 days of spool inactivity, at outpost
start and daily.

## Listing and the key index

Listing scans the datastore (newest version wins per key) and honours prefix,
delimiter, `max-keys` up to 1000, `continuation-token`/`start-after`,
`encoding-type=url`, and the v1 `marker`. `DeleteObjects` batches up to 1000
keys with per-key results.

A SQLite key index under `<state>/objectstore/<outpost>.db` caches
`(bucket, key) -> snapshot` lookups for GET/HEAD. It is a cache, never the
truth: GET/HEAD falls back to a manifest scan before answering `404`, and a
reconciler rebuilds it from the datastore at outpost start and daily. A stale
index degrades latency, never correctness.

## Unsupported, by name

Correct S3 errors instead of lying:

| API                                                                 | Answer                                                                           |
| ------------------------------------------------------------------- | -------------------------------------------------------------------------------- |
| Versioning, lifecycle, replication, tagging, ACL, CORS, object lock | `NotImplemented`                                                                 |
| `CopyObject`                                                        | `NotImplemented` (an index insert plus manifest write, cheap to add)             |
| Any `x-amz-server-side-encryption*` header                          | `NotImplemented` - ignoring an encryption header silently is worse than refusing |
| Presigned URLs, anonymous access                                    | `AccessDenied`                                                                   |

Prometheus metrics and per-mutation UPID tasks are not implemented; PUT and
DELETE emit structured log lines (`s3 put object`, `s3 delete object`) with
bucket, key, size, ETag and datastore instead.

## Testing

Unit tests drive the server with genuine minio-go clients: signed single and
streaming PUTs, GET/HEAD/Range, overwrite, `.protected`, grants, listing with
pagination and URL-encoding, DeleteObjects, multipart `FPutObject` round trips
with ranged reads across part boundaries, restart, abort, bad ETag and reaping

- all against a temporary datastore with real locks. The
  `run-s3-outpost-e2e` action extends this in CI: it creates an S3 outpost
  through the management API and pushes, lists, overwrites and deletes objects
  with the real `mc` client, asserting the snapshots land in the datastore and
  disappear on delete.
