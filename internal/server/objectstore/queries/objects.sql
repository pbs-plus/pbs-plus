-- name: UpsertObject :exec
INSERT INTO objects (
    bucket, key, datastore, namespace, backup_type, backup_id,
    snapshot_time, size, etag, content_type, user_metadata
) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
ON CONFLICT(bucket, key) DO UPDATE SET
    datastore = excluded.datastore,
    namespace = excluded.namespace,
    backup_type = excluded.backup_type,
    backup_id = excluded.backup_id,
    snapshot_time = excluded.snapshot_time,
    size = excluded.size,
    etag = excluded.etag,
    content_type = excluded.content_type,
    user_metadata = excluded.user_metadata;

-- name: GetObject :one
SELECT bucket, key, datastore, namespace, backup_type, backup_id,
    snapshot_time, size, etag, content_type, user_metadata
FROM objects
WHERE bucket = ? AND key = ?;

-- name: DeleteObject :exec
DELETE FROM objects WHERE bucket = ? AND key = ?;

-- name: ListObjectKeysByBucket :many
SELECT key FROM objects WHERE bucket = ?;
