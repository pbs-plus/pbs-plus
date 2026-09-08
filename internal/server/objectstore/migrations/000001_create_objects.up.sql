CREATE TABLE IF NOT EXISTS objects (
    bucket TEXT NOT NULL,
    key TEXT NOT NULL,
    datastore TEXT NOT NULL,
    namespace TEXT NOT NULL,
    backup_type TEXT NOT NULL,
    backup_id TEXT NOT NULL,
    snapshot_time INTEGER NOT NULL,
    size INTEGER NOT NULL,
    etag TEXT NOT NULL,
    content_type TEXT NOT NULL,
    user_metadata TEXT NOT NULL,
    PRIMARY KEY (bucket, key)
);
