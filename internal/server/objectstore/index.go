//go:build linux

package objectstore

import (
	"context"
	"database/sql"
	"embed"
	"encoding/json"
	"fmt"

	"github.com/pbs-plus/pbs-plus/internal/server/objectstore/objectstorequery"
	"github.com/pbs-plus/pbs-plus/internal/sqldb"
)

//go:embed migrations/*.sql
var objectstoreMigrations embed.FS

type indexedObject struct {
	Bucket       string
	Key          string
	Datastore    string
	Namespace    string
	BackupType   string
	BackupID     string
	SnapshotTime int64
	Size         int64
	ETag         string
	ContentType  string
	UserMetadata map[string]string
}

type keyIndex struct {
	*sqldb.Handle
	read  *objectstorequery.Queries
	write *objectstorequery.Queries
}

func openKeyIndex(path string) (*keyIndex, error) {
	db, err := sqldb.Open(path, objectstoreMigrations, "migrations")
	if err != nil {
		return nil, err
	}
	return &keyIndex{
		Handle: db,
		read:   objectstorequery.New(db.Reader()),
		write:  objectstorequery.New(db.Writer()),
	}, nil
}

func (i *keyIndex) get(ctx context.Context, bucket, key string) (indexedObject, bool, error) {
	row, err := i.read.GetObject(ctx, objectstorequery.GetObjectParams{Bucket: bucket, Key: key})
	if err == sql.ErrNoRows {
		return indexedObject{}, false, nil
	}
	if err != nil {
		return indexedObject{}, false, fmt.Errorf("read object index: %w", err)
	}
	object := indexedObject{
		Bucket:       row.Bucket,
		Key:          row.Key,
		Datastore:    row.Datastore,
		Namespace:    row.Namespace,
		BackupType:   row.BackupType,
		BackupID:     row.BackupID,
		SnapshotTime: row.SnapshotTime,
		Size:         row.Size,
		ETag:         row.Etag,
		ContentType:  row.ContentType,
	}
	if err := json.Unmarshal([]byte(row.UserMetadata), &object.UserMetadata); err != nil {
		return indexedObject{}, false, fmt.Errorf("decode object metadata index: %w", err)
	}
	return object, true, nil
}

func (i *keyIndex) put(ctx context.Context, object indexedObject) error {
	metadata, err := json.Marshal(object.UserMetadata)
	if err != nil {
		return fmt.Errorf("encode object metadata index: %w", err)
	}
	if err := i.write.UpsertObject(ctx, objectstorequery.UpsertObjectParams{
		Bucket:       object.Bucket,
		Key:          object.Key,
		Datastore:    object.Datastore,
		Namespace:    object.Namespace,
		BackupType:   object.BackupType,
		BackupID:     object.BackupID,
		SnapshotTime: object.SnapshotTime,
		Size:         object.Size,
		Etag:         object.ETag,
		ContentType:  object.ContentType,
		UserMetadata: string(metadata),
	}); err != nil {
		return fmt.Errorf("write object index: %w", err)
	}
	return nil
}

func (i *keyIndex) listKeys(ctx context.Context, bucket string) (map[string]struct{}, error) {
	rows, err := i.read.ListObjectKeysByBucket(ctx, bucket)
	if err != nil {
		return nil, fmt.Errorf("read object index: %w", err)
	}
	keys := make(map[string]struct{}, len(rows))
	for _, row := range rows {
		keys[row] = struct{}{}
	}
	return keys, nil
}

func (i *keyIndex) delete(ctx context.Context, bucket, key string) error {
	if err := i.write.DeleteObject(ctx, objectstorequery.DeleteObjectParams{Bucket: bucket, Key: key}); err != nil {
		return fmt.Errorf("delete object index: %w", err)
	}
	return nil
}
