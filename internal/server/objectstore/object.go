//go:build linux

package objectstore

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"

	"github.com/pbs-plus/pbs-plus/internal/log"
	"time"

	"github.com/pbs-plus/pxar/backupproxy"
	"github.com/pbs-plus/pxar/buzhash"
	"github.com/pbs-plus/pxar/datastore"

	"github.com/pbs-plus/pbs-plus/internal/proxmox"
	"github.com/pbs-plus/pbs-plus/internal/proxmox/cli"
)

const objectArchiveName = "s3-object.didx"

var (
	errInvalidObjectRange = errors.New("invalid object range")
	errObjectNotFound     = errors.New("object not found")
	errObjectProtected    = errors.New("object snapshot is protected")
	errInvalidPayload     = errors.New("invalid request payload")
)

type manifestObject struct {
	Bucket       string            `json:"bucket"`
	Key          string            `json:"key"`
	ETag         string            `json:"etag"`
	Size         int64             `json:"size"`
	ContentType  string            `json:"content-type"`
	UserMetadata map[string]string `json:"user-metadata"`
}

type manifestUnprotected struct {
	Object manifestObject `json:"pbs-plus-s3"`
}

func resolveDatastoreRoot(name string) (string, error) {
	info, err := cli.GetDatastoreInfo(name)
	if err != nil {
		return "", fmt.Errorf("resolve datastore %q: %w", name, err)
	}
	if info.Path == "" {
		return "", fmt.Errorf("datastore %q has no path", name)
	}
	if info.MaintenanceMode != "" {
		return "", fmt.Errorf("datastore %q is in maintenance mode", name)
	}
	return info.Path, nil
}

func (h *Handler) putObject(r *http.Request, bucket Bucket, credential Credential, key string) (etag string, err error) {
	payload, decodedLength, err := newVerifiedPayload(r, credential)
	if err != nil {
		return "", fmt.Errorf("%w: %v", errInvalidPayload, err)
	}
	defer func() { err = errors.Join(err, payload.Close()) }()

	payloadHash := r.Header.Get("X-Amz-Content-Sha256")
	stream := io.Reader(payload)
	etagFunc := func() string { return `"` + strings.ToLower(payloadHash) + `"` }
	if payloadHash == streamingPayloadHash || payloadHash == streamingTrailerPayloadHash {
		fullPayloadHash := sha256.New()
		stream = io.TeeReader(payload, fullPayloadHash)
		etagFunc = func() string { return `"` + hex.EncodeToString(fullPayloadHash.Sum(nil)) + `"` }
	}
	if err := h.publishObject(r.Context(), bucket, credential, key, objectUpload{
		Stream:      stream,
		Size:        decodedLength,
		ContentType: objectContentType(r.Header),
		Metadata:    objectUserMetadata(r.Header),
		ETag:        etagFunc,
	}); err != nil {
		return "", err
	}
	return etagFunc(), nil
}

// objectUpload is one complete object body handed to publishObject as a stream.
type objectUpload struct {
	Stream      io.Reader
	Size        int64
	ContentType string
	Metadata    map[string]string
	ETag        func() string
}

// publishObject streams one object body through a locked PBS snapshot publication.
func (h *Handler) publishObject(ctx context.Context, bucket Bucket, credential Credential, key string, upload objectUpload) (err error) {
	storeRoot, err := h.datastoreRoot(bucket.Datastore)
	if err != nil {
		return err
	}
	backupType, err := datastore.ParseBackupType(bucket.BackupType)
	if err != nil {
		return fmt.Errorf("parse backup type: %w", err)
	}
	publication, err := beginPublication(ctx, bucket.Datastore, storeRoot, bucket, credential.AuthID, h.now().UTC())
	if err != nil {
		return err
	}
	defer func() { err = errors.Join(err, publication.Close()) }()

	chunkConfig, err := buzhash.NewConfig(4 << 20)
	if err != nil {
		return fmt.Errorf("configure object chunking: %w", err)
	}
	uid, gid := proxmox.BackupUID, proxmox.BackupGID
	if os.Geteuid() != 0 {
		uid, gid = os.Getuid(), os.Getgid()
	}
	store, err := backupproxy.NewDatastoreStore(storeRoot, publication.snapshotDir, chunkConfig, backupproxy.DatastoreStoreOptions{
		Compress: true,
		UID:      uid,
		GID:      gid,
	})
	if err != nil {
		return fmt.Errorf("open datastore publisher: %w", err)
	}
	session, err := store.StartSession(ctx, backupproxy.BackupConfig{
		BackupType: backupType,
		BackupID:   bucket.BackupID,
		BackupTime: publication.backupTime,
		Namespace:  bucket.Namespace,
		CryptMode:  datastore.CryptModeNone,
	})
	if err != nil {
		return fmt.Errorf("start datastore session: %w", err)
	}
	defer func() { err = errors.Join(err, session.Close()) }()

	result, err := session.UploadArchive(ctx, objectArchiveName, upload.Stream)
	if err != nil {
		return fmt.Errorf("upload object archive: %w", err)
	}
	if result.Size != uint64(upload.Size) {
		return fmt.Errorf("uploaded object size %d does not match decoded length %d", result.Size, upload.Size)
	}
	if err := verifyObjectIndex(filepath.Join(publication.snapshotDir, objectArchiveName), result); err != nil {
		return err
	}

	manifest, err := session.Finish(ctx)
	if err != nil {
		return fmt.Errorf("finish datastore session: %w", err)
	}
	object := indexedObject{
		Bucket:       bucket.Name,
		Key:          key,
		Datastore:    bucket.Datastore,
		Namespace:    bucket.Namespace,
		BackupType:   bucket.BackupType,
		BackupID:     bucket.BackupID,
		SnapshotTime: publication.backupTime,
		Size:         upload.Size,
		ETag:         upload.ETag(),
		ContentType:  upload.ContentType,
		UserMetadata: upload.Metadata,
	}
	if err := writeObjectManifest(publication.snapshotDir, manifest, object); err != nil {
		return err
	}
	if h.index != nil {
		if err := h.index.put(ctx, object); err != nil {
			return err
		}
	}
	publication.Commit()
	log.Info("s3 put object",
		"bucket", bucket.Name, "key", key, "size", upload.Size, "etag", object.ETag,
		"datastore", bucket.Datastore, "namespace", bucket.Namespace,
		"backup_type", bucket.BackupType, "backup_id", bucket.BackupID)
	return nil
}

func verifyObjectIndex(path string, result *backupproxy.UploadResult) error {
	index, err := datastore.OpenDynamicIndex(path)
	if err != nil {
		return fmt.Errorf("open published object index: %w", err)
	}
	defer func() { _ = index.Close() }()
	digest, size := index.ComputeCsum()
	if digest != index.IndexCsum() || digest != result.Digest || size != result.Size {
		return errors.New("published object index verification failed")
	}
	return nil
}

func writeObjectManifest(snapshotDir string, manifest *datastore.Manifest, object indexedObject) error {
	unprotected, err := json.Marshal(manifestUnprotected{Object: manifestObject{
		Bucket:       object.Bucket,
		Key:          object.Key,
		ETag:         object.ETag,
		Size:         object.Size,
		ContentType:  object.ContentType,
		UserMetadata: object.UserMetadata,
	}})
	if err != nil {
		return fmt.Errorf("encode object manifest metadata: %w", err)
	}
	manifest.Unprotected = unprotected
	data, err := manifest.Marshal()
	if err != nil {
		return fmt.Errorf("encode object manifest: %w", err)
	}
	blob, err := datastore.EncodeBlob(nil, data)
	if err != nil {
		return fmt.Errorf("encode object manifest blob: %w", err)
	}
	if err := replaceObjectstoreOwnedFile(filepath.Join(snapshotDir, "index.json.blob"), blob, 0o644); err != nil {
		return fmt.Errorf("publish object manifest: %w", err)
	}
	return nil
}

func objectContentType(header http.Header) string {
	if value := header.Get("Content-Type"); value != "" {
		return value
	}
	return "application/octet-stream"
}

func objectUserMetadata(header http.Header) map[string]string {
	metadata := make(map[string]string)
	for name, values := range header {
		name = strings.ToLower(name)
		if suffix, ok := strings.CutPrefix(name, "x-amz-meta-"); ok && suffix != "" {
			metadata[suffix] = strings.Join(values, ",")
		}
	}
	return metadata
}

func (h *Handler) locateObject(ctx context.Context, bucket Bucket, key string) (indexedObject, string, error) {
	storeRoot, err := h.datastoreRoot(bucket.Datastore)
	if err != nil {
		return indexedObject{}, "", err
	}
	if h.index != nil {
		if object, ok, indexErr := h.index.get(ctx, bucket.Name, key); indexErr == nil && ok && objectMatchesBucket(object, bucket) {
			snapshotDir := objectSnapshotDir(storeRoot, object)
			if current, loadErr := loadObjectSnapshot(snapshotDir, bucket, key); loadErr == nil {
				return current, snapshotDir, nil
			}
		}
	}
	object, snapshotDir, err := scanLatestObject(storeRoot, bucket, key)
	if err != nil {
		if errors.Is(err, errObjectNotFound) && h.index != nil {
			_ = h.index.delete(ctx, bucket.Name, key)
		}
		return indexedObject{}, "", err
	}
	if h.index != nil {
		_ = h.index.put(ctx, object)
	}
	return object, snapshotDir, nil
}

func objectMatchesBucket(object indexedObject, bucket Bucket) bool {
	return object.Bucket == bucket.Name && object.Datastore == bucket.Datastore && object.Namespace == bucket.Namespace &&
		object.BackupType == bucket.BackupType && object.BackupID == bucket.BackupID
}

func objectSnapshotDir(storeRoot string, object indexedObject) string {
	groupDir := filepath.Join(proxmox.NamespacePath(storeRoot, object.Namespace), object.BackupType, object.BackupID)
	return filepath.Join(groupDir, time.Unix(object.SnapshotTime, 0).UTC().Format(time.RFC3339))
}

func scanLatestObject(storeRoot string, bucket Bucket, key string) (indexedObject, string, error) {
	groupDir := filepath.Join(proxmox.NamespacePath(storeRoot, bucket.Namespace), bucket.BackupType, bucket.BackupID)
	entries, err := os.ReadDir(groupDir)
	if os.IsNotExist(err) {
		return indexedObject{}, "", errObjectNotFound
	}
	if err != nil {
		return indexedObject{}, "", fmt.Errorf("read object snapshots: %w", err)
	}
	for _, entry := range slices.Backward(entries) {

		if !entry.IsDir() {
			continue
		}
		if _, err := time.Parse(time.RFC3339, entry.Name()); err != nil {
			continue
		}
		snapshotDir := filepath.Join(groupDir, entry.Name())
		object, err := loadObjectSnapshot(snapshotDir, bucket, key)
		if err == nil {
			return object, snapshotDir, nil
		}
	}
	return indexedObject{}, "", errObjectNotFound
}

func readSnapshotObject(snapshotDir string) (manifestObject, int64, error) {
	raw, err := os.ReadFile(filepath.Join(snapshotDir, "index.json.blob"))
	if err != nil {
		return manifestObject{}, 0, err
	}
	data, err := datastore.DecodeBlob(nil, raw)
	if err != nil {
		return manifestObject{}, 0, err
	}
	manifest, err := datastore.UnmarshalManifest(data)
	if err != nil {
		return manifestObject{}, 0, err
	}
	var extra manifestUnprotected
	if err := json.Unmarshal(manifest.Unprotected, &extra); err != nil {
		return manifestObject{}, 0, err
	}
	metadata := extra.Object
	if metadata.Size < 0 || metadata.ETag == "" {
		return manifestObject{}, 0, errors.New("object metadata is invalid")
	}
	foundArchive := false
	for _, file := range manifest.Files {
		if file.Filename == objectArchiveName && int64(file.Size) == metadata.Size {
			foundArchive = true
			break
		}
	}
	if !foundArchive {
		return manifestObject{}, 0, errors.New("object archive is missing from manifest")
	}
	return metadata, manifest.BackupTime, nil
}

func loadObjectSnapshot(snapshotDir string, bucket Bucket, key string) (indexedObject, error) {
	metadata, backupTime, err := readSnapshotObject(snapshotDir)
	if err != nil {
		return indexedObject{}, err
	}
	if metadata.Bucket != bucket.Name || metadata.Key != key {
		return indexedObject{}, errObjectNotFound
	}
	return indexedObject{
		Bucket:       bucket.Name,
		Key:          key,
		Datastore:    bucket.Datastore,
		Namespace:    bucket.Namespace,
		BackupType:   bucket.BackupType,
		BackupID:     bucket.BackupID,
		SnapshotTime: backupTime,
		Size:         metadata.Size,
		ETag:         metadata.ETag,
		ContentType:  metadata.ContentType,
		UserMetadata: metadata.UserMetadata,
	}, nil
}

func (h *Handler) serveObject(w http.ResponseWriter, r *http.Request, bucket Bucket, key string, head bool) error {
	object, snapshotDir, err := h.locateObject(r.Context(), bucket, key)
	if err != nil {
		return err
	}
	index, err := datastore.OpenDynamicIndex(filepath.Join(snapshotDir, objectArchiveName))
	if err != nil {
		return fmt.Errorf("open object index: %w", err)
	}
	defer func() { _ = index.Close() }()
	if index.LastEndOffset() != uint64(object.Size) {
		return errors.New("object index size does not match manifest")
	}
	start, length, partial, err := parseObjectRange(r.Header.Get("Range"), uint64(object.Size))
	if err != nil {
		return err
	}
	w.Header().Set("Accept-Ranges", "bytes")
	w.Header().Set("Content-Length", strconv.FormatUint(length, 10))
	w.Header().Set("Content-Type", object.ContentType)
	w.Header().Set("ETag", object.ETag)
	w.Header().Set("Last-Modified", time.Unix(object.SnapshotTime, 0).UTC().Format(http.TimeFormat))
	for name, value := range object.UserMetadata {
		w.Header().Set("X-Amz-Meta-"+name, value)
	}
	if partial {
		w.Header().Set("Content-Range", fmt.Sprintf("bytes %d-%d/%d", start, start+length-1, object.Size))
		w.WriteHeader(http.StatusPartialContent)
	} else {
		w.WriteHeader(http.StatusOK)
	}
	if head || length == 0 {
		return nil
	}
	chunkStore, err := datastore.NewChunkStore(h.mustDatastoreRoot(bucket.Datastore, snapshotDir, bucket))
	if err != nil {
		return fmt.Errorf("open object chunk store: %w", err)
	}
	restorer := datastore.NewRestorer(datastore.NewChunkStoreSource(chunkStore))
	if partial {
		return restorer.RestoreRange(index, start, length, w)
	}
	return restorer.RestoreFile(index, w)
}

func (h *Handler) mustDatastoreRoot(_ string, snapshotDir string, bucket Bucket) string {
	root := snapshotDir
	levels := 3
	if bucket.Namespace != "" {
		levels += 2 * len(strings.Split(bucket.Namespace, "/"))
	}
	for range levels {
		root = filepath.Dir(root)
	}
	return root
}

func parseObjectRange(value string, size uint64) (start, length uint64, partial bool, err error) {
	if value == "" {
		return 0, size, false, nil
	}
	spec, ok := strings.CutPrefix(value, "bytes=")
	if !ok || strings.Contains(spec, ",") || size == 0 {
		return 0, 0, false, errInvalidObjectRange
	}
	first, last, ok := strings.Cut(spec, "-")
	if !ok {
		return 0, 0, false, errInvalidObjectRange
	}
	if first == "" {
		suffix, parseErr := strconv.ParseUint(last, 10, 64)
		if parseErr != nil || suffix == 0 {
			return 0, 0, false, errInvalidObjectRange
		}
		length = min(suffix, size)
		return size - length, length, true, nil
	}
	start, err = strconv.ParseUint(first, 10, 64)
	if err != nil || start >= size {
		return 0, 0, false, errInvalidObjectRange
	}
	end := size - 1
	if last != "" {
		end, err = strconv.ParseUint(last, 10, 64)
		if err != nil || end < start {
			return 0, 0, false, errInvalidObjectRange
		}
		end = min(end, size-1)
	}
	return start, end - start + 1, true, nil
}

func (h *Handler) deleteObject(ctx context.Context, bucket Bucket, credential Credential, key string) (err error) {
	storeRoot, err := h.datastoreRoot(bucket.Datastore)
	if err != nil {
		return err
	}
	groupDir := filepath.Join(proxmox.NamespacePath(storeRoot, bucket.Namespace), bucket.BackupType, bucket.BackupID)
	if _, err := os.Stat(groupDir); os.IsNotExist(err) {
		if h.index != nil {
			return h.index.delete(ctx, bucket.Name, key)
		}
		return nil
	} else if err != nil {
		return fmt.Errorf("stat object group: %w", err)
	}
	releaseActive, err := beginObjectstoreActiveWrite(bucket.Datastore)
	if err != nil {
		return fmt.Errorf("register active datastore write: %w", err)
	}
	defer func() { err = errors.Join(err, releaseActive()) }()
	groupLock, err := acquireObjectstoreBackupLock(bucket.Datastore, bucket.Namespace, filepath.Join(bucket.BackupType, bucket.BackupID), groupDir, false, true)
	if err != nil {
		return fmt.Errorf("lock object group: %w", err)
	}
	defer func() { err = errors.Join(err, groupLock.Close()) }()
	if err := ensureObjectstoreGroupOwner(groupDir, credential.AuthID, false); err != nil {
		return err
	}
	versions, err := scanObjectVersions(storeRoot, bucket, key)
	if err != nil {
		return err
	}
	locks := make([]io.Closer, 0, len(versions))
	defer func() {
		for _, lock := range slices.Backward(locks) {
			err = errors.Join(err, lock.Close())
		}
	}()
	for _, version := range versions {
		lock, err := acquireObjectstoreBackupLock(
			bucket.Datastore,
			bucket.Namespace,
			filepath.Join(bucket.BackupType, bucket.BackupID, filepath.Base(version)),
			version,
			false,
			true,
		)
		if err != nil {
			return fmt.Errorf("lock object snapshot: %w", err)
		}
		locks = append(locks, lock)
		if _, err := os.Lstat(filepath.Join(version, ".protected")); err == nil {
			return errObjectProtected
		} else if !os.IsNotExist(err) {
			return fmt.Errorf("inspect object protection: %w", err)
		}
	}
	for _, version := range versions {
		if err := os.RemoveAll(version); err != nil {
			return fmt.Errorf("remove object snapshot: %w", err)
		}
	}
	log.Info("s3 delete object", "bucket", bucket.Name, "key", key, "versions", len(versions), "datastore", bucket.Datastore)
	if h.index != nil {
		return h.index.delete(ctx, bucket.Name, key)
	}
	return nil
}

func scanObjectVersions(storeRoot string, bucket Bucket, key string) ([]string, error) {
	groupDir := filepath.Join(proxmox.NamespacePath(storeRoot, bucket.Namespace), bucket.BackupType, bucket.BackupID)
	entries, err := os.ReadDir(groupDir)
	if os.IsNotExist(err) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("read object snapshots: %w", err)
	}
	versions := make([]string, 0)
	for _, entry := range entries {
		if !entry.IsDir() {
			continue
		}
		snapshotDir := filepath.Join(groupDir, entry.Name())
		if _, err := loadObjectSnapshot(snapshotDir, bucket, key); err == nil {
			versions = append(versions, snapshotDir)
		}
	}
	return versions, nil
}
