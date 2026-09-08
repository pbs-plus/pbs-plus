//go:build linux

package objectstore

import (
	"context"
	"encoding/base64"
	"encoding/xml"
	"errors"
	"fmt"
	"io"
	"maps"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/pbs-plus/pbs-plus/internal/proxmox"
)

const (
	maxListKeys        = 1000
	maxDeleteObjects   = 1000
	listingEncodingURL = "url"
)

// scanBucketObjects keeps the newest version per key; one manifest read per snapshot
// is the documented O(snapshots) listing ceiling.
func scanBucketObjects(storeRoot string, bucket Bucket) (map[string]indexedObject, error) {
	groupDir := filepath.Join(proxmox.NamespacePath(storeRoot, bucket.Namespace), bucket.BackupType, bucket.BackupID)
	entries, err := os.ReadDir(groupDir)
	if os.IsNotExist(err) {
		return map[string]indexedObject{}, nil
	}
	if err != nil {
		return nil, fmt.Errorf("read object snapshots: %w", err)
	}
	objects := make(map[string]indexedObject, len(entries))
	for _, entry := range slices.Backward(entries) {

		if !entry.IsDir() {
			continue
		}
		if _, err := time.Parse(time.RFC3339, entry.Name()); err != nil {
			continue
		}
		metadata, backupTime, err := readSnapshotObject(filepath.Join(groupDir, entry.Name()))
		if err != nil || metadata.Bucket != bucket.Name {
			continue
		}
		if _, seen := objects[metadata.Key]; seen {
			continue
		}
		objects[metadata.Key] = indexedObject{
			Bucket:       bucket.Name,
			Key:          metadata.Key,
			Datastore:    bucket.Datastore,
			Namespace:    bucket.Namespace,
			BackupType:   bucket.BackupType,
			BackupID:     bucket.BackupID,
			SnapshotTime: backupTime,
			Size:         metadata.Size,
			ETag:         metadata.ETag,
			ContentType:  metadata.ContentType,
			UserMetadata: metadata.UserMetadata,
		}
	}
	return objects, nil
}

type listBucketResult struct {
	XMLName               xml.Name `xml:"ListBucketResult"`
	XMLNS                 string   `xml:"xmlns,attr"`
	Name                  string
	Prefix                string
	Marker                string `xml:",omitempty"`
	NextMarker            string `xml:",omitempty"`
	ContinuationToken     string `xml:",omitempty"`
	NextContinuationToken string `xml:",omitempty"`
	StartAfter            string `xml:",omitempty"`
	KeyCount              int    `xml:",omitempty"`
	MaxKeys               int
	Delimiter             string `xml:",omitempty"`
	EncodingType          string `xml:",omitempty"`
	IsTruncated           bool
	Contents              []listContentsEntry `xml:"Contents"`
	CommonPrefixes        []listCommonPrefix  `xml:"CommonPrefixes"`
}

type listContentsEntry struct {
	Key          string
	LastModified string
	ETag         string
	Size         int64
	StorageClass string
}

type listCommonPrefix struct {
	Prefix string
}

func (h *Handler) listObjects(w http.ResponseWriter, r *http.Request, bucket Bucket, credential Credential) {
	if !credential.canRead(bucket.Name) {
		writeError(w, r, http.StatusForbidden, "AccessDenied", "Access Denied.")
		return
	}
	query := r.URL.Query()
	prefix := query.Get("prefix")
	delimiter := query.Get("delimiter")
	encoding := query.Get("encoding-type")
	if encoding != "" && encoding != listingEncodingURL {
		writeError(w, r, http.StatusBadRequest, "InvalidArgument", "Invalid Encoding Method specified in Request.")
		return
	}
	maxKeys := maxListKeys
	if raw := query.Get("max-keys"); raw != "" {
		value, err := strconv.Atoi(raw)
		if err != nil || value < 0 {
			writeError(w, r, http.StatusBadRequest, "InvalidArgument", "max-keys must be an integer between 0 and 2147483647.")
			return
		}
		maxKeys = min(value, maxListKeys)
	}
	after := ""
	if token := query.Get("continuation-token"); token != "" {
		raw, err := base64.RawURLEncoding.DecodeString(token)
		if err != nil {
			writeError(w, r, http.StatusBadRequest, "InvalidArgument", "The continuation token provided is incorrect.")
			return
		}
		after = string(raw)
	} else {
		after = query.Get("start-after")
	}
	marker := ""
	if query.Get("list-type") != "2" {
		marker = query.Get("marker")
		after = marker
	}

	storeRoot, err := h.datastoreRoot(bucket.Datastore)
	if err != nil {
		writeObjectError(w, r, err)
		return
	}
	objects, err := scanBucketObjects(storeRoot, bucket)
	if err != nil {
		writeObjectError(w, r, err)
		return
	}
	keys := slices.Sorted(maps.Keys(objects))

	result := listBucketResult{
		XMLNS:        s3XMLNamespace,
		Name:         bucket.Name,
		MaxKeys:      maxKeys,
		IsTruncated:  false,
		Contents:     make([]listContentsEntry, 0, min(maxKeys, len(keys))),
		EncodingType: encoding,
	}
	if encoding != "" {
		result.Prefix = encodeS3Name(prefix)
		result.Delimiter = encodeS3Name(delimiter)
	} else {
		result.Prefix = prefix
		result.Delimiter = delimiter
	}
	if query.Get("list-type") == "2" {
		result.StartAfter = encodeIfURL(encoding, query.Get("start-after"))
		result.ContinuationToken = encodeIfURL(encoding, after)
	} else {
		result.Marker = encodeIfURL(encoding, marker)
	}

	emitted := 0
	lastEmitted := ""
	seenPrefixes := make(map[string]struct{}, maxKeys)
	for _, key := range keys {
		if !strings.HasPrefix(key, prefix) {
			continue
		}
		rollup := key
		if delimiter != "" {
			if index := strings.Index(key[len(prefix):], delimiter); index >= 0 {
				rollup = key[:len(prefix)+index+len(delimiter)]
			}
		}
		if _, seen := seenPrefixes[rollup]; !seen {
			seenPrefixes[rollup] = struct{}{}
		} else {
			continue
		}
		if rollup <= after {
			continue
		}
		if maxKeys > 0 && emitted == maxKeys {
			result.IsTruncated = true
			break
		}
		if rollup == key {
			object := objects[key]
			result.Contents = append(result.Contents, listContentsEntry{
				Key:          encodeIfURL(encoding, key),
				LastModified: time.Unix(object.SnapshotTime, 0).UTC().Format(time.RFC3339),
				ETag:         object.ETag,
				Size:         object.Size,
				StorageClass: "STANDARD",
			})
		} else {
			result.CommonPrefixes = append(result.CommonPrefixes, listCommonPrefix{Prefix: encodeIfURL(encoding, rollup)})
		}
		emitted++
		lastEmitted = rollup
	}
	result.KeyCount = emitted
	if result.IsTruncated {
		if query.Get("list-type") == "2" {
			result.NextContinuationToken = base64.RawURLEncoding.EncodeToString([]byte(lastEmitted))
		} else {
			result.NextMarker = encodeIfURL(encoding, lastEmitted)
		}
	}
	writeXML(w, http.StatusOK, result)
}

func encodeIfURL(encoding, value string) string {
	if encoding != listingEncodingURL {
		return value
	}
	return encodeS3Name(value)
}

func encodeS3Name(value string) string {
	return strings.ReplaceAll(url.QueryEscape(value), "+", "%20")
}

type deleteObjectsRequest struct {
	XMLName xml.Name              `xml:"Delete"`
	Quiet   bool                  `xml:"Quiet"`
	Objects []deleteRequestObject `xml:"Object"`
}

type deleteRequestObject struct {
	Key string
}

type deleteObjectsResult struct {
	XMLName xml.Name            `xml:"DeleteResult"`
	XMLNS   string              `xml:"xmlns,attr"`
	Deleted []deleteResultEntry `xml:"Deleted"`
	Errors  []deleteResultError `xml:"Error"`
}

type deleteResultEntry struct {
	Key string
}

type deleteResultError struct {
	Key     string
	Code    string
	Message string
}

func (h *Handler) deleteObjects(w http.ResponseWriter, r *http.Request, bucket Bucket, credential Credential) {
	if !credential.canDelete(bucket.Name) {
		writeError(w, r, http.StatusForbidden, "AccessDenied", "Access Denied.")
		return
	}
	payload, _, err := newVerifiedPayload(r, credential)
	if err != nil {
		writeObjectError(w, r, fmt.Errorf("%w: %v", errInvalidPayload, err))
		return
	}
	defer func() { _ = payload.Close() }()
	data, err := io.ReadAll(io.LimitReader(payload, 1<<20))
	if err != nil {
		writeObjectError(w, r, err)
		return
	}
	var request deleteObjectsRequest
	if err := xml.Unmarshal(data, &request); err != nil {
		writeError(w, r, http.StatusBadRequest, "MalformedXML", "The XML you provided was not well-formed or did not validate against our published schema.")
		return
	}
	if len(request.Objects) > maxDeleteObjects {
		writeError(w, r, http.StatusBadRequest, "MalformedXML", "The number of keys must be less than or equal to 1000.")
		return
	}
	result := deleteObjectsResult{
		XMLNS:   s3XMLNamespace,
		Deleted: make([]deleteResultEntry, 0, len(request.Objects)),
		Errors:  make([]deleteResultError, 0),
	}
	for _, object := range request.Objects {
		if object.Key == "" {
			continue
		}
		err := h.deleteObject(r.Context(), bucket, credential, object.Key)
		switch {
		case err == nil:
			if !request.Quiet {
				result.Deleted = append(result.Deleted, deleteResultEntry{Key: object.Key})
			}
		case errors.Is(err, errObjectNotFound):
			result.Deleted = append(result.Deleted, deleteResultEntry{Key: object.Key})
		default:
			result.Errors = append(result.Errors, deleteResultError{
				Key:     object.Key,
				Code:    "InternalError",
				Message: err.Error(),
			})
		}
	}
	writeXML(w, http.StatusOK, result)
}

// ReconcileIndex rebuilds key index rows for every configured bucket from the
// datastore so entries pruned outside the outpost disappear.
func (h *Handler) ReconcileIndex(ctx context.Context) error {
	if h.index == nil {
		return nil
	}
	for _, bucket := range h.config.Buckets {
		storeRoot, err := h.datastoreRoot(bucket.Datastore)
		if err != nil {
			return err
		}
		objects, err := scanBucketObjects(storeRoot, bucket)
		if err != nil {
			return err
		}
		rows, err := h.index.listKeys(ctx, bucket.Name)
		if err != nil {
			return err
		}
		for key, object := range objects {
			if err := h.index.put(ctx, object); err != nil {
				return err
			}
			delete(rows, key)
		}
		for key := range rows {
			if err := h.index.delete(ctx, bucket.Name, key); err != nil {
				return err
			}
		}
	}
	return nil
}
