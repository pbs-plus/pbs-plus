//go:build linux

package objectstore

import (
	"crypto/md5"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"encoding/xml"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"sync"
	"time"

	"golang.org/x/sys/unix"
)

const (
	multipartJournalName   = "journal.json"
	multipartReapAge       = 7 * 24 * time.Hour
	maxMultipartPartNumber = 10000
	multipartSpoolMode     = 0o700
)

var errNoSuchUpload = errors.New("multipart upload not found")

type multipartPart struct {
	Number int    `json:"number"`
	Size   int64  `json:"size"`
	ETag   string `json:"etag"`
}

type multipartJournal struct {
	Bucket       string            `json:"bucket"`
	Key          string            `json:"key"`
	ContentType  string            `json:"content_type"`
	UserMetadata map[string]string `json:"user_metadata"`
	Initiated    int64             `json:"initiated"`
	Parts        []multipartPart   `json:"parts"`
}

var multipartCopyBuffers = sync.Pool{
	New: func() any {
		buffer := make([]byte, 256<<10)
		return &buffer
	},
}

// OpenMultipartSpool opens (creating if needed) the multipart upload spool at path.
func (h *Handler) OpenMultipartSpool(path string) error {
	if err := os.MkdirAll(path, multipartSpoolMode); err != nil {
		return err
	}
	h.multipartDir = path
	return nil
}

func (h *Handler) uploadDir(uploadID string) string {
	return filepath.Join(h.multipartDir, uploadID)
}

func loadMultipartJournal(dir string) (multipartJournal, error) {
	data, err := os.ReadFile(filepath.Join(dir, multipartJournalName))
	if err != nil {
		return multipartJournal{}, err
	}
	var journal multipartJournal
	if err := json.Unmarshal(data, &journal); err != nil {
		return multipartJournal{}, err
	}
	return journal, nil
}

// lockMultipartJournal takes the blocking per-upload lock that serializes part
// renames and journal updates against concurrent UploadPart requests.
func lockMultipartJournal(dir string) (*os.File, error) {
	file, err := os.OpenFile(filepath.Join(dir, ".lock"), os.O_CREATE|os.O_RDWR, 0o600)
	if err != nil {
		return nil, err
	}
	if err := unix.Flock(int(file.Fd()), unix.LOCK_EX); err != nil {
		_ = file.Close()
		return nil, err
	}
	return file, nil
}

// writeMultipartJournal replaces the upload journal atomically and durably so
// a crash mid-write can never destroy the part list.
func writeMultipartJournal(dir string, journal multipartJournal) error {
	data, err := json.Marshal(journal)
	if err != nil {
		return err
	}
	file, err := os.CreateTemp(dir, ".journal-*")
	if err != nil {
		return err
	}
	name := file.Name()
	defer func() { _ = os.Remove(name) }()
	if err := file.Chmod(0o600); err != nil {
		_ = file.Close()
		return err
	}
	if _, err := file.Write(data); err != nil {
		_ = file.Close()
		return err
	}
	if err := file.Sync(); err != nil {
		_ = file.Close()
		return err
	}
	if err := file.Close(); err != nil {
		return err
	}
	if err := os.Rename(name, filepath.Join(dir, multipartJournalName)); err != nil {
		return err
	}
	directory, err := os.Open(dir)
	if err != nil {
		return err
	}
	defer func() { _ = directory.Close() }()
	return directory.Sync()
}

// partSequence streams spooled part files in order holding one open file at a
// time, so a 10000-part upload never exhausts file descriptors.
type partSequence struct {
	dir     string
	numbers []int
	next    int
	current *os.File
}

func (s *partSequence) Read(p []byte) (int, error) {
	for {
		if s.current == nil {
			if s.next == len(s.numbers) {
				return 0, io.EOF
			}
			file, err := os.Open(filepath.Join(s.dir, "part-"+strconv.Itoa(s.numbers[s.next])))
			if err != nil {
				return 0, err
			}
			s.next++
			s.current = file
		}
		n, err := s.current.Read(p)
		if n > 0 {
			return n, nil
		}
		if err == io.EOF {
			_ = s.current.Close()
			s.current = nil
			continue
		}
		if err != nil {
			return 0, err
		}
	}
}

func (s *partSequence) Close() error {
	if s.current == nil {
		return nil
	}
	err := s.current.Close()
	s.current = nil
	return err
}

func (h *Handler) serveMultipartRequest(w http.ResponseWriter, r *http.Request, bucket Bucket, credential Credential, key string) {
	if h.multipartDir == "" {
		writeError(w, r, http.StatusInternalServerError, "InternalError", "The multipart spool is unavailable.")
		return
	}
	uploadID := r.URL.Query().Get("uploadId")
	switch {
	case r.Method == http.MethodPost && hasQueryFlag(r, "uploads"):
		h.createMultipartUpload(w, r, bucket, credential, key)
	case r.Method == http.MethodPut && uploadID != "" && hasQueryFlag(r, "partNumber"):
		h.uploadPart(w, r, bucket, credential, key, uploadID)
	case r.Method == http.MethodPost && uploadID != "":
		h.completeMultipartUpload(w, r, bucket, credential, key, uploadID)
	case r.Method == http.MethodDelete && uploadID != "":
		h.abortMultipartUpload(w, r, bucket, credential, key, uploadID)
	case r.Method == http.MethodGet && uploadID != "":
		h.listParts(w, r, bucket, credential, key, uploadID)
	default:
		writeError(w, r, http.StatusNotImplemented, "NotImplemented", "The requested operation is not implemented yet.")
	}
}

func (h *Handler) loadUpload(bucket Bucket, key, uploadID string) (multipartJournal, string, error) {
	dir := h.uploadDir(uploadID)
	journal, err := loadMultipartJournal(dir)
	if err != nil {
		return multipartJournal{}, "", errNoSuchUpload
	}
	if journal.Bucket != bucket.Name || journal.Key != key {
		return multipartJournal{}, "", errNoSuchUpload
	}
	return journal, dir, nil
}

type initiateMultipartUploadResult struct {
	XMLName  xml.Name `xml:"InitiateMultipartUploadResult"`
	XMLNS    string   `xml:"xmlns,attr"`
	Bucket   string
	Key      string
	UploadID string `xml:"UploadId"`
}

func (h *Handler) createMultipartUpload(w http.ResponseWriter, r *http.Request, bucket Bucket, credential Credential, key string) {
	if !credential.canWrite(bucket.Name) {
		writeError(w, r, http.StatusForbidden, "AccessDenied", "Access Denied.")
		return
	}
	var id [16]byte
	if _, err := rand.Read(id[:]); err != nil {
		writeError(w, r, http.StatusInternalServerError, "InternalError", err.Error())
		return
	}
	uploadID := hex.EncodeToString(id[:])
	dir := h.uploadDir(uploadID)
	if err := os.Mkdir(dir, multipartSpoolMode); err != nil {
		writeError(w, r, http.StatusInternalServerError, "InternalError", err.Error())
		return
	}
	journal := multipartJournal{
		Bucket:       bucket.Name,
		Key:          key,
		ContentType:  objectContentType(r.Header),
		UserMetadata: objectUserMetadata(r.Header),
		Initiated:    h.now().UTC().Unix(),
	}
	if err := writeMultipartJournal(dir, journal); err != nil {
		_ = os.RemoveAll(dir)
		writeError(w, r, http.StatusInternalServerError, "InternalError", err.Error())
		return
	}
	writeXML(w, http.StatusOK, initiateMultipartUploadResult{
		XMLNS:    s3XMLNamespace,
		Bucket:   bucket.Name,
		Key:      key,
		UploadID: uploadID,
	})
}

func (h *Handler) uploadPart(w http.ResponseWriter, r *http.Request, bucket Bucket, credential Credential, key, uploadID string) {
	if !credential.canWrite(bucket.Name) {
		writeError(w, r, http.StatusForbidden, "AccessDenied", "Access Denied.")
		return
	}
	partNumber, err := strconv.Atoi(r.URL.Query().Get("partNumber"))
	if err != nil || partNumber < 1 || partNumber > maxMultipartPartNumber {
		writeError(w, r, http.StatusBadRequest, "InvalidArgument", "Part number must be an integer between 1 and 10000, inclusive.")
		return
	}
	journal, dir, err := h.loadUpload(bucket, key, uploadID)
	if err != nil {
		writeError(w, r, http.StatusNotFound, "NoSuchUpload", "The specified upload does not exist.")
		return
	}
	payload, decodedLength, err := newVerifiedPayload(r, credential)
	if err != nil {
		writeObjectError(w, r, fmt.Errorf("%w: %v", errInvalidPayload, err))
		return
	}
	defer func() { _ = payload.Close() }()

	// S3 allows concurrent UploadPart: part data spools to a private temp file
	// and only the rename plus journal update run under the per-upload lock.
	temp, err := os.CreateTemp(dir, ".part-*")
	if err != nil {
		if os.IsNotExist(err) {
			writeError(w, r, http.StatusNotFound, "NoSuchUpload", "The specified upload does not exist.")
		} else {
			writeError(w, r, http.StatusInternalServerError, "InternalError", err.Error())
		}
		return
	}
	tempName := temp.Name()
	defer func() { _ = os.Remove(tempName) }()
	buffer := multipartCopyBuffers.Get().(*[]byte)
	digest := md5.New()
	written, copyErr := io.CopyBuffer(io.MultiWriter(temp, digest), payload, *buffer)
	multipartCopyBuffers.Put(buffer)
	syncErr := temp.Sync()
	closeErr := temp.Close()
	if copyErr != nil || syncErr != nil || closeErr != nil {
		writeObjectError(w, r, errors.Join(copyErr, syncErr, closeErr))
		return
	}
	if written != decodedLength {
		writeError(w, r, http.StatusBadRequest, "IncompleteBody", "You did not provide the number of bytes specified by the Content-Length HTTP header.")
		return
	}
	etag := `"` + hex.EncodeToString(digest.Sum(nil)) + `"`

	lock, err := lockMultipartJournal(dir)
	if err != nil {
		writeError(w, r, http.StatusInternalServerError, "InternalError", err.Error())
		return
	}
	defer func() { _ = lock.Close() }()
	if err := os.Rename(tempName, filepath.Join(dir, "part-"+strconv.Itoa(partNumber))); err != nil {
		writeError(w, r, http.StatusInternalServerError, "InternalError", err.Error())
		return
	}
	journal, err = loadMultipartJournal(dir)
	if err != nil {
		writeError(w, r, http.StatusNotFound, "NoSuchUpload", "The specified upload does not exist.")
		return
	}
	journal.Parts = replaceMultipartPart(journal.Parts, multipartPart{Number: partNumber, Size: written, ETag: etag})
	if err := writeMultipartJournal(dir, journal); err != nil {
		writeError(w, r, http.StatusInternalServerError, "InternalError", err.Error())
		return
	}
	w.Header().Set("ETag", etag)
	w.WriteHeader(http.StatusOK)
}

func replaceMultipartPart(parts []multipartPart, part multipartPart) []multipartPart {
	for i := range parts {
		if parts[i].Number == part.Number {
			parts[i] = part
			return parts
		}
	}
	index := len(parts)
	for i := range parts {
		if parts[i].Number > part.Number {
			index = i
			break
		}
	}
	parts = append(parts, multipartPart{})
	copy(parts[index+1:], parts[index:])
	parts[index] = part
	return parts
}

type completeMultipartUploadRequest struct {
	XMLName xml.Name              `xml:"CompleteMultipartUpload"`
	Parts   []completeRequestPart `xml:"Part"`
}

type completeRequestPart struct {
	PartNumber int    `xml:"PartNumber"`
	ETag       string `xml:"ETag"`
}

type completeMultipartUploadResult struct {
	XMLName xml.Name `xml:"CompleteMultipartUploadResult"`
	XMLNS   string   `xml:"xmlns,attr"`
	Bucket  string
	Key     string
	ETag    string
}

func (h *Handler) completeMultipartUpload(w http.ResponseWriter, r *http.Request, bucket Bucket, credential Credential, key, uploadID string) {
	if !credential.canWrite(bucket.Name) {
		writeError(w, r, http.StatusForbidden, "AccessDenied", "Access Denied.")
		return
	}
	journal, dir, err := h.loadUpload(bucket, key, uploadID)
	if err != nil {
		writeError(w, r, http.StatusNotFound, "NoSuchUpload", "The specified upload does not exist.")
		return
	}
	payload, _, err := newVerifiedPayload(r, credential)
	if err != nil {
		writeObjectError(w, r, fmt.Errorf("%w: %v", errInvalidPayload, err))
		return
	}
	data, readErr := io.ReadAll(io.LimitReader(payload, 1<<20))
	closeErr := payload.Close()
	if readErr != nil || closeErr != nil {
		writeObjectError(w, r, errors.Join(readErr, closeErr))
		return
	}
	var request completeMultipartUploadRequest
	if err := xml.Unmarshal(data, &request); err != nil {
		writeError(w, r, http.StatusBadRequest, "MalformedXML", "The XML you provided was not well-formed or did not validate against our published schema.")
		return
	}
	if len(request.Parts) == 0 {
		writeError(w, r, http.StatusBadRequest, "InvalidRequest", "You must specify at least one part.")
		return
	}

	// the lock spans the whole publish so late UploadPart requests cannot
	// mutate the journal under the part list being committed
	lock, err := lockMultipartJournal(dir)
	if err != nil {
		writeError(w, r, http.StatusInternalServerError, "InternalError", err.Error())
		return
	}
	defer func() { _ = lock.Close() }()
	journal, err = loadMultipartJournal(dir)
	if err != nil {
		writeError(w, r, http.StatusNotFound, "NoSuchUpload", "The specified upload does not exist.")
		return
	}
	parts := make([]multipartPart, 0, len(request.Parts))
	var total int64
	for i, requested := range request.Parts {
		if i > 0 && requested.PartNumber <= request.Parts[i-1].PartNumber {
			writeError(w, r, http.StatusBadRequest, "InvalidPartOrder", "The list of parts was not in ascending order.")
			return
		}
		found := false
		for _, part := range journal.Parts {
			if part.Number == requested.PartNumber && strings.Trim(part.ETag, `"`) == strings.Trim(requested.ETag, `"`) {
				parts = append(parts, part)
				total += part.Size
				found = true
				break
			}
		}
		if !found {
			writeError(w, r, http.StatusBadRequest, "InvalidPart", "One or more of the specified parts could not be found.")
			return
		}
	}
	etag := multipartETag(parts)
	sequence := &partSequence{dir: dir, numbers: partNumbers(parts)}
	err = h.publishObject(r.Context(), bucket, credential, key, objectUpload{
		Stream:      sequence,
		Size:        total,
		ContentType: journal.ContentType,
		Metadata:    journal.UserMetadata,
		ETag:        func() string { return etag },
	})
	_ = sequence.Close()
	if err != nil {
		writeObjectError(w, r, err)
		return
	}
	if err := os.RemoveAll(dir); err != nil {
		writeError(w, r, http.StatusInternalServerError, "InternalError", err.Error())
		return
	}
	writeXML(w, http.StatusOK, completeMultipartUploadResult{
		XMLNS:  s3XMLNamespace,
		Bucket: bucket.Name,
		Key:    key,
		ETag:   etag,
	})
}

func partNumbers(parts []multipartPart) []int {
	numbers := make([]int, len(parts))
	for i, part := range parts {
		numbers[i] = part.Number
	}
	return numbers
}

func multipartETag(parts []multipartPart) string {
	digests := make([]byte, 0, len(parts)*md5.Size)
	for _, part := range parts {
		raw, err := hex.DecodeString(strings.Trim(part.ETag, `"`))
		if err != nil || len(raw) != md5.Size {
			return ""
		}
		digests = append(digests, raw...)
	}
	sum := md5.Sum(digests)
	return `"` + hex.EncodeToString(sum[:]) + "-" + strconv.Itoa(len(parts)) + `"`
}

func (h *Handler) abortMultipartUpload(w http.ResponseWriter, r *http.Request, bucket Bucket, credential Credential, key, uploadID string) {
	if !credential.canWrite(bucket.Name) {
		writeError(w, r, http.StatusForbidden, "AccessDenied", "Access Denied.")
		return
	}
	_, dir, err := h.loadUpload(bucket, key, uploadID)
	if err != nil {
		writeError(w, r, http.StatusNotFound, "NoSuchUpload", "The specified upload does not exist.")
		return
	}
	if err := os.RemoveAll(dir); err != nil {
		writeError(w, r, http.StatusInternalServerError, "InternalError", err.Error())
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

type listPartsResult struct {
	XMLName              xml.Name            `xml:"ListPartsResult"`
	XMLNS                string              `xml:"xmlns,attr"`
	Bucket               string
	Key                  string
	UploadID             string `xml:"UploadId"`
	StorageClass         string
	PartNumberMarker     int
	NextPartNumberMarker int `xml:",omitempty"`
	MaxParts             int
	IsTruncated          bool
	Parts                []listPartsEntry `xml:"Part"`
}

type listPartsEntry struct {
	PartNumber   int
	LastModified string
	ETag         string
	Size         int64
}

func (h *Handler) listParts(w http.ResponseWriter, r *http.Request, bucket Bucket, credential Credential, key, uploadID string) {
	if !credential.canRead(bucket.Name) {
		writeError(w, r, http.StatusForbidden, "AccessDenied", "Access Denied.")
		return
	}
	journal, _, err := h.loadUpload(bucket, key, uploadID)
	if err != nil {
		writeError(w, r, http.StatusNotFound, "NoSuchUpload", "The specified upload does not exist.")
		return
	}
	query := r.URL.Query()
	maxParts := maxListKeys
	if raw := query.Get("max-parts"); raw != "" {
		value, parseErr := strconv.Atoi(raw)
		if parseErr != nil || value < 0 {
			writeError(w, r, http.StatusBadRequest, "InvalidArgument", "max-parts must be an integer between 0 and 2147483647.")
			return
		}
		maxParts = min(value, maxListKeys)
	}
	marker, err := strconv.Atoi(query.Get("part-number-marker"))
	if err != nil {
		marker = 0
	}
	lastModified := time.Unix(journal.Initiated, 0).UTC().Format(time.RFC3339)
	result := listPartsResult{
		XMLNS:            s3XMLNamespace,
		Bucket:           bucket.Name,
		Key:              key,
		UploadID:         uploadID,
		StorageClass:     "STANDARD",
		PartNumberMarker: marker,
		MaxParts:         maxParts,
		Parts:            make([]listPartsEntry, 0, min(maxParts, len(journal.Parts))),
	}
	for _, part := range journal.Parts {
		if part.Number <= marker {
			continue
		}
		if len(result.Parts) == maxParts && maxParts > 0 {
			result.IsTruncated = true
			break
		}
		if len(result.Parts) > 0 {
			result.NextPartNumberMarker = result.Parts[len(result.Parts)-1].PartNumber
		}
		result.Parts = append(result.Parts, listPartsEntry{
			PartNumber:   part.Number,
			LastModified: lastModified,
			ETag:         part.ETag,
			Size:         part.Size,
		})
	}
	if !result.IsTruncated && len(result.Parts) > 0 {
		result.NextPartNumberMarker = result.Parts[len(result.Parts)-1].PartNumber
	}
	writeXML(w, http.StatusOK, result)
}

type listMultipartUploadsResult struct {
	XMLName           xml.Name             `xml:"ListMultipartUploadsResult"`
	XMLNS             string               `xml:"xmlns,attr"`
	Bucket            string
	KeyMarker         string
	UploadIDMarker    string               `xml:"UploadIdMarker"`
	NextKeyMarker     string               `xml:",omitempty"`
	NextUploadIDMarker string               `xml:"NextUploadIdMarker,omitempty"`
	MaxUploads        int
	IsTruncated       bool
	Uploads           []listUploadsEntry `xml:"Upload"`
	EncodingType      string             `xml:",omitempty"`
}

type listUploadsEntry struct {
	Key       string
	UploadID  string `xml:"UploadId"`
	Initiated string
}

func (h *Handler) listMultipartUploads(w http.ResponseWriter, r *http.Request, bucket Bucket, credential Credential) {
	if !credential.canRead(bucket.Name) {
		writeError(w, r, http.StatusForbidden, "AccessDenied", "Access Denied.")
		return
	}
	if h.multipartDir == "" {
		writeError(w, r, http.StatusInternalServerError, "InternalError", "The multipart spool is unavailable.")
		return
	}
	entries, err := os.ReadDir(h.multipartDir)
	if err != nil && !os.IsNotExist(err) {
		writeObjectError(w, r, err)
		return
	}
	uploads := make([]listUploadsEntry, 0, len(entries))
	for _, entry := range entries {
		if !entry.IsDir() {
			continue
		}
		journal, loadErr := loadMultipartJournal(filepath.Join(h.multipartDir, entry.Name()))
		if loadErr != nil || journal.Bucket != bucket.Name {
			continue
		}
		uploads = append(uploads, listUploadsEntry{
			Key:       journal.Key,
			UploadID:  entry.Name(),
			Initiated: time.Unix(journal.Initiated, 0).UTC().Format(time.RFC3339),
		})
	}
	slices.SortFunc(uploads, func(a, b listUploadsEntry) int {
		if c := strings.Compare(a.Key, b.Key); c != 0 {
			return c
		}
		return strings.Compare(a.UploadID, b.UploadID)
	})

	query := r.URL.Query()
	keyMarker := query.Get("key-marker")
	uploadMarker := query.Get("upload-id-marker")
	maxUploads := maxListKeys
	if raw := query.Get("max-uploads"); raw != "" {
		value, parseErr := strconv.Atoi(raw)
		if parseErr != nil || value < 0 {
			writeError(w, r, http.StatusBadRequest, "InvalidArgument", "max-uploads must be an integer between 0 and 2147483647.")
			return
		}
		maxUploads = min(value, maxListKeys)
	}
	encoding := query.Get("encoding-type")
	result := listMultipartUploadsResult{
		XMLNS:          s3XMLNamespace,
		Bucket:         bucket.Name,
		KeyMarker:      encodeIfURL(encoding, keyMarker),
		UploadIDMarker: uploadMarker,
		MaxUploads:     maxUploads,
		EncodingType:   encoding,
	}
	for _, upload := range uploads {
		if upload.Key < keyMarker || (upload.Key == keyMarker && upload.UploadID <= uploadMarker) {
			continue
		}
		if len(result.Uploads) == maxUploads && maxUploads > 0 {
			result.IsTruncated = true
			break
		}
		result.Uploads = append(result.Uploads, listUploadsEntry{
			Key:       encodeIfURL(encoding, upload.Key),
			UploadID:  upload.UploadID,
			Initiated: upload.Initiated,
		})
		result.NextKeyMarker = upload.Key
		result.NextUploadIDMarker = upload.UploadID
	}
	result.NextKeyMarker = encodeIfURL(encoding, result.NextKeyMarker)
	writeXML(w, http.StatusOK, result)
}

// ReapMultipartUploads removes spooled uploads with no activity for the reap age.
func (h *Handler) ReapMultipartUploads(now time.Time) error {
	if h.multipartDir == "" {
		return nil
	}
	entries, err := os.ReadDir(h.multipartDir)
	if os.IsNotExist(err) {
		return nil
	}
	if err != nil {
		return err
	}
	for _, entry := range entries {
		if !entry.IsDir() {
			continue
		}
		info, err := entry.Info()
		if err != nil {
			return err
		}
		if now.Sub(info.ModTime()) < multipartReapAge {
			continue
		}
		if err := os.RemoveAll(filepath.Join(h.multipartDir, entry.Name())); err != nil {
			return err
		}
	}
	return nil
}
