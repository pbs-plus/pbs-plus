//go:build linux

package s3export

import (
	"crypto/rand"
	"encoding/hex"
	"encoding/xml"
	"fmt"
	"net/http"
	"path"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/go-git/go-billy/v5"
	"github.com/pbs-plus/pbs-plus/internal/log"
	"github.com/pbs-plus/pbs-plus/internal/server/objectstore"
)

const s3Namespace = "http://s3.amazonaws.com/doc/2006-03-01/"

type bucket struct {
	share   string
	fs      billy.Filesystem
	created time.Time
}

// Handler serves every added share as a read-only bucket.
type Handler struct {
	config Config
	now    func() time.Time

	mu      sync.RWMutex
	buckets map[string]bucket
}

func NewHandler(config Config) *Handler {
	return &Handler{config: config, now: time.Now, buckets: map[string]bucket{}}
}

func (h *Handler) Add(share string, fs billy.Filesystem) error {
	name := BucketName(share)
	h.mu.Lock()
	defer h.mu.Unlock()
	if existing, ok := h.buckets[name]; ok {
		return fmt.Errorf("share %q maps to bucket %q, already used by share %q", share, name, existing.share)
	}
	h.buckets[name] = bucket{share: share, fs: fs, created: h.now().UTC()}
	return nil
}

func (h *Handler) Remove(share string) {
	h.mu.Lock()
	defer h.mu.Unlock()
	if b, ok := h.buckets[BucketName(share)]; ok && b.share == share {
		delete(h.buckets, BucketName(share))
	}
}

func (h *Handler) Shares() []string {
	h.mu.RLock()
	defer h.mu.RUnlock()
	shares := make([]string, 0, len(h.buckets))
	for _, b := range h.buckets {
		shares = append(shares, b.share)
	}
	sort.Strings(shares)
	return shares
}

func (h *Handler) bucket(name string) (bucket, bool) {
	h.mu.RLock()
	defer h.mu.RUnlock()
	b, ok := h.buckets[name]
	return b, ok
}

func (h *Handler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("x-amz-request-id", requestID())
	if _, err := objectstore.VerifySigV4(r, h.now().UTC(), h.config.RegionName(), h.config.secret); err != nil {
		writeError(w, r, http.StatusForbidden, "AccessDenied", "Access Denied.")
		return
	}
	if r.Method != http.MethodGet && r.Method != http.MethodHead {
		writeError(w, r, http.StatusForbidden, "AccessDenied", "This endpoint is read-only.")
		return
	}
	bucketName, key, _ := strings.Cut(strings.TrimPrefix(r.URL.Path, "/"), "/")
	if bucketName == "" {
		h.listBuckets(w)
		return
	}
	b, ok := h.bucket(bucketName)
	if !ok {
		writeError(w, r, http.StatusNotFound, "NoSuchBucket", "The specified bucket does not exist.")
		return
	}
	if key != "" {
		serveObject(w, r, b.fs, key)
		return
	}
	switch {
	case r.Method == http.MethodHead:
		w.WriteHeader(http.StatusOK)
	case r.URL.Query().Has("location"):
		writeXML(w, http.StatusOK, locationConstraint{Namespace: s3Namespace, Region: h.config.RegionName()})
	default:
		listObjects(w, r, bucketName, b.fs)
	}
}

type listAllMyBucketsResult struct {
	XMLName   xml.Name      `xml:"ListAllMyBucketsResult"`
	Namespace string        `xml:"xmlns,attr"`
	Owner     owner         `xml:"Owner"`
	Buckets   []bucketEntry `xml:"Buckets>Bucket"`
}

type owner struct {
	ID          string `xml:"ID"`
	DisplayName string `xml:"DisplayName"`
}

type bucketEntry struct {
	Name         string `xml:"Name"`
	CreationDate string `xml:"CreationDate"`
}

type locationConstraint struct {
	XMLName   xml.Name `xml:"LocationConstraint"`
	Namespace string   `xml:"xmlns,attr"`
	Region    string   `xml:",chardata"`
}

func (h *Handler) listBuckets(w http.ResponseWriter) {
	h.mu.RLock()
	entries := make([]bucketEntry, 0, len(h.buckets))
	for name, b := range h.buckets {
		entries = append(entries, bucketEntry{Name: name, CreationDate: b.created.Format(time.RFC3339)})
	}
	h.mu.RUnlock()
	sort.Slice(entries, func(i, j int) bool { return entries[i].Name < entries[j].Name })
	writeXML(w, http.StatusOK, listAllMyBucketsResult{
		Namespace: s3Namespace,
		Owner:     owner{ID: "pbs-plus", DisplayName: "pbs-plus"},
		Buckets:   entries,
	})
}

// objectPath maps a key onto a filesystem path, rejecting keys that are not already clean.
func objectPath(key string) (string, bool) {
	p := "/" + key
	return p, path.Clean(p) == p
}

// etag is derived from size and mtime; the "-1" suffix marks it as non-MD5 so clients skip content hash checks.
func etag(size int64, mtime time.Time) string {
	return fmt.Sprintf(`"%016x%016x-1"`, size, mtime.UnixNano())
}

func serveObject(w http.ResponseWriter, r *http.Request, fs billy.Filesystem, key string) {
	p, ok := objectPath(key)
	if !ok {
		writeError(w, r, http.StatusNotFound, "NoSuchKey", "The specified key does not exist.")
		return
	}
	info, err := fs.Lstat(p)
	if err != nil || !info.Mode().IsRegular() {
		writeError(w, r, http.StatusNotFound, "NoSuchKey", "The specified key does not exist.")
		return
	}
	f, err := fs.Open(p)
	if err != nil {
		log.Error(err, "s3 export open "+key)
		writeError(w, r, http.StatusInternalServerError, "InternalError", "We encountered an internal error. Please try again.")
		return
	}
	defer func() {
		if err := f.Close(); err != nil {
			log.Error(err, "s3 export close "+key)
		}
	}()
	w.Header().Set("Content-Type", "binary/octet-stream")
	w.Header().Set("ETag", etag(info.Size(), info.ModTime()))
	w.Header().Set("Accept-Ranges", "bytes")
	http.ServeContent(w, r, "", info.ModTime(), f)
}

type errorResponse struct {
	XMLName   xml.Name `xml:"Error"`
	Code      string   `xml:"Code"`
	Message   string   `xml:"Message"`
	Resource  string   `xml:"Resource"`
	RequestID string   `xml:"RequestId"`
}

func writeError(w http.ResponseWriter, r *http.Request, status int, code, message string) {
	if r.Method == http.MethodHead {
		w.WriteHeader(status)
		return
	}
	writeXML(w, status, errorResponse{Code: code, Message: message, Resource: r.URL.Path, RequestID: w.Header().Get("x-amz-request-id")})
}

func writeXML(w http.ResponseWriter, status int, value any) {
	w.Header().Set("Content-Type", "application/xml")
	w.WriteHeader(status)
	if _, err := w.Write([]byte(xml.Header)); err != nil {
		return
	}
	if err := xml.NewEncoder(w).Encode(value); err != nil {
		log.Error(err, "s3 export response")
	}
}

func requestID() string {
	var b [8]byte
	if _, err := rand.Read(b[:]); err != nil {
		return "0000000000000000"
	}
	return strings.ToUpper(hex.EncodeToString(b[:]))
}
