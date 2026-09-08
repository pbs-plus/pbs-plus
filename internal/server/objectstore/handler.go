//go:build linux

package objectstore

import (
	"crypto/rand"
	"encoding/hex"
	"encoding/xml"
	"fmt"
	"net/http"
	"strings"
	"time"
)

const s3XMLNamespace = "http://s3.amazonaws.com/doc/2006-03-01/"

type Handler struct {
	config    Config
	createdAt time.Time
	now       func() time.Time
}

func NewHandler(config Config, createdAt time.Time) (*Handler, error) {
	if err := config.Validate(); err != nil {
		return nil, err
	}
	return &Handler{config: config, createdAt: createdAt.UTC(), now: time.Now}, nil
}

func (h *Handler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	credential, err := h.config.authenticate(r, h.now().UTC())
	if err != nil {
		writeError(w, r, http.StatusForbidden, "AccessDenied", "Access Denied.")
		return
	}
	path := strings.TrimPrefix(r.URL.Path, "/")
	if path == "" {
		if r.Method != http.MethodGet {
			writeError(w, r, http.StatusNotImplemented, "NotImplemented", "A header you provided implies functionality that is not implemented.")
			return
		}
		h.listBuckets(w, credential)
		return
	}
	bucketName, objectKey, _ := strings.Cut(path, "/")
	if objectKey != "" {
		writeError(w, r, http.StatusNotImplemented, "NotImplemented", "Object operations are not implemented yet.")
		return
	}
	if _, ok := h.config.bucket(bucketName); !ok {
		writeError(w, r, http.StatusNotFound, "NoSuchBucket", "The specified bucket does not exist.")
		return
	}
	if !credential.canAccess(bucketName) {
		writeError(w, r, http.StatusForbidden, "AccessDenied", "Access Denied.")
		return
	}
	switch {
	case r.Method == http.MethodHead:
		w.WriteHeader(http.StatusOK)
	case r.Method == http.MethodGet && hasQueryFlag(r, "location"):
		h.getBucketLocation(w)
	default:
		writeError(w, r, http.StatusNotImplemented, "NotImplemented", "The requested operation is not implemented yet.")
	}
}

type listAllMyBucketsResult struct {
	XMLName xml.Name    `xml:"ListAllMyBucketsResult"`
	XMLNS   string      `xml:"xmlns,attr"`
	Owner   bucketOwner `xml:"Owner"`
	Buckets bucketList  `xml:"Buckets"`
}

type bucketOwner struct {
	ID          string `xml:"ID"`
	DisplayName string `xml:"DisplayName"`
}

type bucketList struct {
	Buckets []bucketEntry `xml:"Bucket"`
}

type bucketEntry struct {
	Name         string `xml:"Name"`
	CreationDate string `xml:"CreationDate"`
}

func (h *Handler) listBuckets(w http.ResponseWriter, credential Credential) {
	names := h.config.bucketNames(credential)
	result := listAllMyBucketsResult{
		XMLNS:   s3XMLNamespace,
		Owner:   bucketOwner{ID: credential.AccessKey, DisplayName: credential.AuthID},
		Buckets: bucketList{Buckets: make([]bucketEntry, 0, len(names))},
	}
	for _, name := range names {
		result.Buckets.Buckets = append(result.Buckets.Buckets, bucketEntry{
			Name: name, CreationDate: h.createdAt.Format(time.RFC3339),
		})
	}
	writeXML(w, http.StatusOK, result)
}

type locationConstraint struct {
	XMLName xml.Name `xml:"LocationConstraint"`
	XMLNS   string   `xml:"xmlns,attr"`
	Region  string   `xml:",chardata"`
}

func (h *Handler) getBucketLocation(w http.ResponseWriter) {
	writeXML(w, http.StatusOK, locationConstraint{XMLNS: s3XMLNamespace, Region: h.config.RegionName()})
}

type errorResponse struct {
	XMLName  xml.Name `xml:"Error"`
	Code     string   `xml:"Code"`
	Message  string   `xml:"Message"`
	Resource string   `xml:"Resource,omitempty"`
	RequestID string  `xml:"RequestId"`
}

func writeError(w http.ResponseWriter, r *http.Request, status int, code, message string) {
	writeXML(w, status, errorResponse{Code: code, Message: message, Resource: r.URL.Path, RequestID: requestID()})
}

func writeXML(w http.ResponseWriter, status int, value any) {
	data, err := xml.Marshal(value)
	if err != nil {
		http.Error(w, "encoding response", http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "application/xml")
	w.WriteHeader(status)
	_, _ = fmt.Fprintf(w, "%s%s", xml.Header, data)
}

func hasQueryFlag(r *http.Request, name string) bool {
	_, ok := r.URL.Query()[name]
	return ok
}

func requestID() string {
	var id [12]byte
	if _, err := rand.Read(id[:]); err != nil {
		return ""
	}
	return hex.EncodeToString(id[:])
}
