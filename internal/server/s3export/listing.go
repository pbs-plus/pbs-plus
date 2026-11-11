//go:build linux

package s3export

import (
	"encoding/base64"
	"encoding/xml"
	"net/http"
	"net/url"
	"os"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/go-git/go-billy/v5"
	"github.com/pbs-plus/pbs-plus/internal/log"
)

const maxListKeys = 1000

type listItem struct {
	key    string
	prefix bool
	info   os.FileInfo
}

type dirEntry struct {
	name string
	dir  bool
	info os.FileInfo
}

// sortKey orders a directory as name+"/" so a depth-first walk yields keys in S3 byte order.
func (e dirEntry) sortKey() string {
	if e.dir {
		return e.name + "/"
	}
	return e.name
}

func readSorted(fs billy.Filesystem, dirKey string) ([]dirEntry, error) {
	infos, err := fs.ReadDir("/" + strings.TrimSuffix(dirKey, "/"))
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil
		}
		return nil, err
	}
	entries := make([]dirEntry, 0, len(infos))
	for _, info := range infos {
		switch {
		case info.IsDir():
			entries = append(entries, dirEntry{name: info.Name(), dir: true, info: info})
		case info.Mode().IsRegular():
			entries = append(entries, dirEntry{name: info.Name(), info: info})
		}
	}
	sort.Slice(entries, func(i, j int) bool { return entries[i].sortKey() < entries[j].sortKey() })
	return entries, nil
}

// lister collects up to max keys after `after` under prefix; delimiter "/" groups subdirectories into common prefixes.
type lister struct {
	fs        billy.Filesystem
	prefix    string
	after     string
	delimited bool
	max       int
	items     []listItem
	truncated bool
}

func (l *lister) add(item listItem) bool {
	if len(l.items) == l.max {
		l.truncated = true
		return false
	}
	l.items = append(l.items, item)
	return true
}

func (l *lister) run() error {
	dirKey := l.prefix[:strings.LastIndex(l.prefix, "/")+1]
	if l.delimited {
		return l.listDir(dirKey)
	}
	_, err := l.walk(dirKey)
	return err
}

func (l *lister) listDir(dirKey string) error {
	entries, err := readSorted(l.fs, dirKey)
	if err != nil {
		return err
	}
	for _, e := range entries {
		key := dirKey + e.sortKey()
		if !strings.HasPrefix(key, l.prefix) || key <= l.after {
			continue
		}
		if !l.add(listItem{key: key, prefix: e.dir, info: e.info}) {
			return nil
		}
	}
	return nil
}

func (l *lister) walk(dirKey string) (bool, error) {
	entries, err := readSorted(l.fs, dirKey)
	if err != nil {
		return false, err
	}
	for _, e := range entries {
		key := dirKey + e.sortKey()
		if e.dir {
			if !strings.HasPrefix(key, l.prefix) && !strings.HasPrefix(l.prefix, key) {
				continue
			}
			if l.after != "" && key < l.after && !strings.HasPrefix(l.after, key) {
				continue
			}
			if stop, err := l.walk(key); stop || err != nil {
				return stop, err
			}
			continue
		}
		if !strings.HasPrefix(key, l.prefix) || key <= l.after {
			continue
		}
		if !l.add(listItem{key: key, info: e.info}) {
			return true, nil
		}
	}
	return false, nil
}

type listResult struct {
	XMLName               xml.Name       `xml:"ListBucketResult"`
	Namespace             string         `xml:"xmlns,attr"`
	Name                  string         `xml:"Name"`
	Prefix                string         `xml:"Prefix"`
	Marker                *string        `xml:"Marker,omitempty"`
	NextMarker            string         `xml:"NextMarker,omitempty"`
	StartAfter            string         `xml:"StartAfter,omitempty"`
	ContinuationToken     string         `xml:"ContinuationToken,omitempty"`
	NextContinuationToken string         `xml:"NextContinuationToken,omitempty"`
	KeyCount              *int           `xml:"KeyCount,omitempty"`
	MaxKeys               int            `xml:"MaxKeys"`
	Delimiter             string         `xml:"Delimiter,omitempty"`
	EncodingType          string         `xml:"EncodingType,omitempty"`
	IsTruncated           bool           `xml:"IsTruncated"`
	Contents              []listContents `xml:"Contents"`
	CommonPrefixes        []commonPrefix `xml:"CommonPrefixes"`
}

type listContents struct {
	Key          string `xml:"Key"`
	LastModified string `xml:"LastModified"`
	ETag         string `xml:"ETag"`
	Size         int64  `xml:"Size"`
	StorageClass string `xml:"StorageClass"`
}

type commonPrefix struct {
	Prefix string `xml:"Prefix"`
}

func listObjects(w http.ResponseWriter, r *http.Request, bucketName string, fs billy.Filesystem) {
	q := r.URL.Query()
	delimiter := q.Get("delimiter")
	if delimiter != "" && delimiter != "/" {
		writeError(w, r, http.StatusNotImplemented, "NotImplemented", "Only the '/' delimiter is supported.")
		return
	}
	maxKeys := maxListKeys
	if raw := q.Get("max-keys"); raw != "" {
		n, err := strconv.Atoi(raw)
		if err != nil || n < 0 {
			writeError(w, r, http.StatusBadRequest, "InvalidArgument", "max-keys must be a non-negative integer.")
			return
		}
		maxKeys = min(n, maxListKeys)
	}
	v2 := q.Get("list-type") == "2"
	after := q.Get("marker")
	if v2 {
		after = q.Get("start-after")
		if token := q.Get("continuation-token"); token != "" {
			decoded, err := base64.RawURLEncoding.DecodeString(token)
			if err != nil {
				writeError(w, r, http.StatusBadRequest, "InvalidArgument", "The continuation token provided is incorrect.")
				return
			}
			after = string(decoded)
		}
	}
	l := &lister{fs: fs, prefix: q.Get("prefix"), after: after, delimited: delimiter == "/", max: maxKeys}
	if maxKeys > 0 {
		if err := l.run(); err != nil {
			log.Error(err, "s3 export list "+bucketName)
			writeError(w, r, http.StatusInternalServerError, "InternalError", "We encountered an internal error. Please try again.")
			return
		}
	}
	urlEncode := q.Get("encoding-type") == "url"
	encode := func(s string) string {
		if urlEncode {
			return strings.ReplaceAll(url.QueryEscape(s), "+", "%20")
		}
		return s
	}
	result := listResult{
		Namespace:   s3Namespace,
		Name:        bucketName,
		Prefix:      encode(l.prefix),
		MaxKeys:     maxKeys,
		Delimiter:   encode(delimiter),
		IsTruncated: l.truncated,
	}
	if urlEncode {
		result.EncodingType = "url"
	}
	for _, item := range l.items {
		if item.prefix {
			result.CommonPrefixes = append(result.CommonPrefixes, commonPrefix{Prefix: encode(item.key)})
			continue
		}
		result.Contents = append(result.Contents, listContents{
			Key:          encode(item.key),
			LastModified: item.info.ModTime().UTC().Format(time.RFC3339),
			ETag:         etag(item.info.Size(), item.info.ModTime()),
			Size:         item.info.Size(),
			StorageClass: "STANDARD",
		})
	}
	var last string
	if n := len(l.items); n > 0 {
		last = l.items[n-1].key
	}
	if v2 {
		count := len(l.items)
		result.KeyCount = &count
		result.StartAfter = encode(q.Get("start-after"))
		result.ContinuationToken = q.Get("continuation-token")
		if l.truncated {
			result.NextContinuationToken = base64.RawURLEncoding.EncodeToString([]byte(last))
		}
	} else {
		marker := encode(after)
		result.Marker = &marker
		if l.truncated {
			result.NextMarker = encode(last)
		}
	}
	writeXML(w, http.StatusOK, result)
}
