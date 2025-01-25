//go:build windows

package readonly_cache

import (
	"bytes"
	"encoding/binary"
	"errors"
	"io/fs"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/go-git/go-billy/v5"
	"github.com/google/uuid"
	"github.com/willscott/go-nfs"
)

const verifierExpiration = 30 * time.Minute

var verifierPool = sync.Pool{
	New: func() interface{} {
		return &verifier{
			contents: make([]fs.FileInfo, 0, 1024),
		}
	},
}

type verifier struct {
	path     string
	contents []fs.FileInfo
	created  time.Time
}

func (v *verifier) reset() {
	v.path = ""
	v.contents = v.contents[:0]
	v.created = time.Time{}
}

type ReadOnlyHandler struct {
	nfs.Handler
	mu              sync.RWMutex
	fsMap           *sync.Map
	idMap           *sync.Map
	activeVerifiers *sync.Map
	bufferPool      sync.Pool
}

func NewReadOnlyHandler(h nfs.Handler) nfs.Handler {
	return &ReadOnlyHandler{
		Handler:         h,
		fsMap:           &sync.Map{},
		idMap:           &sync.Map{},
		activeVerifiers: &sync.Map{},
		bufferPool: sync.Pool{
			New: func() interface{} {
				return bytes.NewBuffer(make([]byte, 0, 256))
			},
		},
	}
}

func validatePath(path []string) error {
	for _, p := range path {
		if p == "" {
			return errors.New("empty path component")
		}

		if p == ".." || strings.Contains(p, "../") || strings.Contains(p, "/..") {
			return errors.New("path traversal not allowed")
		}

		if filepath.Clean(p) != p {
			return errors.New("path contains invalid characters")
		}
	}
	return nil
}

func (c *ReadOnlyHandler) ToHandle(f billy.Filesystem, path []string) []byte {
	if err := validatePath(path); err != nil {
		return nil
	}

	c.mu.RLock()
	defer c.mu.RUnlock()

	var fsid uuid.UUID
	if id, ok := c.fsMap.Load(f); ok {
		fsid = id.(uuid.UUID)
	} else {
		fsid = uuid.New()
		c.fsMap.Store(f, fsid)
		c.idMap.Store(fsid, f)
	}

	buf := c.bufferPool.Get().(*bytes.Buffer)
	buf.Reset()
	defer c.bufferPool.Put(buf)

	buf.Write(fsid[:])
	binary.Write(buf, binary.LittleEndian, uint16(len(path)))
	for _, p := range path {
		binary.Write(buf, binary.LittleEndian, uint16(len(p)))
		buf.WriteString(p)
	}

	result := make([]byte, buf.Len())
	copy(result, buf.Bytes())
	return result
}

func (c *ReadOnlyHandler) FromHandle(fh []byte) (billy.Filesystem, []string, error) {
	if len(fh) < 18 {
		return nil, nil, &nfs.NFSStatusError{NFSStatus: nfs.NFSStatusInval}
	}

	var fsid uuid.UUID
	copy(fsid[:], fh[:16])

	c.mu.RLock()
	fs, ok := c.idMap.Load(fsid)
	c.mu.RUnlock()
	if !ok {
		return nil, nil, &nfs.NFSStatusError{NFSStatus: nfs.NFSStatusStale}
	}

	var pathCount uint16
	reader := bytes.NewReader(fh[16:])
	if err := binary.Read(reader, binary.LittleEndian, &pathCount); err != nil {
		return nil, nil, &nfs.NFSStatusError{NFSStatus: nfs.NFSStatusInval}
	}

	path := make([]string, 0, pathCount)
	for i := 0; i < int(pathCount); i++ {
		var partLen uint16
		if err := binary.Read(reader, binary.LittleEndian, &partLen); err != nil {
			return nil, nil, &nfs.NFSStatusError{NFSStatus: nfs.NFSStatusInval}
		}
		part := make([]byte, partLen)
		if _, err := reader.Read(part); err != nil {
			return nil, nil, &nfs.NFSStatusError{NFSStatus: nfs.NFSStatusInval}
		}
		path = append(path, string(part))
	}

	if err := validatePath(path); err != nil {
		return nil, nil, &nfs.NFSStatusError{NFSStatus: nfs.NFSStatusInval}
	}

	return fs.(billy.Filesystem), path, nil
}

func (c *ReadOnlyHandler) VerifierFor(path string, contents []fs.FileInfo) uint64 {
	v := verifierPool.Get().(*verifier)
	v.path = path
	v.contents = append(v.contents, contents...)
	v.created = time.Now()

	h := hashPathAndContents(path, contents)
	c.activeVerifiers.Store(h, v)
	return h
}

func (c *ReadOnlyHandler) DataForVerifier(id uint64) []fs.FileInfo {
	if v, ok := c.activeVerifiers.Load(id); ok {
		verifier := v.(*verifier)
		if time.Since(verifier.created) > verifierExpiration {
			c.activeVerifiers.Delete(id)
			verifier.reset()
			verifierPool.Put(verifier)
			return nil
		}
		return verifier.contents
	}
	return nil
}

func hashPathAndContents(path string, contents []fs.FileInfo) uint64 {
	h := uint64(14695981039346656037)
	for i := 0; i < len(path); i++ {
		h ^= uint64(path[i])
		h *= uint64(1099511628211)
	}
	for _, c := range contents {
		name := c.Name()
		for i := 0; i < len(name); i++ {
			h ^= uint64(name[i])
			h *= uint64(1099511628211)
		}
		h ^= uint64(c.Mode())
		h *= uint64(1099511628211)
		h ^= uint64(c.ModTime().UnixNano())
		h *= uint64(1099511628211)
	}
	return h
}

func (c *ReadOnlyHandler) InvalidateHandle(_ billy.Filesystem, _ []byte) error {
	return nil
}

func (c *ReadOnlyHandler) HandleLimit() int {
	return int(^uint(0) >> 1)
}

func (c *ReadOnlyHandler) Cleanup() {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.activeVerifiers.Range(func(key, value interface{}) bool {
		v := value.(*verifier)
		v.reset()
		verifierPool.Put(v)
		return true
	})

	c.fsMap = &sync.Map{}
	c.idMap = &sync.Map{}
	c.activeVerifiers = &sync.Map{}
}
