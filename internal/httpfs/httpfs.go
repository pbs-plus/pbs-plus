package httpfs

import (
	"context"
	"crypto/tls"
	"fmt"
	"hash/fnv"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"sync"
	"syscall"
	"time"

	"github.com/hanwen/go-fuse/v2/fs"
	"github.com/hanwen/go-fuse/v2/fuse"
	"github.com/quic-go/quic-go"
	"github.com/quic-go/quic-go/http3"
)

// Config holds the configuration for the HTTP filesystem
type Config struct {
	BaseURL     string        // Base URL for the HTTP server
	UseHTTP3    bool          // Whether to use HTTP/3
	Timeout     time.Duration // Timeout for HTTP requests
	MaxCacheAge time.Duration // Maximum age for cached entries
}

// FileSystem represents the HTTP filesystem
type FileSystem struct {
	config Config
	client *http.Client
	cache  *cache
}

// cache implements a simple in-memory cache for file metadata and contents
type cache struct {
	sync.RWMutex
	entries map[string]*cacheEntry
}

type cacheEntry struct {
	attr     *fuse.Attr
	content  []byte
	modTime  time.Time
	expireAt time.Time
}

// generateInode creates a stable inode number from a path
func generateInode(path string) uint64 {
	h := fnv.New64a()
	h.Write([]byte(path))
	return h.Sum64()
}

// NewFileSystem creates a new HTTP filesystem
func NewFileSystem(config Config) (*FileSystem, error) {
	if config.Timeout == 0 {
		config.Timeout = 30 * time.Second
	}
	if config.MaxCacheAge == 0 {
		config.MaxCacheAge = 5 * time.Minute
	}

	var client *http.Client
	if config.UseHTTP3 {
		client = &http.Client{
			Transport: &http3.Transport{
				TLSClientConfig: &tls.Config{
					MinVersion: tls.VersionTLS13,
				},
				QUICConfig: &quic.Config{
					KeepAlivePeriod: 60 * time.Second,
				},
			},
		}
	} else {
		client = &http.Client{
			Transport: &http.Transport{
				TLSClientConfig: &tls.Config{
					MinVersion: tls.VersionTLS12,
				},
			},
		}
	}

	return &FileSystem{
		config: config,
		client: client,
		cache: &cache{
			entries: make(map[string]*cacheEntry),
		},
	}, nil
}

// Root implements the filesystem interface
func (fs *FileSystem) Root() (fs.InodeEmbedder, error) {
	root := &Dir{
		fs:   fs,
		path: "/",
	}

	// Initialize root attributes
	rootAttr := &fuse.Attr{
		Mode: fuse.S_IFDIR | 0755, // Directory with standard permissions
		Ino:  1,                   // Root inode number
	}

	entry := &cacheEntry{
		attr:     rootAttr,
		modTime:  time.Now(),
		expireAt: time.Now().Add(fs.config.MaxCacheAge),
	}

	fs.cache.set("/", entry)
	return root, nil
}

// Dir represents a directory in the filesystem
type Dir struct {
	fs.Inode
	fs   *FileSystem
	path string
	mu   sync.RWMutex
}

var _ = (fs.NodeLookuper)((*Dir)(nil))

// Lookup implements fs.NodeLookuper
func (d *Dir) Lookup(ctx context.Context, name string, out *fuse.EntryOut) (*fs.Inode, syscall.Errno) {
	d.mu.RLock()
	defer d.mu.RUnlock()

	path := filepath.Join(d.path, name)
	entry, err := d.fs.cache.get(path)
	if err != nil {
		// Try to fetch from server
		resp, err := d.fs.client.Head(d.fs.config.BaseURL + path)
		if err != nil {
			return nil, syscall.ENOENT
		}
		defer resp.Body.Close()

		if resp.StatusCode != http.StatusOK {
			return nil, syscall.ENOENT
		}

		// Create cache entry
		modTime := time.Now()
		if lastMod := resp.Header.Get("Last-Modified"); lastMod != "" {
			if t, err := time.Parse(time.RFC1123, lastMod); err == nil {
				modTime = t
			}
		}

		inode := generateInode(path)
		entry = &cacheEntry{
			attr: &fuse.Attr{
				Mode:  fuse.S_IFREG | 0444,
				Size:  uint64(resp.ContentLength),
				Mtime: uint64(modTime.Unix()),
				Ino:   inode,
			},
			modTime:  modTime,
			expireAt: time.Now().Add(d.fs.config.MaxCacheAge),
		}
		d.fs.cache.set(path, entry)
	}

	// Copy attributes to the output
	out.Attr = *entry.attr

	child := &File{
		fs:   d.fs,
		path: path,
	}

	return d.NewInode(ctx, child, fs.StableAttr{
		Mode: entry.attr.Mode,
		Ino:  entry.attr.Ino,
	}), 0
}

// File represents a file in the filesystem
type File struct {
	fs.Inode
	fs   *FileSystem
	path string
	mu   sync.RWMutex
}

// Read implements the file read operation
func (f *File) Read(ctx context.Context, fh fs.FileHandle, dest []byte, off int64) (fuse.ReadResult, error) {
	f.mu.RLock()
	defer f.mu.RUnlock()

	entry, err := f.fs.cache.get(f.path)
	if err != nil || entry.content == nil {
		// Fetch file content
		resp, err := f.fs.client.Get(f.fs.config.BaseURL + f.path)
		if err != nil {
			return nil, syscall.EIO
		}
		defer resp.Body.Close()

		if resp.StatusCode != http.StatusOK {
			return nil, syscall.EIO
		}

		content, err := io.ReadAll(resp.Body)
		if err != nil {
			return nil, syscall.EIO
		}

		if entry == nil {
			// Create new entry if it doesn't exist
			entry = &cacheEntry{
				attr: &fuse.Attr{
					Mode:  fuse.S_IFREG | 0444,
					Size:  uint64(len(content)),
					Mtime: uint64(time.Now().Unix()),
					Ino:   generateInode(f.path),
				},
				modTime:  time.Now(),
				expireAt: time.Now().Add(f.fs.config.MaxCacheAge),
			}
		}
		entry.content = content
		f.fs.cache.set(f.path, entry)
	}

	if off >= int64(len(entry.content)) {
		return nil, io.EOF
	}

	end := int(off) + len(dest)
	if end > len(entry.content) {
		end = len(entry.content)
	}

	copied := copy(dest, entry.content[off:end])
	return fuse.ReadResultData(dest[:copied]), nil
}

// GetAttr implements the get attributes operation
func (f *File) GetAttr(ctx context.Context, fh fs.FileHandle, out *fuse.AttrOut) syscall.Errno {
	f.mu.RLock()
	defer f.mu.RUnlock()

	entry, err := f.fs.cache.get(f.path)
	if err != nil {
		return syscall.ENOENT
	}
	out.Attr = *entry.attr
	return 0
}

// Mount mounts the filesystem at the specified path
func Mount(mountpoint string, config Config) (*fuse.Server, error) {
	if err := os.MkdirAll(mountpoint, 0755); err != nil {
		return nil, err
	}

	filesystem, err := NewFileSystem(config)
	if err != nil {
		return nil, err
	}

	root, err := filesystem.Root()
	if err != nil {
		return nil, err
	}

	server, err := fs.Mount(mountpoint, root, &fs.Options{
		MountOptions: fuse.MountOptions{
			Debug:      true,
			FsName:     "httpfs",
			Name:       "httpfs",
			AllowOther: true,
		},
		FirstAutomaticIno: 2, // Start with inode 2 (1 is reserved for root)
	})
	if err != nil {
		return nil, err
	}

	return server, nil
}

// cache methods
func (c *cache) get(path string) (*cacheEntry, error) {
	c.RLock()
	defer c.RUnlock()

	entry, ok := c.entries[path]
	if !ok {
		return nil, fmt.Errorf("not found")
	}

	if time.Now().After(entry.expireAt) {
		delete(c.entries, path)
		return nil, fmt.Errorf("expired")
	}

	return entry, nil
}

func (c *cache) set(path string, entry *cacheEntry) {
	c.Lock()
	defer c.Unlock()
	c.entries[path] = entry
}
