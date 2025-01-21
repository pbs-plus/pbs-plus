package httpfs

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"syscall"
	"testing"
	"time"

	"github.com/hanwen/go-fuse/v2/fs"
	"github.com/hanwen/go-fuse/v2/fuse"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func setupTestFS(t *testing.T) (*FileSystem, *httptest.Server, func()) {
	// Create a test HTTP server
	testContent := "Hello, World!"
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/test.txt":
			w.Header().Set("Content-Type", "text/plain")
			w.Header().Set("Content-Length", "13")
			w.Header().Set("Last-Modified", time.Now().Format(time.RFC1123))
			w.WriteHeader(http.StatusOK)
			if r.Method != "HEAD" {
				w.Write([]byte(testContent))
			}
		default:
			w.WriteHeader(http.StatusNotFound)
		}
	}))

	config := Config{
		BaseURL:     server.URL,
		UseHTTP3:    false,
		Timeout:     30 * time.Second,
		MaxCacheAge: 5 * time.Minute,
	}

	filesystem, err := NewFileSystem(config)
	require.NoError(t, err)

	cleanup := func() {
		server.Close()
	}

	return filesystem, server, cleanup
}

func TestHTTPFileSystem(t *testing.T) {
	filesystem, _, cleanup := setupTestFS(t)
	defer cleanup()

	// Create temporary mount point
	tempDir, err := os.MkdirTemp("", "httpfs-test")
	require.NoError(t, err)
	defer os.RemoveAll(tempDir)

	// Get root
	rootData, err := filesystem.Root()
	require.NoError(t, err)
	root := rootData.(*Dir)

	// Mount the filesystem with proper options
	server, err := fs.Mount(tempDir, root, &fs.Options{
		MountOptions: fuse.MountOptions{
			Debug:      true,
			FsName:     "httpfs-test",
			AllowOther: false,
		},
		FirstAutomaticIno: 2, // Start with inode 2 (1 is reserved for root)
	})
	require.NoError(t, err)

	// Ensure cleanup
	defer func() {
		err := server.Unmount()
		assert.NoError(t, err)
	}()

	// Wait for mount to be ready
	time.Sleep(100 * time.Millisecond)

	// Run tests
	ctx := context.Background()

	// Test Lookup
	var entryOut fuse.EntryOut
	child, errno := root.Lookup(ctx, "test.txt", &entryOut)
	require.Equal(t, syscall.Errno(0), errno)
	require.NotNil(t, child)

	// Test file attributes
	var attrOut fuse.AttrOut
	file := child.Operations().(*File)
	errno = file.GetAttr(ctx, nil, &attrOut)
	assert.Equal(t, syscall.Errno(0), errno)
	assert.Equal(t, uint64(13), attrOut.Attr.Size) // Size of "Hello, World!"

	// Test Read
	buf := make([]byte, 13)
	readResult, err := file.Read(ctx, nil, buf, 0)
	require.NoError(t, err)

	result, status := readResult.Bytes(make([]byte, len(buf)))
	require.Equal(t, fuse.OK, status)
	assert.Equal(t, "Hello, World!", string(result))

	// Test non-existent file
	_, errno = root.Lookup(ctx, "nonexistent.txt", &entryOut)
	assert.Equal(t, syscall.ENOENT, errno)
}

func TestHTTPFileSystemCache(t *testing.T) {
	requestCount := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requestCount++
		w.Header().Set("Content-Type", "text/plain")
		w.Header().Set("Content-Length", "5")
		w.Header().Set("Last-Modified", time.Now().Format(time.RFC1123))
		w.WriteHeader(http.StatusOK)
		if r.Method != "HEAD" {
			w.Write([]byte("test!"))
		}
	}))
	defer server.Close()

	config := Config{
		BaseURL:     server.URL,
		UseHTTP3:    false,
		Timeout:     30 * time.Second,
		MaxCacheAge: 100 * time.Millisecond, // Short cache time for testing
	}

	fs, err := NewFileSystem(config)
	require.NoError(t, err)

	// First request - should miss cache
	entry, err := fs.cache.get("/test.txt")
	assert.Error(t, err)
	assert.Nil(t, entry)

	// Create test file entry
	resp, err := fs.client.Head(config.BaseURL + "/test.txt")
	require.NoError(t, err)
	defer resp.Body.Close()

	// Populate cache
	fs.cache.set("/test.txt", &cacheEntry{
		attr: &fuse.Attr{
			Mode:  fuse.S_IFREG | 0444,
			Size:  5,
			Mtime: uint64(time.Now().Unix()),
			Ino:   generateInode("/test.txt"),
		},
		modTime:  time.Now(),
		expireAt: time.Now().Add(config.MaxCacheAge),
	})

	// Second request - should hit cache
	entry, err = fs.cache.get("/test.txt")
	assert.NoError(t, err)
	assert.NotNil(t, entry)
	assert.Equal(t, uint64(5), entry.attr.Size)

	// Wait for cache to expire
	time.Sleep(200 * time.Millisecond)

	// Third request - should miss cache due to expiration
	entry, err = fs.cache.get("/test.txt")
	assert.Error(t, err)
	assert.Nil(t, entry)

	// Verify request count
	assert.Equal(t, 1, requestCount, "Expected exactly one HTTP request")
}

func TestMountAndUnmount(t *testing.T) {
	// Create temporary mount point
	tempDir, err := os.MkdirTemp("", "httpfs-mount-test")
	require.NoError(t, err)
	defer os.RemoveAll(tempDir)

	// Setup test server
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	config := Config{
		BaseURL:     server.URL,
		UseHTTP3:    false,
		Timeout:     30 * time.Second,
		MaxCacheAge: 5 * time.Minute,
	}

	// Test mounting
	fuseServer, err := Mount(tempDir, config)
	require.NoError(t, err)
	require.NotNil(t, fuseServer)

	// Wait for mount to be ready
	time.Sleep(100 * time.Millisecond)

	// Verify mount point exists and is accessible
	_, err = os.Stat(tempDir)
	assert.NoError(t, err)

	// Test unmounting
	err = fuseServer.Unmount()
	assert.NoError(t, err)
}

func TestFileOperations(t *testing.T) {
	filesystem, _, cleanup := setupTestFS(t)
	defer cleanup()

	// Create temporary mount point
	tempDir, err := os.MkdirTemp("", "httpfs-test")
	require.NoError(t, err)
	defer os.RemoveAll(tempDir)

	// Get root
	rootData, err := filesystem.Root()
	require.NoError(t, err)
	root := rootData.(*Dir)

	// Mount the filesystem with proper options
	server, err := fs.Mount(tempDir, root, &fs.Options{
		MountOptions: fuse.MountOptions{
			Debug:      true,
			FsName:     "httpfs-test",
			AllowOther: false,
		},
		FirstAutomaticIno: 2, // Start with inode 2 (1 is reserved for root)
	})
	require.NoError(t, err)
	defer server.Unmount()

	// Wait for mount to be ready
	time.Sleep(100 * time.Millisecond)

	// Run tests
	ctx := context.Background()

	// Test file lookup
	var entryOut fuse.EntryOut
	child, errno := root.Lookup(ctx, "test.txt", &entryOut)
	require.Equal(t, syscall.Errno(0), errno)
	require.NotNil(t, child)

	file := child.Operations().(*File)

	// Test partial read
	buf := make([]byte, 5) // Read only first 5 bytes
	readResult, err := file.Read(ctx, nil, buf, 0)
	require.NoError(t, err)
	result, status := readResult.Bytes(make([]byte, len(buf)))
	require.Equal(t, fuse.OK, status)
	assert.Equal(t, "Hello", string(result))

	// Test offset read
	buf = make([]byte, 6)
	readResult, err = file.Read(ctx, nil, buf, 7) // Start from offset 7
	require.NoError(t, err)
	result, status = readResult.Bytes(make([]byte, len(buf)))
	require.Equal(t, fuse.OK, status)
	assert.Equal(t, "World!", string(result))

	// Test read beyond file size
	readResult, err = file.Read(ctx, nil, buf, 20)
	assert.Equal(t, io.EOF, err)
}
