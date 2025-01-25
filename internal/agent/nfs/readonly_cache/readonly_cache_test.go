//go:build windows

package readonly_cache

import (
	"io/fs"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/go-git/go-billy/v5"
	"github.com/go-git/go-billy/v5/memfs"
	"github.com/go-git/go-billy/v5/osfs"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/willscott/go-nfs"
)

type mockHandler struct {
	nfs.Handler
	readDirContents  map[string][]fs.FileInfo
	readFileContents map[string][]byte
}

func newMockHandler() *mockHandler {
	return &mockHandler{
		readDirContents:  make(map[string][]fs.FileInfo),
		readFileContents: make(map[string][]byte),
	}
}

func (m *mockHandler) SetReadDir(path string, contents []fs.FileInfo) {
	m.readDirContents[path] = contents
}

func (m *mockHandler) SetReadFile(path string, contents []byte) {
	m.readFileContents[path] = contents
}

func (m *mockHandler) ReadDir(path string) ([]fs.FileInfo, error) {
	if contents, ok := m.readDirContents[path]; ok {
		return contents, nil
	}
	return nil, os.ErrNotExist
}

func (m *mockHandler) ReadFile(path string) ([]byte, error) {
	if contents, ok := m.readFileContents[path]; ok {
		return contents, nil
	}
	return nil, os.ErrNotExist
}

func setupTestEnvironment(t *testing.T) (*mockHandler, nfs.Handler) {
	mock := newMockHandler()

	files := []fakeFileInfo{
		{name: "main.go", size: 100},
		{name: "util.go", size: 200},
	}

	mock.SetReadDir("project/src", []fs.FileInfo{&files[0], &files[1]})
	mock.SetReadFile("project/src/main.go", []byte("package main"))

	return mock, NewReadOnlyHandler(mock)
}

func setupTestFS(t *testing.T) (string, billy.Filesystem) {
	// Create temp directory structure
	tempDir := t.TempDir()
	memFS := memfs.New()

	// Create realistic directory structure
	dirs := []string{
		"project/src",
		"project/docs",
		"project/tests",
		"project/.git",
		"project/node_modules",
	}

	files := map[string]string{
		"project/src/main.go":           "package main\n\nfunc main() {}\n",
		"project/docs/README.md":        "# Project\nDescription\n",
		"project/tests/main_test.go":    "package main_test\n",
		"project/.gitignore":            "node_modules/\n",
		"project/package.json":          "{\n  \"name\": \"project\"\n}",
		"project/node_modules/big.json": string(make([]byte, 1024*1024)), // 1MB file
	}

	// Create directories and files in both filesystems
	for _, dir := range dirs {
		require.NoError(t, os.MkdirAll(filepath.Join(tempDir, dir), 0755))
		require.NoError(t, memFS.MkdirAll(dir, 0755))
	}

	for path, content := range files {
		err := os.WriteFile(filepath.Join(tempDir, path), []byte(content), 0644)
		require.NoError(t, err)

		f, err := memFS.Create(path)
		require.NoError(t, err)
		_, err = f.Write([]byte(content))
		require.NoError(t, err)
		require.NoError(t, f.Close())
	}

	return tempDir, memFS
}

func TestReadOnlyHandler_EndToEnd(t *testing.T) {
	tempDir, memFS := setupTestFS(t)
	osFS := osfs.New(tempDir)

	handler := NewReadOnlyHandler(&mockHandler{})

	t.Run("Handle Generation and Recovery", func(t *testing.T) {
		testPaths := [][]string{
			{"project", "src", "main.go"},
			{"project", "docs", "README.md"},
			{"project", "tests", "main_test.go"},
		}

		for _, path := range testPaths {
			handle := handler.(*ReadOnlyHandler).ToHandle(osFS, path)
			require.NotNil(t, handle)

			recoveredFS, recoveredPath, err := handler.(*ReadOnlyHandler).FromHandle(handle)
			require.NoError(t, err)
			assert.Equal(t, path, recoveredPath)
			assert.NotNil(t, recoveredFS)
		}
	})

	t.Run("Invalid Path Detection", func(t *testing.T) {
		invalidPaths := [][]string{
			{"project", "..", "secret"},
			{"project", "src", ""},
			{"project", "/etc/passwd"},
		}

		for _, path := range invalidPaths {
			handle := handler.(*ReadOnlyHandler).ToHandle(osFS, path)
			assert.Nil(t, handle)
		}
	})

	t.Run("Verifier Generation and Expiration", func(t *testing.T) {
		path := "project/src"
		contents, err := osFS.ReadDir(path)
		require.NoError(t, err)

		v1 := handler.(*ReadOnlyHandler).VerifierFor(path, contents)
		assert.NotZero(t, v1)

		// Verify contents match
		retrievedContents := handler.(*ReadOnlyHandler).DataForVerifier(v1)
		assert.Equal(t, len(contents), len(retrievedContents))

		// Test expiration
		h := handler.(*ReadOnlyHandler)
		if v, ok := h.activeVerifiers.Load(v1); ok {
			ver := v.(*verifier)
			ver.created = time.Now().Add(-2 * verifierExpiration)
		}

		// Should be expired now
		assert.Nil(t, handler.(*ReadOnlyHandler).DataForVerifier(v1))
	})

	t.Run("Concurrent Access", func(t *testing.T) {
		done := make(chan bool)
		for i := 0; i < 10; i++ {
			go func() {
				path := []string{"project", "src", "main.go"}
				handle := handler.(*ReadOnlyHandler).ToHandle(memFS, path)
				_, _, err := handler.(*ReadOnlyHandler).FromHandle(handle)
				assert.NoError(t, err)
				done <- true
			}()
		}

		for i := 0; i < 10; i++ {
			<-done
		}
	})

	t.Run("Hash Consistency", func(t *testing.T) {
		path := "project/src"
		contents, err := osFS.ReadDir(path)
		require.NoError(t, err)

		// Same content should produce same hash
		h1 := hashPathAndContents(path, contents)
		h2 := hashPathAndContents(path, contents)
		assert.Equal(t, h1, h2)

		// Different content should produce different hash
		h3 := hashPathAndContents(path+"different", contents)
		assert.NotEqual(t, h1, h3)
	})

	t.Run("Memory Usage", func(t *testing.T) {
		var handles [][]byte
		var verifiers []uint64

		// Generate lots of handles and verifiers
		for i := 0; i < 1000; i++ {
			path := []string{"project", "node_modules", "big.json"}
			handle := handler.(*ReadOnlyHandler).ToHandle(memFS, path)
			handles = append(handles, handle)

			contents, err := memFS.ReadDir("project/node_modules")
			require.NoError(t, err)
			verifier := handler.(*ReadOnlyHandler).VerifierFor("project/node_modules", contents)
			verifiers = append(verifiers, verifier)
		}

		// Cleanup should work
		h := handler.(*ReadOnlyHandler)
		h.Cleanup()

		// Verify cleanup worked
		assert.Zero(t, countMapItems(h.fsMap))
		assert.Zero(t, countMapItems(h.idMap))
		assert.Zero(t, countMapItems(h.activeVerifiers))
	})
}

func countMapItems(m *sync.Map) int {
	count := 0
	m.Range(func(_, _ interface{}) bool {
		count++
		return true
	})
	return count
}

func TestHandlerPerformance(t *testing.T) {
	tempDir := t.TempDir()
	osFS := osfs.New(tempDir)
	handler := NewReadOnlyHandler(&mockHandler{})

	path := []string{"project", "src", "main.go"}
	contents := []fs.FileInfo{
		&fakeFileInfo{name: "main.go", size: 100},
		&fakeFileInfo{name: "util.go", size: 200},
	}

	t.Run("ToHandle Generation Time", func(t *testing.T) {
		start := time.Now()
		for i := 0; i < 10000; i++ {
			handle := handler.(*ReadOnlyHandler).ToHandle(osFS, path)
			require.NotNil(t, handle, "handle generation failed")
		}
		duration := time.Since(start)
		assert.Less(t, duration, 100*time.Millisecond, "handle generation too slow")
	})

	t.Run("FromHandle Parse Time", func(t *testing.T) {
		handle := handler.(*ReadOnlyHandler).ToHandle(osFS, path)
		start := time.Now()
		for i := 0; i < 10000; i++ {
			_, _, err := handler.(*ReadOnlyHandler).FromHandle(handle)
			require.NoError(t, err)
		}
		duration := time.Since(start)
		assert.Less(t, duration, 100*time.Millisecond, "handle parsing too slow")
	})

	t.Run("Verifier Generation Time", func(t *testing.T) {
		start := time.Now()
		for i := 0; i < 10000; i++ {
			v := handler.(*ReadOnlyHandler).VerifierFor("test/path", contents)
			require.NotZero(t, v, "verifier generation failed")
		}
		duration := time.Since(start)
		assert.Less(t, duration, 150*time.Millisecond, "verifier generation too slow")
	})
}

// Helper struct for benchmarking
type fakeFileInfo struct {
	name string
	size int64
}

func (f *fakeFileInfo) Name() string       { return f.name }
func (f *fakeFileInfo) Size() int64        { return f.size }
func (f *fakeFileInfo) Mode() os.FileMode  { return 0644 }
func (f *fakeFileInfo) ModTime() time.Time { return time.Now() }
func (f *fakeFileInfo) IsDir() bool        { return false }
func (f *fakeFileInfo) Sys() interface{}   { return nil }
