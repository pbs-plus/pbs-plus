//go:build windows

package vssfs

import (
	"context"
	"net"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/sonroyaalmerol/pbs-plus/internal/arpc"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestVSSFSServer(t *testing.T) {
	// Setup test directory structure
	testDir, err := os.MkdirTemp("", "vssfs-test")
	require.NoError(t, err)
	defer os.RemoveAll(testDir)

	// Create test files
	testFile1Path := filepath.Join(testDir, "test1.txt")
	err = os.WriteFile(testFile1Path, []byte("test file 1 content"), 0644)
	require.NoError(t, err)

	testFile2Path := filepath.Join(testDir, "test2.txt")
	err = os.WriteFile(testFile2Path, []byte("test file 2 content with more data"), 0644)
	require.NoError(t, err)

	// Create subdirectory with files
	subDir := filepath.Join(testDir, "subdir")
	err = os.Mkdir(subDir, 0755)
	require.NoError(t, err)

	subFilePath := filepath.Join(subDir, "subfile.txt")
	err = os.WriteFile(subFilePath, []byte("content in subdirectory"), 0644)
	require.NoError(t, err)

	// Setup arpc server and client using in-memory connection
	serverConn, clientConn := net.Pipe()

	// Context for the test with timeout
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	// Start the server
	serverRouter := arpc.NewRouter()
	vssServer := NewVSSFSServer("vss", testDir)
	vssServer.RegisterHandlers(serverRouter)

	serverSession, err := arpc.NewServerSession(serverConn, nil)
	require.NoError(t, err)

	// Start server in a goroutine
	go func() {
		err := serverSession.Serve(serverRouter)
		if err != nil && ctx.Err() == nil {
			t.Errorf("Server error: %v", err)
		}
	}()
	defer serverSession.Close()

	// Setup client
	clientSession, err := arpc.NewClientSession(clientConn, nil)
	require.NoError(t, err)
	defer clientSession.Close()

	// Run tests
	t.Run("FSstat", func(t *testing.T) {
		var result struct {
			TotalSize int64 `json:"totalSize"`
		}
		err = clientSession.CallJSON(ctx, "vss/FSstat", nil, &result)
		assert.NoError(t, err)
		assert.True(t, result.TotalSize > 0, "TotalSize should be greater than 0")
	})

	t.Run("Stat", func(t *testing.T) {
		payload := map[string]string{"path": "test1.txt"}
		var result map[string]interface{}
		err = clientSession.CallJSON(ctx, "vss/Stat", payload, &result)
		assert.NoError(t, err)
		assert.NotNil(t, result["size"])
		assert.Equal(t, float64(19), result["size"]) // "test file 1 content" is 19 bytes
	})

	t.Run("ReadDir", func(t *testing.T) {
		payload := map[string]string{"path": "/"}
		var result struct {
			Entries []map[string]interface{} `json:"entries"`
		}
		err = clientSession.CallJSON(ctx, "vss/ReadDir", payload, &result)
		assert.NoError(t, err)
		assert.GreaterOrEqual(t, len(result.Entries), 3) // Should have at least test1.txt, test2.txt, and subdir

		// Verify we can find our test files
		foundTest1 := false
		foundSubdir := false
		for _, entry := range result.Entries {
			name, ok := entry["name"].(string)
			if ok {
				if name == "test1.txt" {
					foundTest1 = true
				} else if name == "subdir" {
					foundSubdir = true
					assert.True(t, entry["isDir"].(bool), "subdir should be identified as a directory")
				}
			}
		}
		assert.True(t, foundTest1, "test1.txt should be found in directory listing")
		assert.True(t, foundSubdir, "subdir should be found in directory listing")
	})

	t.Run("OpenFile_Read_Close", func(t *testing.T) {
		// Open file
		payload := map[string]interface{}{
			"path": "test1.txt",
			"flag": 0, // O_RDONLY
			"perm": 0644,
		}
		var openResult struct {
			HandleID uint64 `json:"handleID"`
		}
		err = clientSession.CallJSON(ctx, "vss/OpenFile", payload, &openResult)
		assert.NoError(t, err)
		assert.NotZero(t, openResult.HandleID)

		// Read file
		readPayload := map[string]interface{}{
			"handleID": openResult.HandleID,
			"length":   100, // More than enough for our test
		}
		var readResult struct {
			Data []byte `json:"data"`
			EOF  bool   `json:"eof"`
		}
		err = clientSession.CallJSON(ctx, "vss/Read", readPayload, &readResult)
		assert.NoError(t, err)
		assert.Equal(t, "test file 1 content", string(readResult.Data))
		assert.True(t, readResult.EOF)

		// Close file
		closePayload := map[string]interface{}{
			"handleID": openResult.HandleID,
		}
		resp, err := clientSession.Call("vss/Close", closePayload)
		assert.NoError(t, err)
		assert.Equal(t, 200, resp.Status)

		// Verify handle is invalid after closing
		_, err = clientSession.Call("vss/Read", readPayload)
		assert.Error(t, err) // Should fail because handle is closed
	})

	t.Run("OpenFile_ReadAt_Close", func(t *testing.T) {
		// Open file
		payload := map[string]interface{}{
			"path": "test2.txt",
			"flag": 0, // O_RDONLY
			"perm": 0644,
		}
		var openResult struct {
			HandleID uint64 `json:"handleID"`
		}
		err = clientSession.CallJSON(ctx, "vss/OpenFile", payload, &openResult)
		assert.NoError(t, err)

		// Read at offset
		readAtPayload := map[string]interface{}{
			"handleID": openResult.HandleID,
			"offset":   10, // Skip "test file " (10 chars)
			"length":   100,
		}
		var readResult struct {
			Data []byte `json:"data"`
			EOF  bool   `json:"eof"`
		}
		err = clientSession.CallJSON(ctx, "vss/ReadAt", readAtPayload, &readResult)
		assert.NoError(t, err)
		assert.Equal(t, "2 content with more data", string(readResult.Data))

		// Close file
		closePayload := map[string]interface{}{
			"handleID": openResult.HandleID,
		}
		resp, err := clientSession.Call("vss/Close", closePayload)
		assert.NoError(t, err)
		assert.Equal(t, 200, resp.Status)
	})

	t.Run("Fstat", func(t *testing.T) {
		// Open file
		payload := map[string]interface{}{
			"path": "test1.txt",
			"flag": 0, // O_RDONLY
			"perm": 0644,
		}
		var openResult struct {
			HandleID uint64 `json:"handleID"`
		}
		err = clientSession.CallJSON(ctx, "vss/OpenFile", payload, &openResult)
		assert.NoError(t, err)

		// Get file info
		fstatPayload := map[string]interface{}{
			"handleID": openResult.HandleID,
		}
		var statResult map[string]interface{}
		err = clientSession.CallJSON(ctx, "vss/Fstat", fstatPayload, &statResult)
		assert.NoError(t, err)
		assert.Equal(t, float64(19), statResult["size"]) // "test file 1 content" is 19 bytes

		// Close file
		closePayload := map[string]interface{}{
			"handleID": openResult.HandleID,
		}
		resp, err := clientSession.Call("vss/Close", closePayload)
		assert.NoError(t, err)
		assert.Equal(t, 200, resp.Status)
	})

	t.Run("OpenDirectory", func(t *testing.T) {
		// Open directory
		payload := map[string]interface{}{
			"path": "subdir",
			"flag": 0, // O_RDONLY
			"perm": 0644,
		}
		var openResult struct {
			HandleID uint64 `json:"handleID"`
		}
		err = clientSession.CallJSON(ctx, "vss/OpenFile", payload, &openResult)
		assert.NoError(t, err)

		// Try to read from directory (should fail)
		readPayload := map[string]interface{}{
			"handleID": openResult.HandleID,
			"length":   100,
		}
		resp, err := clientSession.Call("vss/Read", readPayload)
		assert.NoError(t, err)
		assert.Equal(t, 400, resp.Status) // Bad request, can't read from directory

		// Close handle
		closePayload := map[string]interface{}{
			"handleID": openResult.HandleID,
		}
		resp, err = clientSession.Call("vss/Close", closePayload)
		assert.NoError(t, err)
		assert.Equal(t, 200, resp.Status)
	})

	t.Run("WriteOperationsNotAllowed", func(t *testing.T) {
		// Try to open for writing (should fail)
		payload := map[string]interface{}{
			"path": "test1.txt",
			"flag": 1, // O_WRONLY
			"perm": 0644,
		}
		resp, err := clientSession.Call("vss/OpenFile", payload)
		assert.NoError(t, err)
		assert.Equal(t, 403, resp.Status) // Forbidden, write not allowed
	})

	t.Run("InvalidPath", func(t *testing.T) {
		// Try to access non-existent file
		payload := map[string]string{"path": "nonexistent.txt"}
		resp, err := clientSession.Call("vss/Stat", payload)
		assert.NoError(t, err)
		assert.NotEqual(t, 200, resp.Status) // Should not be OK
	})
}
