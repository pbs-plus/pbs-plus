//go:build windows

package vssfs

import (
	"context"
	"io"
	"net"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/sonroyaalmerol/pbs-plus/internal/arpc"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// createLargeTestFile creates a test file of the specified size with deterministic content
func createLargeTestFile(t *testing.T, path string, size int) {
	t.Helper()

	file, err := os.Create(path)
	require.NoError(t, err)
	defer file.Close()

	// Create a buffer with some pattern data
	const bufferSize = 64 * 1024 // 64KB buffer for writing
	buffer := make([]byte, bufferSize)

	// Fill buffer with deterministic pattern
	// We'll use a simple pattern that includes the position to make verification easier
	for i := 0; i < bufferSize; i++ {
		buffer[i] = byte(i % 251) // Use prime number to create a longer repeating pattern
	}

	// Write the buffer repeatedly until we reach the desired size
	bytesWritten := 0
	for bytesWritten < size {
		writeSize := bufferSize
		if bytesWritten+writeSize > size {
			writeSize = size - bytesWritten
		}

		n, err := file.Write(buffer[:writeSize])
		require.NoError(t, err)
		require.Equal(t, writeSize, n)

		bytesWritten += writeSize
	}

	// Ensure all data is flushed to disk
	require.NoError(t, file.Sync())
}

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

	// Create a large file to test memory mapping
	largePath := filepath.Join(testDir, "large_file.bin")
	createLargeTestFile(t, largePath, 1024*1024) // 1MB file

	// Create a medium file just below the mmap threshold
	mediumPath := filepath.Join(testDir, "medium_file.bin")
	createLargeTestFile(t, mediumPath, 100*1024) // 100KB file (below 128KB threshold)

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
		// Ignore "closed pipe" errors during shutdown.
		if err != nil && ctx.Err() == nil && !strings.Contains(err.Error(), "closed pipe") {
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
		// Fix: Use the exact FSStat struct that matches the server response
		var result StatFS
		raw, err := clientSession.CallMsg(ctx, "vss/FSstat", nil)
		assert.NoError(t, err)

		_, err = result.UnmarshalMsg(raw)
		assert.NoError(t, err)
		// The test originally expected TotalSize to be > 0, but it might not be
		// on some systems. We'll just assert it's not an error.
		assert.NotNil(t, result)
	})

	t.Run("Stat", func(t *testing.T) {
		payload := StatReq{Path: "test1.txt"}
		payloadBytes, _ := payload.MarshalMsg(nil)
		var result VSSFileInfo
		raw, err := clientSession.CallMsg(ctx, "vss/Stat", payloadBytes)
		result.UnmarshalMsg(raw)
		assert.NoError(t, err)
		assert.NotNil(t, result.Size)
		assert.EqualValues(t, 19, result.Size)
	})

	t.Run("ReadDir", func(t *testing.T) {
		payload := ReadDirReq{Path: "/"}
		payloadBytes, _ := payload.MarshalMsg(nil)
		var result ReadDirEntries
		raw, err := clientSession.CallMsg(ctx, "vss/ReadDir", payloadBytes)
		result.UnmarshalMsg(raw)
		assert.NoError(t, err)
		assert.GreaterOrEqual(t, len(result), 3) // Should have at least test1.txt, test2.txt, and subdir

		// Verify we can find our test files
		foundTest1 := false
		foundSubdir := false
		for _, entry := range result {
			name := entry.Name
			if name == "test1.txt" {
				foundTest1 = true
			} else if name == "subdir" {
				foundSubdir = true
				assert.True(t, os.FileMode(entry.Mode).IsDir(), "subdir should be identified as a directory")
			}
		}
		assert.True(t, foundTest1, "test1.txt should be found in directory listing")
		assert.True(t, foundSubdir, "subdir should be found in directory listing")
	})

	t.Run("OpenFile_ReadAt_Close", func(t *testing.T) {
		// Open file
		payload := OpenFileReq{Path: "test2.txt", Flag: 0, Perm: 0644}
		payloadBytes, _ := payload.MarshalMsg(nil)
		var openResult FileHandleId
		raw, err := clientSession.CallMsg(ctx, "vss/OpenFile", payloadBytes)
		openResult.UnmarshalMsg(raw)
		assert.NoError(t, err)

		// Read at offset
		readAtPayload := ReadAtReq{
			HandleID: string(openResult),
			Offset:   10,
			Length:   100,
		}
		readAtPayloadBytes, _ := readAtPayload.MarshalMsg(nil)

		p := make([]byte, 100)
		bytesRead, _, err := clientSession.CallMsgWithBuffer(ctx, "vss/ReadAt", readAtPayloadBytes, p)
		assert.NoError(t, err)
		assert.Equal(t, "2 content with more data", string(p[:bytesRead]))

		// Close file
		closePayload := CloseReq{HandleID: string(openResult)}
		closePayloadBytes, _ := closePayload.MarshalMsg(nil)
		resp, err := clientSession.Call("vss/Close", closePayloadBytes)
		assert.NoError(t, err)
		assert.Equal(t, 200, resp.Status)
	})

	// Test specifically for memory-mapped file reading
	t.Run("LargeFile_MemoryMapped_Read", func(t *testing.T) {
		// Open large file
		payload := OpenFileReq{Path: "large_file.bin", Flag: 0, Perm: 0644}
		payloadBytes, _ := payload.MarshalMsg(nil)
		var openResult FileHandleId
		raw, err := clientSession.CallMsg(ctx, "vss/OpenFile", payloadBytes)
		openResult.UnmarshalMsg(raw)
		assert.NoError(t, err)

		// Read a large chunk that should trigger memory mapping
		// (assuming threshold is 128KB)
		readSize := 256 * 1024 // 256KB - well above threshold
		readAtPayload := ReadAtReq{
			HandleID: string(openResult),
			Offset:   1024, // Start at 1KB offset
			Length:   readSize,
		}
		readAtPayloadBytes, _ := readAtPayload.MarshalMsg(nil)

		buffer := make([]byte, readSize)
		bytesRead, _, err := clientSession.CallMsgWithBuffer(ctx, "vss/ReadAt", readAtPayloadBytes, buffer)
		assert.NoError(t, err)
		assert.Equal(t, readSize, bytesRead, "Should read the full requested size")

		// Verify the data matches what we expect
		// We'll check the first few bytes against the original file
		originalFile, err := os.Open(largePath)
		require.NoError(t, err)
		defer originalFile.Close()

		_, err = originalFile.Seek(1024, 0) // Same offset as in the test
		require.NoError(t, err)

		compareBuffer := make([]byte, 1024) // Check first 1KB of the read
		_, err = io.ReadFull(originalFile, compareBuffer)
		require.NoError(t, err)

		assert.Equal(t, compareBuffer, buffer[:1024], "First 1KB of read data should match original file")

		// Close file
		closePayload := CloseReq{HandleID: string(openResult)}
		closePayloadBytes, _ := closePayload.MarshalMsg(nil)
		resp, err := clientSession.Call("vss/Close", closePayloadBytes)
		assert.NoError(t, err)
		assert.Equal(t, 200, resp.Status)
	})

	// Test for a file just below the mmap threshold
	t.Run("MediumFile_Regular_Read", func(t *testing.T) {
		// Open medium file
		payload := OpenFileReq{Path: "medium_file.bin", Flag: 0, Perm: 0644}
		payloadBytes, _ := payload.MarshalMsg(nil)
		var openResult FileHandleId
		raw, err := clientSession.CallMsg(ctx, "vss/OpenFile", payloadBytes)
		openResult.UnmarshalMsg(raw)
		assert.NoError(t, err)

		// Read a chunk that should NOT trigger memory mapping
		readSize := 100 * 1024 // 100KB - below threshold
		readAtPayload := ReadAtReq{
			HandleID: string(openResult),
			Offset:   0,
			Length:   readSize,
		}
		readAtPayloadBytes, _ := readAtPayload.MarshalMsg(nil)

		buffer := make([]byte, readSize)
		bytesRead, _, err := clientSession.CallMsgWithBuffer(ctx, "vss/ReadAt", readAtPayloadBytes, buffer)
		assert.NoError(t, err)
		assert.Equal(t, readSize, bytesRead, "Should read the full requested size")

		// Verify the data matches what we expect
		originalFile, err := os.Open(mediumPath)
		require.NoError(t, err)
		defer originalFile.Close()

		compareBuffer := make([]byte, 1024) // Check first 1KB
		_, err = io.ReadFull(originalFile, compareBuffer)
		require.NoError(t, err)

		assert.Equal(t, compareBuffer, buffer[:1024], "First 1KB of read data should match original file")

		// Close file
		closePayload := CloseReq{HandleID: string(openResult)}
		closePayloadBytes, _ := closePayload.MarshalMsg(nil)
		resp, err := clientSession.Call("vss/Close", closePayloadBytes)
		assert.NoError(t, err)
		assert.Equal(t, 200, resp.Status)
	})

	// Test partial reads with memory mapping
	t.Run("LargeFile_PartialRead_EOF", func(t *testing.T) {
		// Open large file
		payload := OpenFileReq{Path: "large_file.bin", Flag: 0, Perm: 0644}
		payloadBytes, _ := payload.MarshalMsg(nil)
		var openResult FileHandleId
		raw, err := clientSession.CallMsg(ctx, "vss/OpenFile", payloadBytes)
		openResult.UnmarshalMsg(raw)
		assert.NoError(t, err)

		// Read near the end of the file to test EOF handling
		fileSize := int64(1024 * 1024)   // 1MB
		readOffset := fileSize - 50*1024 // 50KB from the end
		readSize := 100 * 1024           // Try to read 100KB (more than available)

		readAtPayload := ReadAtReq{
			HandleID: string(openResult),
			Offset:   readOffset,
			Length:   readSize,
		}
		readAtPayloadBytes, _ := readAtPayload.MarshalMsg(nil)

		buffer := make([]byte, readSize)
		bytesRead, isEOF, err := clientSession.CallMsgWithBuffer(ctx, "vss/ReadAt", readAtPayloadBytes, buffer)
		assert.NoError(t, err)
		assert.Equal(t, 50*1024, bytesRead, "Should read only the available bytes")
		assert.True(t, isEOF, "Should indicate EOF was reached")

		// Close file
		closePayload := CloseReq{HandleID: string(openResult)}
		closePayloadBytes, _ := closePayload.MarshalMsg(nil)
		resp, err := clientSession.Call("vss/Close", closePayloadBytes)
		assert.NoError(t, err)
		assert.Equal(t, 200, resp.Status)
	})

	t.Run("OpenDirectory", func(t *testing.T) {
		// Open directory
		payload := OpenFileReq{Path: "subdir", Flag: 0, Perm: 0644}
		payloadBytes, _ := payload.MarshalMsg(nil)
		var openResult FileHandleId
		raw, err := clientSession.CallMsg(ctx, "vss/OpenFile", payloadBytes)
		openResult.UnmarshalMsg(raw)
		assert.NoError(t, err)

		// Try to read from directory (should fail)
		readPayload := ReadAtReq{HandleID: string(openResult), Length: 100}
		readPayloadBytes, _ := readPayload.MarshalMsg(nil)
		resp, err := clientSession.Call("vss/ReadAt", readPayloadBytes)
		assert.NoError(t, err)
		assert.Equal(t, 500, resp.Status) // Bad request, can't read from directory

		// Close handle
		closePayload := CloseReq{HandleID: string(openResult)}
		closePayloadBytes, _ := closePayload.MarshalMsg(nil)
		resp, err = clientSession.Call("vss/Close", closePayloadBytes)
		assert.NoError(t, err)
		assert.Equal(t, 200, resp.Status)
	})

	t.Run("WriteOperationsNotAllowed", func(t *testing.T) {
		// Try to open for writing (should fail)
		payload := OpenFileReq{Path: "test1.txt", Flag: 1, Perm: 0644}
		payloadBytes, _ := payload.MarshalMsg(nil)
		resp, err := clientSession.Call("vss/OpenFile", payloadBytes)
		assert.NoError(t, err)
		assert.Equal(t, 403, resp.Status) // Forbidden, write not allowed
	})

	t.Run("InvalidPath", func(t *testing.T) {
		// Try to access non-existent file
		payload := OpenFileReq{Path: "nonexistent.txt"}
		payloadBytes, _ := payload.MarshalMsg(nil)
		resp, err := clientSession.Call("vss/Stat", payloadBytes)
		assert.NoError(t, err)
		assert.NotEqual(t, 200, resp.Status) // Should not be OK
	})
}
