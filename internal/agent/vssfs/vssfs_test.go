//go:build windows

package vssfs

import (
	"context"
	"fmt"
	"io"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/sonroyaalmerol/pbs-plus/internal/agent/snapshots"
	"github.com/sonroyaalmerol/pbs-plus/internal/agent/vssfs/types"
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

// createSparseFileWithFsutil creates a sparse file using the fsutil command.
func createSparseFileWithFsutil(filePath string) error {
	file, err := os.Create(filePath)
	if err != nil {
		return fmt.Errorf("failed to create file: %w", err)
	}
	defer file.Close()

	cmd := exec.Command("fsutil", "sparse", "setflag", filePath)
	if err := cmd.Run(); err != nil {
		return fmt.Errorf("failed to mark file as sparse: %w", err)
	}

	// Write data region 1 (offset 0, length 65536 bytes)
	_, err = file.WriteAt([]byte("data1"), 0)
	if err != nil {
		return fmt.Errorf("failed to write data region 1: %w", err)
	}

	// Write data region 2 (offset 1048576, length 65536 bytes)
	_, err = file.WriteAt([]byte("data2"), 1048576)
	if err != nil {
		return fmt.Errorf("failed to write data region 2: %w", err)
	}

	// Write data region 3 (offset 3145728, length 5 bytes)
	_, err = file.WriteAt([]byte("data3"), 3145728)
	if err != nil {
		return fmt.Errorf("failed to write data region 3: %w", err)
	}

	return nil
}

// logHandleMap logs information about the handles in the VSSFSServer
func dumpHandleMap(server *VSSFSServer) string {
	if server == nil || server.handles == nil {
		return "Server or handles map is nil"
	}

	var info strings.Builder
	info.WriteString(fmt.Sprintf("Current handles map contains %d entries:\n", server.handles.Len()))

	server.handles.ForEach(func(key uint64, fh *FileHandle) bool {
		info.WriteString(fmt.Sprintf("  - Handle ID: %d, IsDir: %v\n", key, fh.isDir))
		return true
	})

	return info.String()
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
	vssServer := NewVSSFSServer("vss", snapshots.WinVSSSnapshot{SnapshotPath: testDir, DriveLetter: ""})
	vssServer.RegisterHandlers(&serverRouter)

	serverSession, err := arpc.NewServerSession(serverConn, nil)
	require.NoError(t, err)

	serverSession.SetRouter(serverRouter)

	// Start server in a goroutine
	go func() {
		err := serverSession.Serve()
		// Ignore "closed pipe" errors during shutdown.
		if err != nil && ctx.Err() == nil && err != io.EOF && !strings.Contains(err.Error(), "closed pipe") {
			t.Errorf("Server error: %v", err)
		}
	}()
	defer serverSession.Close()

	// Setup client
	clientSession, err := arpc.NewClientSession(clientConn, nil)
	require.NoError(t, err)
	defer clientSession.Close()

	t.Run("Stat", func(t *testing.T) {
		payload := types.StatReq{Path: ("test1.txt")}
		var result types.VSSFileInfo
		raw, err := clientSession.CallMsg(ctx, "vss/Stat", &payload)
		result.Decode(raw)
		assert.NoError(t, err)
		assert.NotNil(t, result.Size)
		assert.EqualValues(t, 19, result.Size)
	})

	t.Run("ReadDir", func(t *testing.T) {
		payload := types.ReadDirReq{Path: ("/")}
		var result types.ReadDirEntries
		raw, err := clientSession.CallMsg(ctx, "vss/ReadDir", &payload)
		result.Decode(raw)
		assert.NoError(t, err)
		assert.GreaterOrEqual(t, len(result), 3) // Should have at least test1.txt, test2.txt, and subdir

		// Verify we can find our test files
		foundTest1 := false
		foundSubdir := false
		for _, entry := range result {
			name := (entry.Name)
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
		// Log handles before open
		t.Log("Before OpenFile:", dumpHandleMap(vssServer))

		// Open file
		payload := types.OpenFileReq{Path: ("test2.txt"), Flag: 0, Perm: 0644}
		var openResult types.FileHandleId
		raw, err := clientSession.CallMsg(ctx, "vss/OpenFile", &payload)
		require.NoError(t, err, "OpenFile should succeed")
		openResult.Decode(raw)

		// Log handle ID and handles map after open
		t.Logf("After OpenFile - Handle ID received: %d", uint64(openResult))
		t.Log(dumpHandleMap(vssServer))

		// Verify the handle exists in the server's map
		exists := false
		vssServer.handles.ForEach(func(key uint64, fh *FileHandle) bool {
			if key == uint64(openResult) {
				exists = true
				return false // stop iteration
			}
			return true
		})
		require.True(t, exists, "Handle ID should exist in server's handles map")

		// Read at offset
		readAtPayload := types.ReadAtReq{
			HandleID: openResult,
			Offset:   10,
			Length:   100,
		}

		// Log before ReadAt
		t.Logf("Before ReadAt - Using Handle ID: %d", uint64(readAtPayload.HandleID))
		t.Log(dumpHandleMap(vssServer))

		p := make([]byte, 100)
		bytesRead, err := clientSession.CallBinary(ctx, "vss/ReadAt", &readAtPayload, p)
		if err != nil {
			t.Logf("ReadAt error: %v - Current handle map: %s", err, dumpHandleMap(vssServer))
			t.FailNow()
		}

		assert.Equal(t, "2 content with more data", string(p[:bytesRead]))

		// Log before Close
		t.Logf("Before Close - Using Handle ID: %d", uint64(openResult))
		t.Log(dumpHandleMap(vssServer))

		// Close file
		closePayload := types.CloseReq{HandleID: openResult}
		resp, err := clientSession.Call("vss/Close", &closePayload)
		if err != nil {
			t.Logf("Close error: %v - Current handle map: %s", err, dumpHandleMap(vssServer))
			t.FailNow()
		}
		assert.Equal(t, 200, resp.Status)

		// Log after Close
		t.Log("After Close:", dumpHandleMap(vssServer))
	})

	// New test to stress handle management with multiple files
	t.Run("MultipleFiles_HandleManagement", func(t *testing.T) {
		t.Log("Initial handle map:", dumpHandleMap(vssServer))

		// Store handles to verify them later
		handles := make([]types.FileHandleId, 0, 5)

		// Open multiple files
		files := []string{"test1.txt", "test2.txt", "large_file.bin", "medium_file.bin", "subdir/subfile.txt"}
		for i, fileName := range files {
			t.Logf("Opening file %d: %s", i, fileName)

			payload := types.OpenFileReq{Path: (fileName), Flag: 0, Perm: 0644}
			var openResult types.FileHandleId
			raw, err := clientSession.CallMsg(ctx, "vss/OpenFile", &payload)
			require.NoError(t, err, "OpenFile should succeed for %s", fileName)
			openResult.Decode(raw)

			t.Logf("Received handle ID: %d for file: %s", uint64(openResult), fileName)
			handles = append(handles, openResult)

			// Verify handle was added correctly
			t.Log(dumpHandleMap(vssServer))
		}

		// Verify all handles can be read from
		for i, handle := range handles {
			t.Logf("Reading from file %d with handle: %d", i, uint64(handle))

			readAtPayload := types.ReadAtReq{
				HandleID: handle,
				Offset:   0,
				Length:   10, // Just read a small amount
			}

			p := make([]byte, 10)
			_, err := clientSession.CallBinary(ctx, "vss/ReadAt", &readAtPayload, p)
			if err != nil {
				t.Logf("ReadAt error for handle %d: %v - Current handle map: %s",
					uint64(handle), err, dumpHandleMap(vssServer))
				t.FailNow()
			}
		}

		// Now close all handles
		for i, handle := range handles {
			t.Logf("Closing file %d with handle: %d", i, uint64(handle))

			closePayload := types.CloseReq{HandleID: handle}

			t.Log("Before Close:", dumpHandleMap(vssServer))
			resp, err := clientSession.Call("vss/Close", &closePayload)
			if err != nil {
				t.Logf("Close error for handle %d: %v - Current handle map: %s",
					uint64(handle), err, dumpHandleMap(vssServer))
				t.FailNow()
			}
			assert.Equal(t, 200, resp.Status)
			t.Log("After Close:", dumpHandleMap(vssServer))
		}
	})

	t.Run("LargeFile_Read", func(t *testing.T) {
		// Open large file
		payload := types.OpenFileReq{Path: ("large_file.bin"), Flag: 0, Perm: 0644}
		var openResult types.FileHandleId
		raw, err := clientSession.CallMsg(ctx, "vss/OpenFile", &payload)
		openResult.Decode(raw)
		assert.NoError(t, err)

		t.Logf("Large file open, handle ID: %d", uint64(openResult))
		t.Log(dumpHandleMap(vssServer))

		readSize := 256 * 1024 // 256KB - well above threshold
		readAtPayload := types.ReadAtReq{
			HandleID: openResult,
			Offset:   1024, // Start at 1KB offset
			Length:   readSize,
		}

		buffer := make([]byte, readSize)
		bytesRead, err := clientSession.CallBinary(ctx, "vss/ReadAt", &readAtPayload, buffer)
		if err != nil {
			t.Logf("Large file ReadAt error: %v - Current handle map: %s", err, dumpHandleMap(vssServer))
			t.FailNow()
		}
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
		closePayload := types.CloseReq{HandleID: openResult}
		resp, err := clientSession.Call("vss/Close", &closePayload)
		if err != nil {
			t.Logf("Large file Close error: %v - Current handle map: %s", err, dumpHandleMap(vssServer))
			t.FailNow()
		}
		assert.Equal(t, 200, resp.Status)
	})

	// Test for error conditions with invalid handles
	t.Run("InvalidHandle_Operations", func(t *testing.T) {
		// Try to read with a non-existent handle
		readAtPayload := types.ReadAtReq{
			HandleID: 33123,
			Offset:   0,
			Length:   100,
		}

		t.Log("Current handle map before invalid ReadAt:", dumpHandleMap(vssServer))
		resp, err := clientSession.Call("vss/ReadAt", &readAtPayload)
		assert.NoError(t, err, "Call should succeed but response should indicate error")
		assert.Equal(t, 500, resp.Status, "ReadAt with invalid handle should return 500 status")

		// Try to close a non-existent handle
		closePayload := types.CloseReq{HandleID: 33123}

		t.Log("Current handle map before invalid Close:", dumpHandleMap(vssServer))
		resp, err = clientSession.Call("vss/Close", &closePayload)
		assert.NoError(t, err, "Call should succeed but response should indicate error")
		assert.Equal(t, 500, resp.Status, "Close with invalid handle should return 500 status")
	})

	// Test for double close behavior
	t.Run("DoubleClose", func(t *testing.T) {
		// Open file
		payload := types.OpenFileReq{Path: ("test1.txt"), Flag: 0, Perm: 0644}
		var openResult types.FileHandleId
		raw, err := clientSession.CallMsg(ctx, "vss/OpenFile", &payload)
		require.NoError(t, err)
		openResult.Decode(raw)

		t.Logf("File opened with handle ID: %d", uint64(openResult))
		t.Log(dumpHandleMap(vssServer))

		// First close - should succeed
		closePayload := types.CloseReq{HandleID: openResult}
		resp, err := clientSession.Call("vss/Close", &closePayload)
		assert.NoError(t, err)
		assert.Equal(t, 200, resp.Status)

		t.Log("After first close:", dumpHandleMap(vssServer))

		// Second close - should fail
		resp, err = clientSession.Call("vss/Close", &closePayload)
		assert.NoError(t, err, "Call should succeed but response should indicate error")
		assert.NotEqual(t, 200, resp.Status, "Second close with same handle should return error status")

		t.Log("After second close:", dumpHandleMap(vssServer))
	})

	t.Run("Lseek", func(t *testing.T) {
		// Open a test file
		payload := types.OpenFileReq{Path: ("test2.txt"), Flag: 0, Perm: 0644}
		var openResult types.FileHandleId
		raw, err := clientSession.CallMsg(ctx, "vss/OpenFile", &payload)
		require.NoError(t, err, "OpenFile should succeed")
		openResult.Decode(raw)

		t.Logf("File opened with handle ID: %d", uint64(openResult))
		t.Log(dumpHandleMap(vssServer))

		// Test seeking to the beginning of the file
		t.Run("SeekStart", func(t *testing.T) {
			lseekPayload := types.LseekReq{
				HandleID: openResult,
				Offset:   0,
				Whence:   io.SeekStart,
			}

			raw, err := clientSession.CallMsg(ctx, "vss/Lseek", &lseekPayload)
			require.NoError(t, err, "Lseek should succeed")
			var lseekResp types.LseekResp
			lseekResp.Decode(raw)

			assert.Equal(t, int64(0), lseekResp.NewOffset, "Offset should be at the start of the file")
		})

		// Test seeking to the middle of the file
		t.Run("SeekMiddle", func(t *testing.T) {
			lseekPayload := types.LseekReq{
				HandleID: openResult,
				Offset:   10,
				Whence:   io.SeekStart,
			}

			raw, err := clientSession.CallMsg(ctx, "vss/Lseek", &lseekPayload)
			require.NoError(t, err, "Lseek should succeed")
			var lseekResp types.LseekResp
			lseekResp.Decode(raw)

			assert.Equal(t, int64(10), lseekResp.NewOffset, "Offset should be at position 10")
		})

		// Test seeking relative to the current position
		t.Run("SeekCurrent", func(t *testing.T) {
			lseekPayload := types.LseekReq{
				HandleID: openResult,
				Offset:   5,
				Whence:   io.SeekCurrent,
			}

			raw, err := clientSession.CallMsg(ctx, "vss/Lseek", &lseekPayload)
			require.NoError(t, err, "Lseek should succeed")
			var lseekResp types.LseekResp
			lseekResp.Decode(raw)

			assert.Equal(t, int64(15), lseekResp.NewOffset, "Offset should be at position 15")
		})

		// Test seeking to the end of the file
		t.Run("SeekEnd", func(t *testing.T) {
			lseekPayload := types.LseekReq{
				HandleID: openResult,
				Offset:   -5,
				Whence:   io.SeekEnd,
			}

			raw, err := clientSession.CallMsg(ctx, "vss/Lseek", &lseekPayload)
			require.NoError(t, err, "Lseek should succeed")
			var lseekResp types.LseekResp
			lseekResp.Decode(raw)

			t.Logf("File size: %d", 34)                      // Log the expected file size
			t.Logf("Expected offset: %d", 34-5)              // Log the expected offset
			t.Logf("Actual offset: %d", lseekResp.NewOffset) // Log the actual offset

			assert.Equal(t, int64(29), lseekResp.NewOffset, "Offset should be 5 bytes before the end of the file")
		})

		// Test seeking beyond the end of the file
		t.Run("SeekBeyondEOF", func(t *testing.T) {
			lseekPayload := types.LseekReq{
				HandleID: openResult,
				Offset:   100,
				Whence:   io.SeekStart,
			}

			_, err := clientSession.CallMsg(ctx, "vss/Lseek", &lseekPayload)
			require.Error(t, err, "Lseek should fail when seeking beyond EOF")
		})

		// Test seeking in a sparse file
		t.Run("SeekSparseFile", func(t *testing.T) {
			// Create a sparse file using fsutil
			sparseFilePath := filepath.Join(testDir, "sparse_file.bin")
			err := createSparseFileWithFsutil(sparseFilePath)
			require.NoError(t, err, "Failed to create sparse file with fsutil")

			// Open the sparse file
			payload := types.OpenFileReq{Path: ("sparse_file.bin"), Flag: 0, Perm: 0644}
			var openResult types.FileHandleId
			raw, err := clientSession.CallMsg(ctx, "vss/OpenFile", &payload)
			require.NoError(t, err, "OpenFile should succeed for sparse file")
			openResult.Decode(raw)

			t.Logf("Sparse file opened with handle ID: %d", uint64(openResult))
			t.Log(dumpHandleMap(vssServer))

			// Test SeekData
			t.Run("SeekData", func(t *testing.T) {
				// Seek to the first data region
				lseekPayload := types.LseekReq{
					HandleID: openResult,
					Offset:   0,
					Whence:   SeekData,
				}

				raw, err := clientSession.CallMsg(ctx, "vss/Lseek", &lseekPayload)
				require.NoError(t, err, "SeekData should succeed")
				var lseekResp types.LseekResp
				lseekResp.Decode(raw)

				t.Logf("SeekData returned offset: %d", lseekResp.NewOffset)
				assert.Equal(t, int64(0), lseekResp.NewOffset, "SeekData should return the start of the first data region")

				// Seek to the second data region
				lseekPayload.Offset = 1024 * 1024 // Start searching after the first data region

				raw, err = clientSession.CallMsg(ctx, "vss/Lseek", &lseekPayload)
				require.NoError(t, err, "SeekData should succeed")
				lseekResp.Decode(raw)

				t.Logf("SeekData returned offset: %d", lseekResp.NewOffset)
				assert.Equal(t, int64(1024*1024), lseekResp.NewOffset, "SeekData should return the start of the second data region")
			})

			// Test SeekHole
			t.Run("SeekHole", func(t *testing.T) {
				// Seek to the first hole region
				lseekPayload := types.LseekReq{
					HandleID: openResult,
					Offset:   0,
					Whence:   SeekHole,
				}

				raw, err := clientSession.CallMsg(ctx, "vss/Lseek", &lseekPayload)
				require.NoError(t, err, "SeekHole should succeed")
				var lseekResp types.LseekResp
				lseekResp.Decode(raw)

				t.Logf("SeekHole returned offset: %d", lseekResp.NewOffset)
				assert.Equal(t, int64(65536), lseekResp.NewOffset, "SeekHole should return the start of the first hole region")

				// Seek to the second hole region
				lseekPayload.Offset = 1048576 // Start searching after the first data region
				raw, err = clientSession.CallMsg(ctx, "vss/Lseek", &lseekPayload)
				require.NoError(t, err, "SeekHole should succeed")
				lseekResp.Decode(raw)

				t.Logf("SeekHole returned offset: %d", lseekResp.NewOffset)
				assert.Equal(t, int64(1114112), lseekResp.NewOffset, "SeekHole should return the start of the second hole region")
			})

			// Close the file
			closePayload := types.CloseReq{HandleID: openResult}
			resp, err := clientSession.Call("vss/Close", &closePayload)
			assert.NoError(t, err, "Close should succeed")
			assert.Equal(t, 200, resp.Status)
		})

		// Close the file
		closePayload := types.CloseReq{HandleID: openResult}
		resp, err := clientSession.Call("vss/Close", &closePayload)
		assert.NoError(t, err, "Close should succeed")
		assert.Equal(t, 200, resp.Status)
	})
}
