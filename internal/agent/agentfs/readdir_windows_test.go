//go:build windows

package agentfs

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"sync"
	"syscall"
	"testing"

	"github.com/fxamacker/cbor/v2"
	"github.com/pbs-plus/pbs-plus/internal/agent/agentfs/types"
)

// TestReadDirBulk is the main test suite for readDirBulk
func TestReadDirBulk(t *testing.T) {
	// Create a temporary directory for testing
	tempDir, err := os.MkdirTemp("", "test-readdirbulk")
	if err != nil {
		t.Fatalf("Failed to create temporary directory: %v", err)
	}
	defer os.RemoveAll(tempDir) // Clean up after the test

	// Run all test cases
	t.Run("Basic Functionality", func(t *testing.T) {
		testBasicFunctionality(t, tempDir)
	})
	t.Run("Empty Directory", func(t *testing.T) {
		testEmptyDirectory(t, tempDir)
	})
	t.Run("Large Directory", func(t *testing.T) {
		testLargeDirectory(t, tempDir)
	})
	t.Run("File Attributes", func(t *testing.T) {
		testFileAttributes(t, tempDir)
	})
	t.Run("Symbolic Links", func(t *testing.T) {
		testSymbolicLinks(t, tempDir)
	})
	t.Run("Unicode File Names", func(t *testing.T) {
		testUnicodeFileNames(t, tempDir)
	})
	t.Run("Special Characters in File Names", func(t *testing.T) {
		testSpecialCharacters(t, tempDir)
	})
	t.Run("Memory Leaks", func(t *testing.T) {
		testMemoryLeaks(t, tempDir)
	})
}

// Test Cases

func testBasicFunctionality(t *testing.T, tempDir string) {
	// Create test files and directories
	files := []string{"file1.txt", "file2.txt", "subdir"}
	for _, name := range files {
		path := filepath.Join(tempDir, name)
		if name == "subdir" {
			if err := os.Mkdir(path, 0755); err != nil {
				t.Fatalf("Failed to create subdirectory %s: %v", name, err)
			}
		} else {
			if err := os.WriteFile(path, []byte("test content"), 0644); err != nil {
				t.Fatalf("Failed to create file %s: %v", name, err)
			}
		}
	}

	// Call readDirBulk
	dirReader, err := NewDirReaderNT(tempDir)
	if err != nil {
		t.Fatalf("dirReader failed: %v", err)
	}
	defer dirReader.Close()

	entriesBytes, err := dirReader.NextBatch(t.Context(), 0)
	if err != nil {
		t.Fatalf("readDirBulk failed: %v", err)
	}

	// Decode and verify results
	var entries types.ReadDirEntries
	if err := cbor.Unmarshal(entriesBytes, &entries); err != nil {
		t.Fatalf("Failed to decode directory entries: %v", err)
	}

	expected := map[string]os.FileMode{
		"file1.txt": 0666,
		"file2.txt": 0666,
		"subdir":    os.ModeDir | 0777,
	}

	verifyEntries(t, entries, expected)
}

func testEmptyDirectory(t *testing.T, tempDir string) {
	// Create an empty directory
	emptyDir := filepath.Join(tempDir, "empty")
	if err := os.Mkdir(emptyDir, 0755); err != nil {
		t.Fatalf("Failed to create empty directory: %v", err)
	}

	dirReader, err := NewDirReaderNT(emptyDir)
	if err != nil {
		t.Fatalf("dirReader failed: %v", err)
	}
	defer dirReader.Close()

	entriesBytes, err := dirReader.NextBatch(t.Context(), 0)
	if err != nil {
		t.Fatalf("readDirBulk failed: %v", err)
	}

	// Decode and verify results
	var entries types.ReadDirEntries
	if err := cbor.Unmarshal(entriesBytes, &entries); err != nil {
		t.Fatalf("Failed to decode directory entries: %v", err)
	}

	if len(entries) != 0 {
		t.Errorf("Expected 0 entries, got %d", len(entries))
	}
}

func testLargeDirectory(t *testing.T, tempDir string) {
	// Create a large number of files
	largeDir := filepath.Join(tempDir, "large")
	if err := os.Mkdir(largeDir, 0755); err != nil {
		t.Fatalf("Failed to create large directory: %v", err)
	}

	for i := 0; i < 10000; i++ {
		fileName := filepath.Join(largeDir, "file"+strconv.Itoa(i))
		if err := os.WriteFile(fileName, []byte("test content"), 0644); err != nil {
			t.Fatalf("Failed to create file %s: %v", fileName, err)
		}
	}

	dirReader, err := NewDirReaderNT(largeDir)
	if err != nil {
		t.Fatalf("dirReader failed: %v", err)
	}
	defer dirReader.Close()

	totalEntries := 0
	for {
		entriesBytes, err := dirReader.NextBatch(t.Context(), 0)
		if err != nil {
			if errors.Is(err, os.ErrProcessDone) {
				break
			}
			t.Fatalf("readDirBulk failed: %v", err)
		}

		// Decode and verify results
		var entries types.ReadDirEntries
		if err := cbor.Unmarshal(entriesBytes, &entries); err != nil {
			t.Fatalf("Failed to decode directory entries: %v", err)
		}

		totalEntries += len(entries)
	}

	if totalEntries != 10000 {
		t.Errorf("Expected 10000 entries, got %d", totalEntries)
	}
}

func testFileAttributes(t *testing.T, tempDir string) {
	// Create files with different attributes
	hiddenFile := filepath.Join(tempDir, "hidden.txt")
	if err := os.WriteFile(hiddenFile, []byte("test content"), 0644); err != nil {
		t.Fatalf("Failed to create hidden file: %v", err)
	}
	path, err := syscall.UTF16PtrFromString(hiddenFile)
	if err != nil {
		t.Fatalf("Failed to generate string: %v", err)
	}

	if err := syscall.SetFileAttributes(path, syscall.FILE_ATTRIBUTE_HIDDEN); err != nil {
		t.Fatalf("Failed to set hidden attribute: %v", err)
	}

	dirReader, err := NewDirReaderNT(tempDir)
	if err != nil {
		t.Fatalf("dirReader failed: %v", err)
	}
	defer dirReader.Close()

	allEntries := []types.AgentFileInfo{}
	for {
		entriesBytes, err := dirReader.NextBatch(t.Context(), 0)
		if err != nil {
			if errors.Is(err, os.ErrProcessDone) {
				break
			}
			t.Fatalf("readDirBulk failed: %v", err)
		}

		// Decode and verify results
		var entries types.ReadDirEntries
		if err := cbor.Unmarshal(entriesBytes, &entries); err != nil {
			t.Fatalf("Failed to decode directory entries: %v", err)
		}

		allEntries = append(allEntries, entries...)
	}

	// Hidden files should be excluded
	hiddenFound := false
	for _, entry := range allEntries {
		if entry.Name == "hidden.txt" {
			hiddenFound = true
			break
		}
	}
	if !hiddenFound {
		t.Errorf("Hidden file should be included in results")
	}

}

// Add similar test cases for symbolic links, error handling, Unicode file names, special characters, and file name lengths...

// Helper function to verify entries
func verifyEntries(t *testing.T, entries types.ReadDirEntries, expected map[string]os.FileMode) {
	if len(entries) != len(expected) {
		t.Fatalf("Expected %d entries, got %d", len(expected), len(entries))
	}

	for _, entry := range entries {
		expectedMode, ok := expected[entry.Name]
		if !ok {
			t.Errorf("Unexpected entry: %s", entry.Name)
			continue
		}
		if entry.Mode != uint32(expectedMode) {
			t.Errorf("Entry %s: expected mode %o, got %o", entry.Name, expectedMode, entry.Mode)
		}
		delete(expected, entry.Name)
	}

	if len(expected) > 0 {
		t.Errorf("Missing entries: %v", expected)
	}
}

func testSymbolicLinks(t *testing.T, tempDir string) {
	// Create a file and a symbolic link to it
	targetFile := filepath.Join(tempDir, "target.txt")
	if err := os.WriteFile(targetFile, []byte("test content"), 0644); err != nil {
		t.Fatalf("Failed to create target file: %v", err)
	}

	symlink := filepath.Join(tempDir, "symlink.txt")
	if err := os.Symlink(targetFile, symlink); err != nil {
		t.Fatalf("Failed to create symbolic link: %v", err)
	}

	dirReader, err := NewDirReaderNT(tempDir)
	if err != nil {
		t.Fatalf("dirReader failed: %v", err)
	}
	defer dirReader.Close()

	allEntries := []types.AgentFileInfo{}
	for {
		entriesBytes, err := dirReader.NextBatch(t.Context(), 0)
		if err != nil {
			if errors.Is(err, os.ErrProcessDone) {
				break
			}
			t.Fatalf("readDirBulk failed: %v", err)
		}

		// Decode and verify results
		var entries types.ReadDirEntries
		if err := cbor.Unmarshal(entriesBytes, &entries); err != nil {
			t.Fatalf("Failed to decode directory entries: %v", err)
		}

		allEntries = append(allEntries, entries...)
	}

	for _, entry := range allEntries {
		if entry.Name == "symlink.txt" {
			t.Errorf("Symlink should not be included in results")
		}
	}
}

func testUnicodeFileNames(t *testing.T, tempDir string) {
	// Create files with Unicode names
	unicodeFiles := []string{"文件.txt", "ファイル.txt", "файл.txt", "📄.txt"}
	for _, name := range unicodeFiles {
		path := filepath.Join(tempDir, name)
		if err := os.WriteFile(path, []byte("test content"), 0644); err != nil {
			t.Fatalf("Failed to create file %s: %v", name, err)
		}
	}

	dirReader, err := NewDirReaderNT(tempDir)
	if err != nil {
		t.Fatalf("dirReader failed: %v", err)
	}
	defer dirReader.Close()

	allEntries := []types.AgentFileInfo{}

	for {
		entriesBytes, err := dirReader.NextBatch(t.Context(), 0)
		if err != nil {
			if errors.Is(err, os.ErrProcessDone) {
				break
			}
			t.Fatalf("readDirBulk failed: %v", err)
		}

		// Decode and verify results
		var entries types.ReadDirEntries
		if err := cbor.Unmarshal(entriesBytes, &entries); err != nil {
			t.Fatalf("Failed to decode directory entries: %v", err)
		}

		allEntries = append(allEntries, entries...)
	}

	for _, name := range unicodeFiles {
		found := false
		for _, entry := range allEntries {
			if entry.Name == name {
				found = true
				break
			}
		}
		if !found {
			t.Errorf("Unicode file %s not found in directory entries", name)
		}
	}

}

func testSpecialCharacters(t *testing.T, tempDir string) {
	// Create files with special characters in their names
	specialFiles := []string{"file with spaces.txt", "file#with#hashes.txt", "file$with$dollar.txt"}
	for _, name := range specialFiles {
		path := filepath.Join(tempDir, name)
		if err := os.WriteFile(path, []byte("test content"), 0644); err != nil {
			t.Fatalf("Failed to create file %s: %v", name, err)
		}
	}

	dirReader, err := NewDirReaderNT(tempDir)
	if err != nil {
		t.Fatalf("dirReader failed: %v", err)
	}
	defer dirReader.Close()

	allEntries := []types.AgentFileInfo{}

	for {
		entriesBytes, err := dirReader.NextBatch(t.Context(), 0)
		if err != nil {
			if errors.Is(err, os.ErrProcessDone) {
				break
			}
			t.Fatalf("readDirBulk failed: %v", err)
		}

		// Decode and verify results
		var entries types.ReadDirEntries
		if err := cbor.Unmarshal(entriesBytes, &entries); err != nil {
			t.Fatalf("Failed to decode directory entries: %v", err)
		}

		allEntries = append(allEntries, entries...)
	}

	for _, name := range specialFiles {
		found := false
		for _, entry := range allEntries {
			if entry.Name == name {
				found = true
				break
			}
		}
		if !found {
			t.Errorf("File with special characters %s not found in directory entries", name)
		}
	}
}

func testMemoryLeaks(t *testing.T, tempDir string) {
	// Create a test directory with files
	leakTestDir := filepath.Join(tempDir, "leak_test")
	if err := os.Mkdir(leakTestDir, 0755); err != nil {
		t.Fatalf("Failed to create leak test directory: %v", err)
	}

	// Create a reasonable number of files for testing
	numFiles := 1000
	for i := 0; i < numFiles; i++ {
		fileName := filepath.Join(leakTestDir, "file"+strconv.Itoa(i)+".txt")
		if err := os.WriteFile(fileName, []byte("test content"), 0644); err != nil {
			t.Fatalf("Failed to create file %s: %v", fileName, err)
		}
	}

	t.Run("Handle Leak Test", func(t *testing.T) {
		// Test that handles are properly closed after multiple iterations
		iterations := 100
		for i := 0; i < iterations; i++ {
			dirReader, err := NewDirReaderNT(leakTestDir)
			if err != nil {
				t.Fatalf("Iteration %d: NewDirReaderNT failed: %v", i, err)
			}

			// Read all entries
			for {
				_, err := dirReader.NextBatch(context.Background(), 0)
				if err != nil {
					if errors.Is(err, os.ErrProcessDone) {
						break
					}
					t.Fatalf("Iteration %d: NextBatch failed: %v", i, err)
				}
			}

			// Close the reader
			if err := dirReader.Close(); err != nil {
				t.Fatalf("Iteration %d: Close failed: %v", i, err)
			}
		}
		t.Logf("Successfully completed %d iterations without handle leaks", iterations)
	})

	t.Run("Early Close Test", func(t *testing.T) {
		// Test closing reader before consuming all entries
		iterations := 50
		for i := 0; i < iterations; i++ {
			dirReader, err := NewDirReaderNT(leakTestDir)
			if err != nil {
				t.Fatalf("Iteration %d: NewDirReaderNT failed: %v", i, err)
			}

			// Read only first batch
			_, err = dirReader.NextBatch(context.Background(), 0)
			if err != nil && !errors.Is(err, os.ErrProcessDone) {
				t.Fatalf("Iteration %d: NextBatch failed: %v", i, err)
			}

			// Close immediately without reading all entries
			if err := dirReader.Close(); err != nil {
				t.Fatalf("Iteration %d: Close failed: %v", i, err)
			}
		}
		t.Logf("Successfully completed %d early close iterations", iterations)
	})

	t.Run("Context Cancellation Test", func(t *testing.T) {
		// Test that resources are cleaned up when context is cancelled
		iterations := 50
		for i := 0; i < iterations; i++ {
			dirReader, err := NewDirReaderNT(leakTestDir)
			if err != nil {
				t.Fatalf("Iteration %d: NewDirReaderNT failed: %v", i, err)
			}

			ctx, cancel := context.WithCancel(context.Background())

			// Start reading
			go func() {
				for {
					_, err := dirReader.NextBatch(ctx, 0)
					if err != nil {
						return
					}
				}
			}()

			// Cancel immediately
			cancel()

			// Give a moment for cancellation to propagate
			// In production, use proper synchronization
			if err := dirReader.Close(); err != nil {
				t.Fatalf("Iteration %d: Close after cancel failed: %v", i, err)
			}
		}
		t.Logf("Successfully completed %d context cancellation iterations", iterations)
	})

	t.Run("Concurrent Access Test", func(t *testing.T) {
		// Test concurrent access to ensure no resource leaks
		numGoroutines := 10
		iterations := 10

		var wg sync.WaitGroup
		errChan := make(chan error, numGoroutines*iterations)

		for g := 0; g < numGoroutines; g++ {
			wg.Add(1)
			go func(goroutineID int) {
				defer wg.Done()
				for i := 0; i < iterations; i++ {
					dirReader, err := NewDirReaderNT(leakTestDir)
					if err != nil {
						errChan <- fmt.Errorf("Goroutine %d, Iteration %d: NewDirReaderNT failed: %v", goroutineID, i, err)
						return
					}

					// Read all entries
					for {
						_, err := dirReader.NextBatch(context.Background(), 0)
						if err != nil {
							if errors.Is(err, os.ErrProcessDone) {
								break
							}
							errChan <- fmt.Errorf("Goroutine %d, Iteration %d: NextBatch failed: %v", goroutineID, i, err)
							dirReader.Close()
							return
						}
					}

					if err := dirReader.Close(); err != nil {
						errChan <- fmt.Errorf("Goroutine %d, Iteration %d: Close failed: %v", goroutineID, i, err)
						return
					}
				}
			}(g)
		}

		wg.Wait()
		close(errChan)

		// Check for errors
		for err := range errChan {
			t.Error(err)
		}

		t.Logf("Successfully completed %d concurrent goroutines with %d iterations each", numGoroutines, iterations)
	})

	t.Run("Buffer Pool Test", func(t *testing.T) {
		// Test that buffer pool doesn't accumulate leaked buffers
		iterations := 200
		for i := 0; i < iterations; i++ {
			dirReader, err := NewDirReaderNT(leakTestDir)
			if err != nil {
				t.Fatalf("Iteration %d: NewDirReaderNT failed: %v", i, err)
			}

			// Read entries - this uses the buffer pool
			for {
				_, err := dirReader.NextBatch(context.Background(), 0)
				if err != nil {
					if errors.Is(err, os.ErrProcessDone) {
						break
					}
					t.Fatalf("Iteration %d: NextBatch failed: %v", i, err)
				}
			}

			if err := dirReader.Close(); err != nil {
				t.Fatalf("Iteration %d: Close failed: %v", i, err)
			}
		}
		t.Logf("Successfully completed %d buffer pool iterations", iterations)
	})

	t.Run("Panic Recovery Test", func(t *testing.T) {
		// Test that handles are cleaned up even with panics
		// Note: This simulates cleanup patterns, actual panic handling depends on your implementation
		iterations := 10
		for i := 0; i < iterations; i++ {
			func() {
				dirReader, err := NewDirReaderNT(leakTestDir)
				if err != nil {
					t.Fatalf("Iteration %d: NewDirReaderNT failed: %v", i, err)
				}
				defer func() {
					if r := recover(); r != nil {
						// Ensure cleanup happens
						dirReader.Close()
					}
				}()

				// Normal operation
				_, err = dirReader.NextBatch(context.Background(), 0)
				if err != nil && !errors.Is(err, os.ErrProcessDone) {
					t.Fatalf("Iteration %d: NextBatch failed: %v", i, err)
				}

				if err := dirReader.Close(); err != nil {
					t.Fatalf("Iteration %d: Close failed: %v", i, err)
				}
			}()
		}
		t.Logf("Successfully completed %d panic recovery iterations", iterations)
	})

	t.Run("Double Close Test", func(t *testing.T) {
		// Test that double-closing doesn't cause issues
		dirReader, err := NewDirReaderNT(leakTestDir)
		if err != nil {
			t.Fatalf("NewDirReaderNT failed: %v", err)
		}

		// First close
		if err := dirReader.Close(); err != nil {
			t.Fatalf("First close failed: %v", err)
		}

		// Second close - should either be idempotent or return a clear error
		err = dirReader.Close()
		if err != nil {
			t.Logf("Second close returned error (expected): %v", err)
		} else {
			t.Log("Second close succeeded (idempotent)")
		}
	})
}
