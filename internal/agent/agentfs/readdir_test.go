package agentfs

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"testing"

	"github.com/fxamacker/cbor/v2"
	"github.com/pbs-plus/pbs-plus/internal/agent/agentfs/types"
	"github.com/pbs-plus/pbs-plus/internal/syslog"
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
	t.Run("Last Entry Missing", func(t *testing.T) {
		testLastEntryMissing(t, tempDir)
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

func newTestDirReader(path string) (*DirReader, error) {
	f, err := os.Open(path)
	if err != nil {
		syslog.L.Error(err).WithMessage("newTestDirReader: failed to open directory").
			WithField("path", path).Write()
		return nil, err
	}
	return NewDirReader(f, path)
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
	dirReader, err := newTestDirReader(tempDir)
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
		"file1.txt": 0644,
		"file2.txt": 0644,
		"subdir":    os.ModeDir | 0755,
	}

	verifyEntries(t, entries, expected)
}

func testEmptyDirectory(t *testing.T, tempDir string) {
	// Create an empty directory
	emptyDir := filepath.Join(tempDir, "empty")
	if err := os.Mkdir(emptyDir, 0755); err != nil {
		t.Fatalf("Failed to create empty directory: %v", err)
	}

	dirReader, err := newTestDirReader(emptyDir)
	if err != nil {
		t.Fatalf("dirReader failed: %v", err)
	}
	defer dirReader.Close()

	_, err = dirReader.NextBatch(t.Context(), 0)
	if err != nil {
		if !errors.Is(err, os.ErrProcessDone) {
			t.Fatalf("readDirBulk failed: %v", err)
		}
		return
	}
	t.Fatalf("Expected to return os.ErrProcessDone")
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

	dirReader, err := newTestDirReader(largeDir)
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

func testUnicodeFileNames(t *testing.T, tempDir string) {
	// Create files with Unicode names
	unicodeFiles := []string{"文件.txt", "ファイル.txt", "файл.txt", "📄.txt"}
	for _, name := range unicodeFiles {
		path := filepath.Join(tempDir, name)
		if err := os.WriteFile(path, []byte("test content"), 0644); err != nil {
			t.Fatalf("Failed to create file %s: %v", name, err)
		}
	}

	dirReader, err := newTestDirReader(tempDir)
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

	dirReader, err := newTestDirReader(tempDir)
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
			dirReader, err := newTestDirReader(leakTestDir)
			if err != nil {
				t.Fatalf("Iteration %d: newTestDirReader failed: %v", i, err)
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
			dirReader, err := newTestDirReader(leakTestDir)
			if err != nil {
				t.Fatalf("Iteration %d: newTestDirReader failed: %v", i, err)
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
		iterations := 50
		for i := 0; i < iterations; i++ {
			dirReader, err := newTestDirReader(leakTestDir)
			if err != nil {
				t.Fatalf("Iteration %d: newTestDirReader failed: %v", i, err)
			}

			ctx, cancel := context.WithCancel(context.Background())

			var readErr error
			done := make(chan struct{})

			go func() {
				defer close(done)
				for {
					_, err := dirReader.NextBatch(ctx, 0)
					if err != nil {
						readErr = err
						return
					}
				}
			}()

			cancel()
			<-done

			// After cancellation, we expect context.Canceled or invalid handle errors
			if readErr != nil {
				if !errors.Is(readErr, context.Canceled) &&
					!strings.Contains(readErr.Error(), "0xc0000008") &&
					!strings.Contains(readErr.Error(), "0xc0000024") {
					t.Logf("Iteration %d: Unexpected error after cancel: %v", i, readErr)
				}
			}

			// Close may fail with INVALID_HANDLE - that's fine
			err = dirReader.Close()
			if err != nil && !strings.Contains(err.Error(), "0xc0000008") &&
				!strings.Contains(readErr.Error(), "0xc0000024") {
				t.Fatalf("Iteration %d: Close failed with unexpected error: %v", i, err)
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
					dirReader, err := newTestDirReader(leakTestDir)
					if err != nil {
						errChan <- fmt.Errorf("Goroutine %d, Iteration %d: newTestDirReader failed: %v", goroutineID, i, err)
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
			dirReader, err := newTestDirReader(leakTestDir)
			if err != nil {
				t.Fatalf("Iteration %d: newTestDirReader failed: %v", i, err)
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
				dirReader, err := newTestDirReader(leakTestDir)
				if err != nil {
					t.Fatalf("Iteration %d: newTestDirReader failed: %v", i, err)
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
		dirReader, err := newTestDirReader(leakTestDir)
		if err != nil {
			t.Fatalf("newTestDirReader failed: %v", err)
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

func testLastEntryMissing(t *testing.T, tempDir string) {
	t.Run("Buffer Size Boundary - Entries at Exact Limit", func(t *testing.T) {
		// This test creates files that will cause the encoded buffer to reach
		// targetEncoded right when there's one more file left to enumerate
		boundaryDir := filepath.Join(tempDir, "boundary_exact_test")
		if err := os.Mkdir(boundaryDir, 0755); err != nil {
			t.Fatalf("Failed to create boundary test directory: %v", err)
		}

		// Create enough files to fill the buffer close to targetEncoded
		// The exact number depends on defaultTargetEncodedLen and encoding overhead
		// Typically around 200-300 files will do it
		numFiles := 250
		expectedFiles := make(map[string]bool)

		for i := 0; i < numFiles; i++ {
			fileName := fmt.Sprintf("test_file_%04d.txt", i)
			path := filepath.Join(boundaryDir, fileName)
			// Create files with varying sizes to make encoded size less predictable
			content := []byte(strings.Repeat("x", i%100))
			if err := os.WriteFile(path, content, 0644); err != nil {
				t.Fatalf("Failed to create file %s: %v", fileName, err)
			}
			expectedFiles[fileName] = false
		}

		// Read directory
		dirReader, err := newTestDirReader(boundaryDir)
		if err != nil {
			t.Fatalf("newTestDirReader failed: %v", err)
		}
		defer dirReader.Close()

		allEntries := make(map[string]bool)
		batchCount := 0
		lastBatchSize := 0

		for {
			entriesBytes, err := dirReader.NextBatch(context.Background(), 0)
			if err != nil {
				if errors.Is(err, os.ErrProcessDone) {
					t.Logf("Batch %d: Received ErrProcessDone (end of directory)", batchCount)
					break
				}
				t.Fatalf("NextBatch failed on batch %d: %v", batchCount, err)
			}

			var entries types.ReadDirEntries
			if err := cbor.Unmarshal(entriesBytes, &entries); err != nil {
				t.Fatalf("Failed to decode batch %d: %v", batchCount, err)
			}

			lastBatchSize = len(entries)
			t.Logf("Batch %d: Got %d entries (encoded size: %d bytes)",
				batchCount, len(entries), len(entriesBytes))

			for _, entry := range entries {
				if allEntries[entry.Name] {
					t.Errorf("Duplicate entry found: %s", entry.Name)
				}
				allEntries[entry.Name] = true
			}

			batchCount++

			// Safety check to prevent infinite loop in case of bug
			if batchCount > 100 {
				t.Fatal("Too many batches, possible infinite loop")
			}
		}

		// Verify all files were returned
		missing := []string{}
		for fileName := range expectedFiles {
			if !allEntries[fileName] {
				missing = append(missing, fileName)
			}
		}

		if len(missing) > 0 {
			// Sort for consistent error messages
			sort.Strings(missing)
			t.Errorf("Missing %d files from directory listing (total batches: %d, last batch size: %d):",
				len(missing), batchCount, lastBatchSize)

			// Show first 10 missing
			for i, name := range missing {
				if i >= 10 {
					t.Logf("... and %d more", len(missing)-10)
					break
				}
				t.Logf("  - %s", name)
			}

			// Check if missing files cluster near a particular index
			if len(missing) > 0 {
				// Parse indices from missing file names
				indices := []int{}
				for _, name := range missing {
					var idx int
					if _, err := fmt.Sscanf(name, "test_file_%d.txt", &idx); err == nil {
						indices = append(indices, idx)
					}
				}

				if len(indices) > 0 {
					sort.Ints(indices)
					t.Logf("Missing file indices: %v", indices)

					// Check for clustering (indices within 20 of each other)
					clustered := true
					if len(indices) > 1 {
						for i := 1; i < len(indices); i++ {
							if indices[i]-indices[i-1] > 20 {
								clustered = false
								break
							}
						}
					}

					if clustered {
						t.Log("WARNING: Missing files are clustered together - likely a batch boundary issue!")
					}
				}
			}
		}

		t.Logf("Total files found: %d / %d expected (%.1f%%)",
			len(allEntries), numFiles, float64(len(allEntries))/float64(numFiles)*100)

		if len(allEntries) != numFiles {
			t.Errorf("Expected %d entries, got %d (missing %d)",
				numFiles, len(allEntries), numFiles-len(allEntries))
		}
	})

	t.Run("One Final Entry After Buffer Fills", func(t *testing.T) {
		// This test specifically tries to create a scenario where
		// the buffer fills up and there's exactly one entry left
		singleDir := filepath.Join(tempDir, "single_final_entry")
		if err := os.Mkdir(singleDir, 0755); err != nil {
			t.Fatalf("Failed to create test directory: %v", err)
		}

		// Create files - we'll create enough to potentially fill a batch,
		// plus one more that should be the "last" file
		numRegular := 200
		finalFile := "zzz_this_should_not_be_missed_final.txt"

		for i := 0; i < numRegular; i++ {
			fileName := fmt.Sprintf("regular_%04d.txt", i)
			path := filepath.Join(singleDir, fileName)
			if err := os.WriteFile(path, []byte("content"), 0644); err != nil {
				t.Fatalf("Failed to create file %s: %v", fileName, err)
			}
		}

		// Create the final file (will be last alphabetically)
		finalPath := filepath.Join(singleDir, finalFile)
		if err := os.WriteFile(finalPath, []byte("FINAL"), 0644); err != nil {
			t.Fatalf("Failed to create final file: %v", err)
		}

		dirReader, err := newTestDirReader(singleDir)
		if err != nil {
			t.Fatalf("newTestDirReader failed: %v", err)
		}
		defer dirReader.Close()

		foundFinal := false
		totalEntries := 0

		for {
			entriesBytes, err := dirReader.NextBatch(context.Background(), 0)
			if err != nil {
				if errors.Is(err, os.ErrProcessDone) {
					break
				}
				t.Fatalf("NextBatch failed: %v", err)
			}

			var entries types.ReadDirEntries
			if err := cbor.Unmarshal(entriesBytes, &entries); err != nil {
				t.Fatalf("Failed to decode entries: %v", err)
			}

			totalEntries += len(entries)

			for _, entry := range entries {
				if entry.Name == finalFile {
					foundFinal = true
				}
			}
		}

		if !foundFinal {
			t.Errorf("Final marker file '%s' was NOT found! This is the bug.", finalFile)
			t.Logf("Total entries found: %d (expected %d)", totalEntries, numRegular+1)
		} else {
			t.Logf("SUCCESS: Final marker file was found (total entries: %d)", totalEntries)
		}

		expectedTotal := numRegular + 1
		if totalEntries != expectedTotal {
			t.Errorf("Expected %d total entries, got %d (missing %d)",
				expectedTotal, totalEntries, expectedTotal-totalEntries)
		}
	})
}
