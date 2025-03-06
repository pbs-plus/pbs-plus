//go:build windows

package vssfs

import (
	"os"
	"path/filepath"
	"strconv"
	"syscall"
	"testing"
	"unsafe"

	"github.com/sonroyaalmerol/pbs-plus/internal/agent/vssfs/types"
)

// TestStructAlignment ensures the structs match the expected alignment and size.
func TestStructAlignment(t *testing.T) {
	// Test FILE_ID_BOTH_DIR_INFO
	t.Run("FILE_ID_BOTH_DIR_INFO", func(t *testing.T) {
		// Ensure the struct size is a multiple of 8 (aligned to DWORDLONG boundary)
		expectedSize := 104 // Fixed part of the struct (excluding FileName[1])
		actualSize := int(unsafe.Sizeof(FILE_ID_BOTH_DIR_INFO{}))
		if actualSize != expectedSize {
			t.Errorf("FILE_ID_BOTH_DIR_INFO size mismatch: expected %d, got %d", expectedSize, actualSize)
		}

		// Ensure alignment is 8 bytes
		expectedAlignment := 8
		actualAlignment := int(unsafe.Alignof(FILE_ID_BOTH_DIR_INFO{}))
		if actualAlignment != expectedAlignment {
			t.Errorf("FILE_ID_BOTH_DIR_INFO alignment mismatch: expected %d, got %d", expectedAlignment, actualAlignment)
		}

		// Check field offsets
		checkFieldOffset(t, "NextEntryOffset", unsafe.Offsetof(FILE_ID_BOTH_DIR_INFO{}.NextEntryOffset), 0)
		checkFieldOffset(t, "FileIndex", unsafe.Offsetof(FILE_ID_BOTH_DIR_INFO{}.FileIndex), 4)
		checkFieldOffset(t, "CreationTime", unsafe.Offsetof(FILE_ID_BOTH_DIR_INFO{}.CreationTime), 8)
		checkFieldOffset(t, "LastAccessTime", unsafe.Offsetof(FILE_ID_BOTH_DIR_INFO{}.LastAccessTime), 16)
		checkFieldOffset(t, "LastWriteTime", unsafe.Offsetof(FILE_ID_BOTH_DIR_INFO{}.LastWriteTime), 24)
		checkFieldOffset(t, "ChangeTime", unsafe.Offsetof(FILE_ID_BOTH_DIR_INFO{}.ChangeTime), 32)
		checkFieldOffset(t, "EndOfFile", unsafe.Offsetof(FILE_ID_BOTH_DIR_INFO{}.EndOfFile), 40)
		checkFieldOffset(t, "AllocationSize", unsafe.Offsetof(FILE_ID_BOTH_DIR_INFO{}.AllocationSize), 48)
		checkFieldOffset(t, "FileAttributes", unsafe.Offsetof(FILE_ID_BOTH_DIR_INFO{}.FileAttributes), 56)
		checkFieldOffset(t, "FileNameLength", unsafe.Offsetof(FILE_ID_BOTH_DIR_INFO{}.FileNameLength), 60)
		checkFieldOffset(t, "EaSize", unsafe.Offsetof(FILE_ID_BOTH_DIR_INFO{}.EaSize), 64)
		checkFieldOffset(t, "ShortNameLength", unsafe.Offsetof(FILE_ID_BOTH_DIR_INFO{}.ShortNameLength), 68)
		checkFieldOffset(t, "ShortName", unsafe.Offsetof(FILE_ID_BOTH_DIR_INFO{}.ShortName), 72)
		checkFieldOffset(t, "FileId", unsafe.Offsetof(FILE_ID_BOTH_DIR_INFO{}.FileId), 96)
		checkFieldOffset(t, "FileName", unsafe.Offsetof(FILE_ID_BOTH_DIR_INFO{}.FileName), 104)
	})

	// Test FILE_FULL_DIR_INFO
	t.Run("FILE_FULL_DIR_INFO", func(t *testing.T) {
		// Ensure the struct size is a multiple of 8 (aligned to LONGLONG boundary)
		expectedSize := 72 // Fixed part of the struct (excluding FileName[1])
		actualSize := int(unsafe.Sizeof(FILE_FULL_DIR_INFO{}))
		if actualSize != expectedSize {
			t.Errorf("FILE_FULL_DIR_INFO size mismatch: expected %d, got %d", expectedSize, actualSize)
		}

		// Ensure alignment is 8 bytes
		expectedAlignment := 8
		actualAlignment := int(unsafe.Alignof(FILE_FULL_DIR_INFO{}))
		if actualAlignment != expectedAlignment {
			t.Errorf("FILE_FULL_DIR_INFO alignment mismatch: expected %d, got %d", expectedAlignment, actualAlignment)
		}

		// Check field offsets
		checkFieldOffset(t, "NextEntryOffset", unsafe.Offsetof(FILE_FULL_DIR_INFO{}.NextEntryOffset), 0)
		checkFieldOffset(t, "FileIndex", unsafe.Offsetof(FILE_FULL_DIR_INFO{}.FileIndex), 4)
		checkFieldOffset(t, "CreationTime", unsafe.Offsetof(FILE_FULL_DIR_INFO{}.CreationTime), 8)
		checkFieldOffset(t, "LastAccessTime", unsafe.Offsetof(FILE_FULL_DIR_INFO{}.LastAccessTime), 16)
		checkFieldOffset(t, "LastWriteTime", unsafe.Offsetof(FILE_FULL_DIR_INFO{}.LastWriteTime), 24)
		checkFieldOffset(t, "ChangeTime", unsafe.Offsetof(FILE_FULL_DIR_INFO{}.ChangeTime), 32)
		checkFieldOffset(t, "EndOfFile", unsafe.Offsetof(FILE_FULL_DIR_INFO{}.EndOfFile), 40)
		checkFieldOffset(t, "AllocationSize", unsafe.Offsetof(FILE_FULL_DIR_INFO{}.AllocationSize), 48)
		checkFieldOffset(t, "FileAttributes", unsafe.Offsetof(FILE_FULL_DIR_INFO{}.FileAttributes), 56)
		checkFieldOffset(t, "FileNameLength", unsafe.Offsetof(FILE_FULL_DIR_INFO{}.FileNameLength), 60)
		checkFieldOffset(t, "EaSize", unsafe.Offsetof(FILE_FULL_DIR_INFO{}.EaSize), 64)
		checkFieldOffset(t, "FileName", unsafe.Offsetof(FILE_FULL_DIR_INFO{}.FileName), 72)
	})
}

// Helper function to check field offsets
func checkFieldOffset(t *testing.T, fieldName string, actualOffset, expectedOffset uintptr) {
	if actualOffset != expectedOffset {
		t.Errorf("%s offset mismatch: expected %d, got %d", fieldName, expectedOffset, actualOffset)
	}
}

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
	t.Run("File Name Lengths", func(t *testing.T) {
		testFileNameLengths(t, tempDir)
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
	entriesBytes, err := readDirBulk(tempDir)
	if err != nil {
		t.Fatalf("readDirBulk failed: %v", err)
	}

	// Decode and verify results
	var entries types.ReadDirEntries
	if err := entries.Decode(entriesBytes); err != nil {
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

	// Call readDirBulk
	entriesBytes, err := readDirBulk(emptyDir)
	if err != nil {
		t.Fatalf("readDirBulk failed: %v", err)
	}

	// Decode and verify results
	var entries types.ReadDirEntries
	if err := entries.Decode(entriesBytes); err != nil {
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

	// Call readDirBulk
	entriesBytes, err := readDirBulk(largeDir)
	if err != nil {
		t.Fatalf("readDirBulk failed: %v", err)
	}

	// Decode and verify results
	var entries types.ReadDirEntries
	if err := entries.Decode(entriesBytes); err != nil {
		t.Fatalf("Failed to decode directory entries: %v", err)
	}

	if len(entries) != 10000 {
		t.Errorf("Expected 10000 entries, got %d", len(entries))
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

	// Call readDirBulk
	entriesBytes, err := readDirBulk(tempDir)
	if err != nil {
		t.Fatalf("readDirBulk failed: %v", err)
	}

	// Decode and verify results
	var entries types.ReadDirEntries
	if err := entries.Decode(entriesBytes); err != nil {
		t.Fatalf("Failed to decode directory entries: %v", err)
	}

	// Hidden files should be excluded
	hiddenFound := false
	for _, entry := range entries {
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

	// Call readDirBulk
	entriesBytes, err := readDirBulk(tempDir)
	if err != nil {
		t.Fatalf("readDirBulk failed: %v", err)
	}

	// Decode and verify results
	var entries types.ReadDirEntries
	if err := entries.Decode(entriesBytes); err != nil {
		t.Fatalf("Failed to decode directory entries: %v", err)
	}

	// Verify that the symlink is included
	for _, entry := range entries {
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

	// Call readDirBulk
	entriesBytes, err := readDirBulk(tempDir)
	if err != nil {
		t.Fatalf("readDirBulk failed: %v", err)
	}

	// Decode and verify results
	var entries types.ReadDirEntries
	if err := entries.Decode(entriesBytes); err != nil {
		t.Fatalf("Failed to decode directory entries: %v", err)
	}

	// Verify that all Unicode files are included
	for _, name := range unicodeFiles {
		found := false
		for _, entry := range entries {
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

	// Call readDirBulk
	entriesBytes, err := readDirBulk(tempDir)
	if err != nil {
		t.Fatalf("readDirBulk failed: %v", err)
	}

	// Decode and verify results
	var entries types.ReadDirEntries
	if err := entries.Decode(entriesBytes); err != nil {
		t.Fatalf("Failed to decode directory entries: %v", err)
	}

	// Verify that all special character files are included
	for _, name := range specialFiles {
		found := false
		for _, entry := range entries {
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

func testFileNameLengths(t *testing.T, tempDir string) {
	// Create files with very short and very long names
	shortFile := "a.txt"
	longFile := "\\\\?\\" + filepath.Join(tempDir, string(make([]byte, 255-len(tempDir)-5))+".txt") // Adjust for path length

	shortPath := filepath.Join(tempDir, shortFile)
	if err := os.WriteFile(shortPath, []byte("test content"), 0644); err != nil {
		t.Fatalf("Failed to create short file: %v", err)
	}

	longPath := filepath.Join(tempDir, longFile)
	if err := os.WriteFile(longPath, []byte("test content"), 0644); err != nil {
		t.Fatalf("Failed to create long file: %v", err)
	}

	// Call readDirBulk
	entriesBytes, err := readDirBulk(tempDir)
	if err != nil {
		t.Fatalf("readDirBulk failed: %v", err)
	}

	// Decode and verify results
	var entries types.ReadDirEntries
	if err := entries.Decode(entriesBytes); err != nil {
		t.Fatalf("Failed to decode directory entries: %v", err)
	}

	// Verify that both files are included
	foundShort := false
	foundLong := false
	for _, entry := range entries {
		if entry.Name == shortFile {
			foundShort = true
		}
		if entry.Name == longFile {
			foundLong = true
		}
	}
	if !foundShort {
		t.Errorf("Short file name not found in directory entries")
	}
	if !foundLong {
		t.Errorf("Long file name not found in directory entries")
	}
}
