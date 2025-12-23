//go:build linux

package backup

import (
	"bufio"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"runtime"
	"sync"
	"sync/atomic"
	"syscall"
	"time"
)

func processFile(path string, removedCount *int64) error {
	inputFile, err := os.Open(path)
	if err != nil {
		return fmt.Errorf("opening file %s: %w", path, err)
	}
	defer inputFile.Close()

	// Get original file info to preserve permissions, ownership, and timestamps
	info, err := inputFile.Stat()
	if err != nil {
		return fmt.Errorf("getting stat of file %s: %w", path, err)
	}
	origMode := info.Mode()
	origModTime := info.ModTime()
	origAccessTime := origModTime // Use ModTime as AccessTime if not available

	// Retrieve UID and GID from the underlying stat
	statT, ok := info.Sys().(*syscall.Stat_t)
	if !ok {
		return fmt.Errorf("failed to retrieve underlying stat from file %s", path)
	}
	origUid := int(statT.Uid)
	origGid := int(statT.Gid)

	// On Unix systems, get the actual access time
	origAccessTime = time.Unix(statT.Atim.Sec, statT.Atim.Nsec)

	dir := filepath.Dir(path)
	tmpFile, err := os.CreateTemp(dir, "clean_")
	if err != nil {
		return fmt.Errorf("creating temp file in %s: %w", dir, err)
	}

	tmpName := tmpFile.Name()
	defer tmpFile.Close()

	scanner := bufio.NewScanner(inputFile)
	writer := bufio.NewWriter(tmpFile)

	var removedInFile int64
	for scanner.Scan() {
		line := scanner.Text()
		if isJunkLog(line) {
			// Count the removed junk log line and skip writing it
			removedInFile++
		} else {
			if _, err := writer.WriteString(line + "\n"); err != nil {
				return fmt.Errorf("writing to temp file for %s: %w", path, err)
			}
		}
	}
	if err := scanner.Err(); err != nil {
		return fmt.Errorf("scanning file %s: %w", path, err)
	}
	if err := writer.Flush(); err != nil {
		return fmt.Errorf("flushing writer for %s: %w", path, err)
	}

	// Ensure the temp file has the same permissions as the original
	if err := os.Chmod(tmpName, origMode); err != nil {
		return fmt.Errorf("setting permissions on temp file for %s: %w", path, err)
	}

	// Preserve the owner and group of the original file
	if err := os.Chown(tmpName, origUid, origGid); err != nil {
		return fmt.Errorf("setting ownership on temp file for %s: %w", path, err)
	}

	// Preserve the original timestamps
	if err := os.Chtimes(tmpName, origAccessTime, origModTime); err != nil {
		return fmt.Errorf("setting timestamps on temp file for %s: %w", path, err)
	}

	if err := os.Rename(tmpName, path); err != nil {
		return fmt.Errorf("renaming temp file for %s: %w", path, err)
	}

	atomic.AddInt64(removedCount, removedInFile)
	return nil
}

func RemoveJunkLogsRecursively(rootDir string) (int64, error) {
	fileCh := make(chan string, 100)

	var wg sync.WaitGroup
	numWorkers := runtime.NumCPU()

	var errOnce sync.Once
	var finalErr error

	// Global atomic counter for removed lines.
	var totalRemoved int64

	worker := func() {
		defer wg.Done()
		for path := range fileCh {
			log.Printf("Processing file: %s", path)
			if err := processFile(path, &totalRemoved); err != nil {
				errOnce.Do(func() {
					finalErr = err
				})
				log.Printf("Error processing file %s: %v", path, err)
			}
		}
	}

	// Start worker goroutines.
	for i := 0; i < numWorkers; i++ {
		wg.Add(1)
		go worker()
	}

	// List the entries in the root directory.
	entries, err := os.ReadDir(rootDir)
	if err != nil {
		return 0, err
	}

	// Process only the subdirectories of rootDir.
	for _, entry := range entries {
		if entry.IsDir() {
			subDir := filepath.Join(rootDir, entry.Name())
			err := filepath.WalkDir(subDir, func(path string, d os.DirEntry, err error) error {
				if err != nil {
					return err
				}
				if d.Type().IsRegular() {
					fileCh <- path
				}
				return nil
			})
			if err != nil {
				errOnce.Do(func() {
					finalErr = err
				})
				log.Printf("Error walking directory %s: %v", subDir, err)
			}
		}
	}

	close(fileCh)
	wg.Wait()

	if finalErr != nil {
		return totalRemoved, finalErr
	}

	return totalRemoved, nil
}
