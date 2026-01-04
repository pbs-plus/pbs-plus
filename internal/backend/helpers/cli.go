//go:build linux

package helpers

import (
	"bufio"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"
)

var (
	JunkSubstrings = []string{
		"upload_chunk done:",
		"POST /dynamic_chunk",
		"POST /dynamic_index",
		"PUT /dynamic_index",
		"dynamic_append",
		"successfully added chunk",
		"created new dynamic index",
		"GET /previous",
		"from previous backup.",
	}
)

func IsJunkLog(line string) bool {
	for _, junk := range JunkSubstrings {
		if strings.Contains(line, junk) {
			return true
		}
	}
	return false
}

func processFile(path string, removedCount *int64) error {
	inputFile, err := os.Open(path)
	if err != nil {
		return fmt.Errorf("opening file %s: %w", path, err)
	}
	defer inputFile.Close()

	info, err := inputFile.Stat()
	if err != nil {
		return fmt.Errorf("getting stat of file %s: %w", path, err)
	}

	statT, ok := info.Sys().(*syscall.Stat_t)
	if !ok {
		return fmt.Errorf("failed to retrieve underlying stat from file %s", path)
	}

	dir := filepath.Dir(path)
	tmpFile, err := os.CreateTemp(dir, "clean_")
	if err != nil {
		return fmt.Errorf("creating temp file in %s: %w", dir, err)
	}

	tmpName := tmpFile.Name()
	defer tmpFile.Close()

	scanner := bufio.NewScanner(inputFile)
	buf := make([]byte, 0, 1024*1024)
	scanner.Buffer(buf, 10*1024*1024)
	writer := bufio.NewWriterSize(tmpFile, 256*1024)

	var removedInFile int64
	for scanner.Scan() {
		line := scanner.Bytes()
		if IsJunkLog(string(line)) {
			removedInFile++
		} else {
			if _, err := writer.Write(line); err != nil {
				os.Remove(tmpName)
				return fmt.Errorf("writing to temp file for %s: %w", path, err)
			}
			if err := writer.WriteByte('\n'); err != nil {
				os.Remove(tmpName)
				return fmt.Errorf("writing newline for %s: %w", path, err)
			}
		}
	}
	if err := scanner.Err(); err != nil {
		os.Remove(tmpName)
		return fmt.Errorf("scanning file %s: %w", path, err)
	}
	if err := writer.Flush(); err != nil {
		os.Remove(tmpName)
		return fmt.Errorf("flushing writer for %s: %w", path, err)
	}

	if removedInFile == 0 {
		tmpFile.Close()
		os.Remove(tmpName)
		return nil
	}

	if err := os.Chmod(tmpName, info.Mode()); err != nil {
		return fmt.Errorf("setting permissions on temp file for %s: %w", path, err)
	}
	if err := os.Chown(tmpName, int(statT.Uid), int(statT.Gid)); err != nil {
		return fmt.Errorf("setting ownership on temp file for %s: %w", path, err)
	}
	origAccessTime := time.Unix(statT.Atim.Sec, statT.Atim.Nsec)
	if err := os.Chtimes(tmpName, origAccessTime, info.ModTime()); err != nil {
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

	for i := 0; i < numWorkers; i++ {
		wg.Add(1)
		go worker()
	}

	entries, err := os.ReadDir(rootDir)
	if err != nil {
		return 0, err
	}

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
