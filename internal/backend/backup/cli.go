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
)

func processFile(path string, removedCount *int64) error {
	inputFile, err := os.Open(path)
	if err != nil {
		return fmt.Errorf("opening file %s: %w", path, err)
	}
	defer inputFile.Close()

	dir := filepath.Dir(path)
	tmpFile, err := os.CreateTemp(dir, "clean_")
	if err != nil {
		return fmt.Errorf("creating temp file in %s: %w", dir, err)
	}

	tmpName := tmpFile.Name()
	defer tmpFile.Close()

	scanner := bufio.NewScanner(inputFile)
	writer := bufio.NewWriter(tmpFile)

	// local accumulator for this file
	var removedInFile int64
	for scanner.Scan() {
		line := scanner.Text()
		if isJunkLog(line) {
			// count the removed junk log line and skip writing it
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

	// global atomic counter for removed lines
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

	err := filepath.WalkDir(rootDir, func(path string, d os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.Type().IsRegular() {
			fileCh <- path
		}
		return nil
	})

	close(fileCh)
	wg.Wait()

	if err != nil {
		return totalRemoved, err
	}

	return totalRemoved, finalErr
}
