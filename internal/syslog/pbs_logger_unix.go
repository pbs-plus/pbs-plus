//go:build unix

package syslog

import (
	"bufio"
	"bytes"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/puzpuzpuz/xsync/v3"
)

type BackupLogger struct {
	*os.File
	Path      string
	jobId     string
	Writer    *bufio.Writer
	StartTime time.Time

	sync.Mutex
}

var backupLoggers = xsync.NewMapOf[string, *BackupLogger]()

func CreateBackupLogger(jobId string) *BackupLogger {
	logger, _ := backupLoggers.Compute(jobId, func(_ *BackupLogger, _ bool) (*BackupLogger, bool) {
		tempDir := os.TempDir()
		fileName := fmt.Sprintf("backup-%s-stdout", jobId)
		filePath := filepath.Join(tempDir, fileName)

		clientLogFile, err := os.Create(filePath)
		if err != nil {
			return nil, true
		}

		return &BackupLogger{
			File:      clientLogFile,
			Path:      filePath,
			jobId:     jobId,
			Writer:    bufio.NewWriter(clientLogFile),
			StartTime: time.Now(),
		}, false
	})

	return logger
}

func GetExistingBackupLogger(jobId string) *BackupLogger {
	logger, _ := backupLoggers.LoadOrCompute(jobId, func() *BackupLogger {
		tempDir := os.TempDir()
		fileName := fmt.Sprintf("backup-%s-stdout", jobId)
		filePath := filepath.Join(tempDir, fileName)

		flags := os.O_WRONLY | os.O_CREATE | os.O_APPEND
		perm := os.FileMode(0666)

		clientLogFile, err := os.OpenFile(filePath, flags, perm)
		if err != nil {
			return nil
		}

		return &BackupLogger{
			File:      clientLogFile,
			Path:      filePath,
			jobId:     jobId,
			Writer:    bufio.NewWriter(clientLogFile),
			StartTime: time.Now(),
		}
	})
	return logger
}

func (b *BackupLogger) Write(in []byte) (n int, err error) {
	b.Lock()
	defer b.Unlock()

	bytesConsumedFromInput := len(in)

	scanner := bufio.NewScanner(bytes.NewReader(in))
	var stringBuilder strings.Builder

	hasContent := false
	for scanner.Scan() {
		hasContent = true
		line := scanner.Text()
		timestamp := time.Now().Format(time.RFC3339)
		_, formatErr := fmt.Fprintf(&stringBuilder, "%s: %s\n", timestamp, line)
		if formatErr != nil {
			return 0, fmt.Errorf("error formatting log line: %w", formatErr)
		}
	}

	if scanErr := scanner.Err(); scanErr != nil {
		return 0, fmt.Errorf("error scanning input for lines: %w", scanErr)
	}

	// Only write to the internal buffer if there was actual content processed.
	// This avoids writing empty strings if `in` was empty or only whitespace
	// that scanner might skip (though default scanner processes empty lines).
	if hasContent || (len(in) > 0 && stringBuilder.Len() == 0) {
		// The second condition (len(in) > 0 && stringBuilder.Len() == 0)
		// handles cases like `in` being just "\n". Scanner produces one empty line,
		// which Fprintf formats as "timestamp: \n". So `stringBuilder.Len()` would be > 0.
		// If `in` is `[]byte{}`, `hasContent` is false, `stringBuilder` is empty. Nothing written.
		// This logic ensures that if `in` was not empty, we attempt to write *something*
		// (even if it's just timestamped newlines).

		formattedLogMessage := stringBuilder.String()
		if len(formattedLogMessage) > 0 { // Ensure we actually have something to write
			if b.Writer == nil {
				return 0, errors.New("logger closed")
			}
			_, writeErr := b.Writer.WriteString(formattedLogMessage)
			if writeErr != nil {
				// Don't attempt to flush if WriteString itself failed.
				return 0, fmt.Errorf(
					"error writing formatted message to logger's internal buffer: %w",
					writeErr,
				)
			}

			flushErr := b.Writer.Flush()
			if flushErr != nil {
				// Data made it to buffer but not to disk.
				// Return 0 because the write operation to the final destination failed.
				return 0, fmt.Errorf(
					"error flushing logger buffer to file: %w",
					flushErr,
				)
			}
		}
	}

	// If all operations were successful (or if `in` was empty and nothing needed to be written),
	// we report that we have processed all bytes from the input slice `in`.
	return bytesConsumedFromInput, nil
}

func (b *BackupLogger) Flush() error {
	b.Lock()
	defer b.Unlock()

	return b.Writer.Flush()
}

func (b *BackupLogger) Close() error {
	b.Lock()
	defer b.Unlock()

	var multiError []string

	if b.Writer != nil {
		if err := b.Writer.Flush(); err != nil {
			multiError = append(multiError, fmt.Sprintf("flush error: %v", err))
		}
	}

	if b.File != nil {
		if err := b.File.Close(); err != nil {
			multiError = append(multiError, fmt.Sprintf("file close error: %v", err))
		}
		// Mark as nil so subsequent calls to Write/Flush/Close on this instance might fail clearly
		b.File = nil
		b.Writer = nil
	}

	// Always try to remove from map and delete file
	backupLoggers.Delete(b.jobId)

	if b.Path != "" {
		if err := os.RemoveAll(b.Path); err != nil {
			multiError = append(multiError, fmt.Sprintf("file remove error (%s): %v", b.Path, err))
		}
	}

	if len(multiError) > 0 {
		return errors.New(strings.Join(multiError, "; "))
	}
	return nil
}
