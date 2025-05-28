//go:build unix

package syslog

import (
	"bufio"
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
	Path   string
	jobId  string
	Writer *bufio.Writer

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
			File:  clientLogFile,
			Path:  filePath,
			jobId: jobId,
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
			File:   clientLogFile,
			Path:   filePath,
			jobId:  jobId,
			Writer: bufio.NewWriter(clientLogFile),
		}
	})
	return logger
}

func (b *BackupLogger) Write(in []byte) (n int, err error) {
	b.Lock()
	defer b.Unlock()

	message := string(in)
	var stringBuilder strings.Builder

	for _, line := range strings.Split(message, "\n") {
		timestamp := time.Now().Format(time.RFC3339)
		_, formatErr := fmt.Fprintf(&stringBuilder, "%s: %s\n", timestamp, line)
		if formatErr != nil {
			return 0, fmt.Errorf("error formatting log line: %w", formatErr)
		}
	}
	formattedLogMessage := stringBuilder.String()

	bytesWrittenToBuffer, writeErr := b.Writer.WriteString(formattedLogMessage)
	if writeErr != nil {
		_ = b.Writer.Flush()
		return 0, fmt.Errorf(
			"error writing formatted message to logger's internal buffer: %w",
			writeErr,
		)
	}

	flushErr := b.Writer.Flush()
	if flushErr != nil {
		return bytesWrittenToBuffer, fmt.Errorf(
			"error flushing logger buffer to file: %w",
			flushErr,
		)
	}

	return bytesWrittenToBuffer, nil
}

func (b *BackupLogger) Close() error {
	b.Lock()
	defer b.Unlock()

	err := b.Writer.Flush() // Ensure anything remaining in the buffer is written
	if err != nil {
		// Handle flush error, but still attempt to close the file
		backupLoggers.Delete(b.jobId)
		closeErr := b.File.Close()
		if closeErr != nil {
			os.RemoveAll(b.File.Name())
			return fmt.Errorf("flush error: %w, close error: %v", err, closeErr)
		}
		os.RemoveAll(b.File.Name())
		return fmt.Errorf("flush error: %w", err)
	}

	_ = b.File.Close()

	backupLoggers.Delete(b.jobId)
	return os.RemoveAll(b.File.Name())
}
