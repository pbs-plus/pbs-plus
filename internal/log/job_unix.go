//go:build unix

package log

import (
	"bufio"
	"bytes"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/pbs-plus/pbs-plus/internal/safemap"
	"github.com/pbs-plus/pbs-plus/internal/validate"
)

type jobLogger struct {
	*os.File
	path      string
	jobID     string
	writer    *bufio.Writer
	startTime time.Time

	mu sync.Mutex
}

var jobLoggers = safemap.New[string, *jobLogger]()

func safeJobLogPath(jobID string) (string, error) {
	if err := validate.ValidateJobId(jobID); err != nil {
		return "", fmt.Errorf("invalid jobID: %w", err)
	}

	tempDir := os.TempDir()
	fileName := fmt.Sprintf("job-%s-stdout", jobID)
	filePath := filepath.Join(tempDir, fileName)

	cleanPath := filepath.Clean(filePath)
	if !strings.HasPrefix(cleanPath, filepath.Clean(tempDir)) {
		return "", errors.New("path traversal detected")
	}

	return cleanPath, nil
}

func newJobLogger(jobID string) *jobLogger {
	filePath, err := safeJobLogPath(jobID)
	if err != nil {
		return nil
	}

	logger, _ := jobLoggers.GetOrCompute(jobID, func() *jobLogger {
		clientLogFile, createErr := os.Create(filePath)
		if createErr != nil {
			return nil
		}

		return &jobLogger{
			File:      clientLogFile,
			path:      filePath,
			jobID:     jobID,
			writer:    bufio.NewWriter(clientLogFile),
			startTime: time.Now(),
		}
	})

	return logger
}

func getExistingJobLogger(jobID string) *jobLogger {
	filePath, err := safeJobLogPath(jobID)
	if err != nil {
		return nil
	}

	logger, _ := jobLoggers.GetOrCompute(jobID, func() *jobLogger {
		flags := os.O_WRONLY | os.O_CREATE | os.O_APPEND
		perm := os.FileMode(0666)

		clientLogFile, openErr := os.OpenFile(filePath, flags, perm)
		if openErr != nil {
			return nil
		}

		return &jobLogger{
			File:      clientLogFile,
			path:      filePath,
			jobID:     jobID,
			writer:    bufio.NewWriter(clientLogFile),
			startTime: time.Now(),
		}
	})

	return logger
}

func (b *jobLogger) Write(in []byte) (n int, err error) {
	b.mu.Lock()
	defer b.mu.Unlock()

	bytesConsumedFromInput := len(in)

	scanner := bufio.NewScanner(bytes.NewReader(in))
	var sb strings.Builder

	hasContent := false
	for scanner.Scan() {
		hasContent = true
		line := scanner.Text()
		timestamp := time.Now().Format(time.RFC3339)
		_, formatErr := fmt.Fprintf(&sb, "%s: %s\n", timestamp, line)
		if formatErr != nil {
			return 0, fmt.Errorf("error formatting log line: %w", formatErr)
		}
	}

	if scanErr := scanner.Err(); scanErr != nil {
		return 0, fmt.Errorf("error scanning input for lines: %w", scanErr)
	}

	if hasContent || (len(in) > 0 && sb.Len() == 0) {
		formattedLogMessage := sb.String()
		if len(formattedLogMessage) > 0 {
			if b.writer == nil {
				return 0, errors.New("logger closed")
			}
			_, writeErr := b.writer.WriteString(formattedLogMessage)
			if writeErr != nil {
				return 0, fmt.Errorf(
					"error writing formatted message to logger's internal buffer: %w",
					writeErr,
				)
			}

			flushErr := b.writer.Flush()
			if flushErr != nil {
				return 0, fmt.Errorf(
					"error flushing logger buffer to file: %w",
					flushErr,
				)
			}
		}
	}
	return bytesConsumedFromInput, nil
}

func (b *jobLogger) Flush() error {
	b.mu.Lock()
	defer b.mu.Unlock()

	return b.writer.Flush()
}

func (b *jobLogger) Close() error {
	b.mu.Lock()
	defer b.mu.Unlock()

	var multiError []string

	if b.writer != nil {
		if err := b.writer.Flush(); err != nil {
			multiError = append(multiError, fmt.Sprintf("flush error: %v", err))
		}
	}

	if b.File != nil {
		if err := b.File.Close(); err != nil {
			multiError = append(multiError, fmt.Sprintf("file close error: %v", err))
		}
		b.File = nil
		b.writer = nil
	}

	jobLoggers.Del(b.jobID)

	if b.path != "" {
		if strings.HasPrefix(filepath.Clean(b.path), filepath.Clean(os.TempDir())) {
			if err := os.RemoveAll(b.path); err != nil {
				multiError = append(multiError, fmt.Sprintf("file remove error (%s): %v", b.path, err))
			}
		} else {
			multiError = append(multiError, fmt.Sprintf("refusing to delete file outside temp directory: %s", b.path))
		}
	}

	if len(multiError) > 0 {
		return errors.New(strings.Join(multiError, "; "))
	}
	return nil
}

func (b *jobLogger) Path() string {
	return b.path
}

func (l *Logger) ensureJobLogger() {
	if l.jobID == "" {
		return
	}
	if l.core().Server {
		newJobLogger(l.jobID)
	}
}

func (l *Logger) JobStdoutWriter() io.Writer {
	if l.jobID == "" {
		return nil
	}
	return getExistingJobLogger(l.jobID)
}

func (l *Logger) closeJobLogger() {
	if l.jobID == "" {
		return
	}
	if jl := getExistingJobLogger(l.jobID); jl != nil {
		_ = jl.Close()
	}
}
