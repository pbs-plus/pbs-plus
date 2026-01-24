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

	"github.com/pbs-plus/pbs-plus/internal/utils"
	"github.com/puzpuzpuz/xsync/v3"
)

type JobLogger struct {
	*os.File
	Path      string
	jobId     string
	Writer    *bufio.Writer
	StartTime time.Time

	sync.Mutex
}

var jobLoggers = xsync.NewMapOf[string, *JobLogger]()

func safeJobLogPath(jobId string) (string, error) {
	if err := utils.ValidateJobId(jobId); err != nil {
		return "", fmt.Errorf("invalid jobId: %w", err)
	}

	tempDir := os.TempDir()
	fileName := fmt.Sprintf("job-%s-stdout", jobId)
	filePath := filepath.Join(tempDir, fileName)

	cleanPath := filepath.Clean(filePath)
	if !strings.HasPrefix(cleanPath, filepath.Clean(tempDir)) {
		return "", errors.New("path traversal detected")
	}

	return cleanPath, nil
}

func CreateJobLogger(jobId string) (*JobLogger, error) {
	filePath, err := safeJobLogPath(jobId)
	if err != nil {
		return nil, err
	}

	logger, _ := jobLoggers.Compute(jobId, func(_ *JobLogger, _ bool) (*JobLogger, bool) {
		clientLogFile, createErr := os.Create(filePath)
		if createErr != nil {
			return nil, true
		}

		return &JobLogger{
			File:      clientLogFile,
			Path:      filePath,
			jobId:     jobId,
			Writer:    bufio.NewWriter(clientLogFile),
			StartTime: time.Now(),
		}, false
	})

	if logger == nil {
		return nil, errors.New("failed to create job logger")
	}

	return logger, nil
}

func GetExistingJobLogger(jobId string) *JobLogger {
	filePath, err := safeJobLogPath(jobId)
	if err != nil {
		return nil
	}

	logger, _ := jobLoggers.LoadOrCompute(jobId, func() *JobLogger {
		flags := os.O_WRONLY | os.O_CREATE | os.O_APPEND
		perm := os.FileMode(0666)

		clientLogFile, openErr := os.OpenFile(filePath, flags, perm)
		if openErr != nil {
			return nil
		}

		return &JobLogger{
			File:      clientLogFile,
			Path:      filePath,
			jobId:     jobId,
			Writer:    bufio.NewWriter(clientLogFile),
			StartTime: time.Now(),
		}
	})

	if logger == nil {
		return nil
	}

	return logger
}

func (b *JobLogger) Write(in []byte) (n int, err error) {
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

	if hasContent || (len(in) > 0 && stringBuilder.Len() == 0) {
		formattedLogMessage := stringBuilder.String()
		if len(formattedLogMessage) > 0 {
			if b.Writer == nil {
				return 0, errors.New("logger closed")
			}
			_, writeErr := b.Writer.WriteString(formattedLogMessage)
			if writeErr != nil {
				return 0, fmt.Errorf(
					"error writing formatted message to logger's internal buffer: %w",
					writeErr,
				)
			}

			flushErr := b.Writer.Flush()
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

func (b *JobLogger) Flush() error {
	b.Lock()
	defer b.Unlock()

	return b.Writer.Flush()
}

func (b *JobLogger) Close() error {
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
		b.File = nil
		b.Writer = nil
	}

	jobLoggers.Delete(b.jobId)

	if b.Path != "" {
		if strings.HasPrefix(filepath.Clean(b.Path), filepath.Clean(os.TempDir())) {
			if err := os.RemoveAll(b.Path); err != nil {
				multiError = append(multiError, fmt.Sprintf("file remove error (%s): %v", b.Path, err))
			}
		} else {
			multiError = append(multiError, fmt.Sprintf("refusing to delete file outside temp directory: %s", b.Path))
		}
	}

	if len(multiError) > 0 {
		return errors.New(strings.Join(multiError, "; "))
	}
	return nil
}
