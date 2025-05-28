//go:build unix

package syslog

import (
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
	Path  string
	jobId string

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
			File:  clientLogFile,
			Path:  filePath,
			jobId: jobId,
		}
	})
	return logger
}

func (b *BackupLogger) Write(in []byte) (n int, err error) {
	b.Lock()
	defer b.Unlock()

	message := string(in)

	for _, line := range strings.Split(message, "\n") {
		timestamp := time.Now().Format(time.RFC3339)
		m, err := b.File.Write([]byte(fmt.Sprintf("%s: %s\n", timestamp, line)))
		if err != nil {
			return n, err
		}

		n += m
	}
	return n, err
}

func (b *BackupLogger) Close() error {
	b.Lock()
	defer b.Unlock()

	backupLoggers.Delete(b.jobId)
	_ = b.File.Close()
	return os.RemoveAll(b.File.Name())
}
