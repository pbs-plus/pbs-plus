//go:build linux

package syslog

import (
	"fmt"
	"os"
	"sync"
	"sync/atomic"

	"github.com/puzpuzpuz/xsync/v3"
)

type BackupLogger struct {
	*os.File
	Path  string
	jobId string
	Count atomic.Uint32

	sync.RWMutex
}

var backupLoggers = xsync.NewMapOf[string, *BackupLogger]()

func GetOrCreateBackupLogger(jobId string) *BackupLogger {
	logger, _ := backupLoggers.LoadOrCompute(jobId, func() *BackupLogger {
		clientLogFile, err := os.CreateTemp("", fmt.Sprintf("backup-%s-stdout-*", jobId))
		if err != nil {
			return nil
		}
		clientLogPath := clientLogFile.Name()
		return &BackupLogger{
			File:  clientLogFile,
			Path:  clientLogPath,
			jobId: jobId,
		}
	})

	return logger
}

func GetExistingBackupLogger(jobId string) *BackupLogger {
	logger, _ := backupLoggers.Load(jobId)
	return logger
}

func (b *BackupLogger) Write(message string) {
	b.RLock()
	defer b.RUnlock()

	_, err := b.File.Write([]byte(message + "\n"))
	if err != nil {
		fmt.Printf("Failed to write to log: %v\n", err)
	}
	b.Count.Add(1)
}

func (b *BackupLogger) Close() error {
	b.Lock()
	defer b.Unlock()

	backupLoggers.Delete(b.jobId)
	_ = b.File.Close()
	return os.RemoveAll(b.File.Name())
}
