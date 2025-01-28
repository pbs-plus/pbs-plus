//go:build windows

package agent

import (
	"sync"
)

// BackupStatus manages the state of ongoing backups
type BackupStatus struct {
	activeBackups sync.Map
	mu            sync.RWMutex
}

var (
	backupStatus     *BackupStatus
	backupStatusOnce sync.Once
)

// GetBackupStatus returns the singleton instance of BackupStatus
func GetBackupStatus() *BackupStatus {
	backupStatusOnce.Do(func() {
		backupStatus = &BackupStatus{}
	})
	return backupStatus
}

// StartBackup marks a backup as started for a given drive
func (bs *BackupStatus) StartBackup(drive string) {
	bs.activeBackups.Store(drive, true)
}

// EndBackup marks a backup as completed for a given drive
func (bs *BackupStatus) EndBackup(drive string) {
	bs.activeBackups.Delete(drive)
}

// HasActiveBackups checks if there are any ongoing backups
func (bs *BackupStatus) HasActiveBackups() bool {
	hasActive := false
	bs.activeBackups.Range(func(_, _ interface{}) bool {
		hasActive = true
		return false // Stop iteration as soon as we find one active backup
	})
	return hasActive
}
