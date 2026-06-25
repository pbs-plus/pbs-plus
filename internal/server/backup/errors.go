package backup

import "errors"

var (
	ErrPrepareBackupCommand               = errors.New("failed to prepare backup command")
	ErrTaskMonitoringInitializationFailed = errors.New("task monitoring initialization failed")
	ErrTaskMonitoringTimedOut             = errors.New("task monitoring initialization timed out")
	ErrProxmoxBackupClientStart           = errors.New("proxmox-backup-client start error")
	ErrTaskDetectionFailed                = errors.New("failed while waiting for backup to start")
)
