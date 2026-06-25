package backup

import (
	"errors"

	"github.com/pbs-plus/pbs-plus/internal/server/jobs"
)

var (
	ErrPrepareBackupCommand               = errors.New("failed to prepare backup command")
	ErrTaskMonitoringInitializationFailed = errors.New("task monitoring initialization failed")
	ErrTaskMonitoringTimedOut             = errors.New("task monitoring initialization timed out")
	ErrProxmoxBackupClientStart           = errors.New("proxmox-backup-client start error")
	ErrTaskDetectionFailed                = errors.New("failed while waiting for backup to start")
)

var (
	ErrMountEmpty        = jobs.ErrMountEmpty
	ErrCanceled          = jobs.ErrCanceled
	ErrUnexpected        = jobs.ErrUnexpected
	ErrTargetUnreachable = jobs.ErrTargetUnreachable
	ErrSubpathNotFound   = jobs.ErrSubpathNotFound
)
