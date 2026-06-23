//go:build linux

package application

import (
	"github.com/pbs-plus/pbs-plus/internal/server/database"
)

type BackupRepository interface {
	GetAllBackups() ([]database.Backup, error)
	GetBackup(id string) (database.Backup, error)
	CreateBackup(tx *database.Transaction, backup database.Backup) error
	UpdateBackup(tx *database.Transaction, backup database.Backup) error
	DeleteBackup(tx *database.Transaction, id string) error
	GetAllQueuedBackups() ([]database.Backup, error)
}

type TargetRepository interface {
	GetAllTargets() ([]database.Target, error)
}
