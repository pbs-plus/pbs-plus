//go:build linux

package application

import (
	"github.com/pbs-plus/pbs-plus/internal/server/coredb"
)

type BackupRepository interface {
	GetAllBackups() ([]coredb.Backup, error)
	GetBackup(id string) (coredb.Backup, error)
	CreateBackup(tx *coredb.Transaction, backup coredb.Backup) error
	UpdateBackup(tx *coredb.Transaction, backup coredb.Backup) error
	DeleteBackup(tx *coredb.Transaction, id string) error
}

type TargetRepository interface {
	GetAllTargets() ([]coredb.Target, error)
}
