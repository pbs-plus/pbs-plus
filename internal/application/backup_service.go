//go:build linux

package application

import (
	"github.com/pbs-plus/pbs-plus/internal/backend/vfs"
	"github.com/pbs-plus/pbs-plus/internal/store/database"
	"github.com/pbs-plus/pbs-plus/internal/backend/vfs/sessions"
)

// BackupService encapsulates backup business logic between the HTTP layer
// and the persistence layer. Handlers delegate to this service instead of
// calling storeInstance.Database directly.
type BackupService struct {
	db *database.Database
}

// NewBackupService creates a BackupService backed by the given database.
func NewBackupService(db *database.Database) *BackupService {
	return &BackupService{db: db}
}

// ListBackups returns all backups with live VFS stats enriched.
func (s *BackupService) ListBackups() ([]database.Backup, error) {
	backups, err := s.db.GetAllBackups()
	if err != nil {
		return nil, err
	}

	for i, backup := range backups {
		switch backup.Target.Type {
		case database.TargetTypeAgent:
			session := sessions.GetSessionARPCFS(backup.GetStreamID())
			if session == nil {
				continue
			}
			backups[i].CurrentStats = jobStatsFromVFS(session.GetStats())
		case database.TargetTypeS3:
			session := sessions.GetSessionS3FS(backup.GetStreamID())
			if session == nil {
				continue
			}
			backups[i].CurrentStats = jobStatsFromVFS(session.GetStats())
		}
	}

	return backups, nil
}

// GetBackup returns a single backup by ID.
func (s *BackupService) GetBackup(id string) (database.Backup, error) {
	return s.db.GetBackup(id)
}

// CreateBackup creates a new backup job.
func (s *BackupService) CreateBackup(backup database.Backup) error {
	return s.db.CreateBackup(nil, backup)
}

// UpdateBackup updates an existing backup job.
func (s *BackupService) UpdateBackup(backup database.Backup) error {
	return s.db.UpdateBackup(nil, backup)
}

// DeleteBackup removes a backup job.
func (s *BackupService) DeleteBackup(id string) error {
	return s.db.DeleteBackup(nil, id)
}

// GetAllQueuedBackups returns all backups currently in the queue.
func (s *BackupService) GetAllQueuedBackups() ([]database.Backup, error) {
	return s.db.GetAllQueuedBackups()
}

func jobStatsFromVFS(stats vfs.VFSStats) database.JobStats {
	return database.JobStats{
		CurrentFileCount:   int(stats.FilesAccessed),
		CurrentFolderCount: int(stats.FoldersAccessed),
		CurrentBytesTotal:  int(stats.TotalBytes),
		CurrentBytesSpeed:  int(stats.ByteReadSpeed),
		CurrentFilesSpeed:  int(stats.FileAccessSpeed),
		StatCacheHits:      int(stats.StatCacheHits),
	}
}
