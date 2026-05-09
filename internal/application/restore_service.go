//go:build linux

package application

import "github.com/pbs-plus/pbs-plus/internal/store/database"

// RestoreService encapsulates restore job business logic.
type RestoreService struct {
	db *database.Database
}

// NewRestoreService creates a RestoreService.
func NewRestoreService(db *database.Database) *RestoreService {
	return &RestoreService{db: db}
}

// ListRestores returns all restore jobs.
func (s *RestoreService) ListRestores() ([]database.Restore, error) {
	return s.db.GetAllRestores()
}

// GetRestore returns a single restore job by ID.
func (s *RestoreService) GetRestore(id string) (database.Restore, error) {
	return s.db.GetRestore(id)
}

// CreateRestore creates a new restore job.
func (s *RestoreService) CreateRestore(restore database.Restore) error {
	return s.db.CreateRestore(nil, restore)
}

// UpdateRestore updates an existing restore job.
func (s *RestoreService) UpdateRestore(restore database.Restore) error {
	return s.db.UpdateRestore(nil, restore)
}

// DeleteRestore removes a restore job.
func (s *RestoreService) DeleteRestore(id string) error {
	return s.db.DeleteRestore(nil, id)
}
