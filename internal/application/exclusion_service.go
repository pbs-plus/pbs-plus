//go:build linux

package application

import "github.com/pbs-plus/pbs-plus/internal/store/database"

// ExclusionService encapsulates exclusion management logic.
type ExclusionService struct {
	db *database.Database
}

func NewExclusionService(db *database.Database) *ExclusionService {
	return &ExclusionService{db: db}
}

func (s *ExclusionService) ListGlobal() ([]database.Exclusion, error) {
	return s.db.GetAllGlobalExclusions()
}

func (s *ExclusionService) Get(path string) (*database.Exclusion, error) {
	return s.db.GetExclusion(path)
}

func (s *ExclusionService) Create(exclusion database.Exclusion) error {
	return s.db.CreateExclusion(nil, exclusion)
}

func (s *ExclusionService) Update(exclusion database.Exclusion) error {
	return s.db.UpdateExclusion(nil, exclusion)
}

func (s *ExclusionService) Delete(path string) error {
	return s.db.DeleteExclusion(nil, path)
}
