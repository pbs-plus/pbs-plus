//go:build linux

package application

import "github.com/pbs-plus/pbs-plus/internal/store/database"

// ScriptService encapsulates script management logic.
type ScriptService struct {
	db *database.Database
}

func NewScriptService(db *database.Database) *ScriptService {
	return &ScriptService{db: db}
}

func (s *ScriptService) List() ([]database.Script, error) {
	return s.db.GetAllScripts()
}

func (s *ScriptService) Get(path string) (database.Script, error) {
	return s.db.GetScript(path)
}

func (s *ScriptService) Create(script database.Script) error {
	return s.db.CreateScript(nil, script)
}

func (s *ScriptService) Update(script database.Script) error {
	return s.db.UpdateScript(nil, script)
}

func (s *ScriptService) Delete(path string) error {
	return s.db.DeleteScript(nil, path)
}
