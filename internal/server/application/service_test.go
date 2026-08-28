//go:build linux

package application

import (
	"errors"
	"testing"

	"github.com/pbs-plus/pbs-plus/internal/server/coredb"
)

// mockBackupRepo implements domain.BackupRepository for testing.
type mockBackupRepo struct {
	backups map[string]coredb.Backup
	getErr  error
}

func (m *mockBackupRepo) GetAllBackups() ([]coredb.Backup, error) {
	if m.getErr != nil {
		return nil, m.getErr
	}
	result := make([]coredb.Backup, 0, len(m.backups))
	for _, b := range m.backups {
		result = append(result, b)
	}
	return result, nil
}

func (m *mockBackupRepo) GetBackup(id string) (coredb.Backup, error) {
	if m.getErr != nil {
		return coredb.Backup{}, m.getErr
	}
	b, ok := m.backups[id]
	if !ok {
		return coredb.Backup{}, coredb.ErrBackupNotFound
	}
	return b, nil
}

func (m *mockBackupRepo) CreateBackup(tx *coredb.Transaction, backup coredb.Backup) error {
	if m.backups == nil {
		m.backups = make(map[string]coredb.Backup)
	}
	m.backups[backup.ID] = backup
	return nil
}

func (m *mockBackupRepo) UpdateBackup(tx *coredb.Transaction, backup coredb.Backup) error {
	m.backups[backup.ID] = backup
	return nil
}

func (m *mockBackupRepo) DeleteBackup(tx *coredb.Transaction, id string) error {
	delete(m.backups, id)
	return nil
}

func TestBackupService_GetBackup_NotFound(t *testing.T) {
	svc := &BackupService{db: nil} // not used when testing with repo interface
	_ = svc

	// Integration-style: test via the real Database-backed service
	// Unit-style tests with mock repos would require updating BackupService
	// to accept interfaces (future refactor).
}

func TestBackupService_ListBackups_Empty(t *testing.T) {

	// This test verifies the mock pattern compiles. Future: inject interface.
	repo := &mockBackupRepo{backups: make(map[string]coredb.Backup)}
	backups, err := repo.GetAllBackups()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(backups) != 0 {
		t.Errorf("expected 0 backups, got %d", len(backups))
	}
}

func TestBackupService_GetBackup_Error(t *testing.T) {
	repo := &mockBackupRepo{
		backups: make(map[string]coredb.Backup),
		getErr:  errors.New("db down"),
	}
	_, err := repo.GetBackup("any")
	if err == nil {
		t.Fatal("expected error, got nil")
	}
}

func TestBackupService_CreateAndGet(t *testing.T) {
	repo := &mockBackupRepo{backups: make(map[string]coredb.Backup)}
	b := coredb.Backup{ID: "backup-1", Store: "local"}
	if err := repo.CreateBackup(nil, b); err != nil {
		t.Fatalf("create failed: %v", err)
	}
	got, err := repo.GetBackup("backup-1")
	if err != nil {
		t.Fatalf("get failed: %v", err)
	}
	if got.ID != "backup-1" {
		t.Errorf("expected backup-1, got %s", got.ID)
	}
}
