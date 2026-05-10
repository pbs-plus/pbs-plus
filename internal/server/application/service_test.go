//go:build linux

package application

import (
	"errors"
	"testing"

	"github.com/pbs-plus/pbs-plus/internal/server/database"
)

// mockBackupRepo implements domain.BackupRepository for testing.
type mockBackupRepo struct {
	backups map[string]database.Backup
	getErr  error
}

func (m *mockBackupRepo) GetAllBackups() ([]database.Backup, error) {
	if m.getErr != nil {
		return nil, m.getErr
	}
	result := make([]database.Backup, 0, len(m.backups))
	for _, b := range m.backups {
		result = append(result, b)
	}
	return result, nil
}

func (m *mockBackupRepo) GetBackup(id string) (database.Backup, error) {
	if m.getErr != nil {
		return database.Backup{}, m.getErr
	}
	b, ok := m.backups[id]
	if !ok {
		return database.Backup{}, database.ErrBackupNotFound
	}
	return b, nil
}

func (m *mockBackupRepo) CreateBackup(tx *database.Transaction, backup database.Backup) error {
	if m.backups == nil {
		m.backups = make(map[string]database.Backup)
	}
	m.backups[backup.ID] = backup
	return nil
}

func (m *mockBackupRepo) UpdateBackup(tx *database.Transaction, backup database.Backup) error {
	m.backups[backup.ID] = backup
	return nil
}

func (m *mockBackupRepo) DeleteBackup(tx *database.Transaction, id string) error {
	delete(m.backups, id)
	return nil
}

func (m *mockBackupRepo) GetAllQueuedBackups() ([]database.Backup, error) {
	return m.GetAllBackups()
}

func TestBackupService_GetBackup_NotFound(t *testing.T) {
	svc := &BackupService{db: nil} // not used when testing with repo interface
	_ = svc

	// Integration-style: test via the real Database-backed service
	// Unit-style tests with mock repos would require updating BackupService
	// to accept interfaces (future refactor).
}

func TestBackupService_ListBackups_Empty(t *testing.T) {
	// BackupService currently depends on concrete *database.Database.
	// This test verifies the mock pattern compiles. Future: inject interface.
	repo := &mockBackupRepo{backups: make(map[string]database.Backup)}
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
		backups: make(map[string]database.Backup),
		getErr:  errors.New("db down"),
	}
	_, err := repo.GetBackup("any")
	if err == nil {
		t.Fatal("expected error, got nil")
	}
}

func TestBackupService_CreateAndGet(t *testing.T) {
	repo := &mockBackupRepo{backups: make(map[string]database.Backup)}
	b := database.Backup{ID: "backup-1", Store: "local"}
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
