//go:build linux

package application

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/pbs-plus/pbs-plus/internal/server/coredb"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func setupTestStore(t *testing.T) *Runtime {
	t.Helper()

	dir, err := os.MkdirTemp("", "pbs-plus-test-*")
	require.NoError(t, err)

	dbPath := filepath.Join(dir, "test.db")
	paths := map[string]string{
		"sqlite": dbPath,
	}

	store, err := New(t.Context(), paths)
	require.NoError(t, err)

	t.Cleanup(func() {
		_ = store.Close()
		os.RemoveAll(dir)
	})

	return store
}

// Backup Tests
func TestBackupCRUD(t *testing.T) {
	store := setupTestStore(t)

	t.Run("Basic CRUD Operations", func(t *testing.T) {
		backup := coredb.Backup{
			ID:               "test-backup-1",
			Store:            "local",
			Target:           coredb.Target{Name: "test-target"},
			Subpath:          "backups/test",
			Schedule:         "daily",
			Comment:          "Test backup backup",
			NotificationMode: "always",
			Namespace:        "test",
		}

		err := store.CoreDB.CreateBackup(nil, backup)
		assert.NoError(t, err)

		// Test Get
		retrievedBackup, err := store.CoreDB.GetBackup(backup.ID)
		assert.NoError(t, err)
		assert.NotNil(t, retrievedBackup)
		assert.Equal(t, backup.ID, retrievedBackup.ID)
		assert.Equal(t, backup.Store, retrievedBackup.Store)
		assert.Equal(t, backup.Target, retrievedBackup.Target)

		// Test Update
		backup.Comment = "Updated comment"
		err = store.CoreDB.UpdateBackup(nil, backup)
		assert.NoError(t, err)

		updatedBackup, err := store.CoreDB.GetBackup(backup.ID)
		assert.NoError(t, err)
		assert.Equal(t, "Updated comment", updatedBackup.Comment)

		// Test GetAll
		backups, err := store.CoreDB.GetAllBackups()
		assert.NoError(t, err)
		assert.Len(t, backups, 1)

		// Test Delete
		err = store.CoreDB.DeleteBackup(nil, backup.ID)
		assert.NoError(t, err)

		_, err = store.CoreDB.GetBackup(backup.ID)
		assert.ErrorIs(t, err, coredb.ErrBackupNotFound)
	})

	t.Run("Concurrent Operations", func(t *testing.T) {
		var wg sync.WaitGroup
		backupCount := 10

		// Concurrent creation
		for i := range backupCount {
			wg.Add(1)
			go func(idx int) {
				defer wg.Done()
				backup := coredb.Backup{
					ID:               fmt.Sprintf("concurrent-backup-%d", idx),
					Store:            "local",
					Target:           coredb.Target{Name: "test-target"},
					Subpath:          fmt.Sprintf("backups/test-%d", idx),
					Schedule:         `mon..fri *-*-* 00:00:00`,
					Comment:          fmt.Sprintf("Concurrent test backup %d", idx),
					NotificationMode: "always",
					Namespace:        "test",
				}
				err := store.CoreDB.CreateBackup(nil, backup)
				assert.NoError(t, err)
			}(i)
		}
		wg.Wait()

		// Verify all backups were created
		backups, err := store.CoreDB.GetAllBackups()
		assert.NoError(t, err)
		assert.Len(t, backups, backupCount)
	})

	t.Run("Special Characters", func(t *testing.T) {
		backup := coredb.Backup{
			ID:               "test-backup-special-!@#$%^",
			Store:            "local",
			Target:           coredb.Target{Name: "test-target"},
			Subpath:          "backups/test/special/!@#$%^",
			Schedule:         `mon..fri *-*-* 00:00:00`,
			Comment:          "Test backup with special characters !@#$%^",
			NotificationMode: "always",
			Namespace:        "test",
		}
		err := store.CoreDB.CreateBackup(nil, backup)
		assert.Error(t, err) // Should reject special characters
	})
}

func TestBackupValidation(t *testing.T) {
	store := setupTestStore(t)

	tests := []struct {
		name    string
		backup  coredb.Backup
		wantErr bool
		errMsg  string
	}{
		{
			name: "valid backup with all fields",
			backup: coredb.Backup{
				ID:               "test-valid",
				Store:            "local",
				Target:           coredb.Target{Name: "test"},
				Subpath:          "valid/path",
				Schedule:         `*-*-* 00:00:00`,
				Comment:          "Valid test backup",
				NotificationMode: "always",
				Namespace:        "test",
			},
			wantErr: false,
		},
		{
			name: "invalid schedule string",
			backup: coredb.Backup{
				ID:        "test-invalid-cron",
				Store:     "local",
				Target:    coredb.Target{Name: "test"},
				Schedule:  "invalid-cron",
				Namespace: "test",
			},
			wantErr: true,
			errMsg:  "invalid schedule string",
		},
		{
			name: "empty required fields",
			backup: coredb.Backup{
				ID: "test-empty",
			},
			wantErr: true,
			errMsg:  "is empty",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := store.CoreDB.CreateBackup(nil, tt.backup)
			if tt.wantErr {
				assert.Error(t, err)
				if tt.errMsg != "" && err != nil {
					assert.Contains(t, err.Error(), tt.errMsg)
				}
			} else {
				assert.NoError(t, err)
			}
		})
	}
}

func TestTargetValidation(t *testing.T) {
	store := setupTestStore(t)

	tests := []struct {
		name    string
		target  coredb.Target
		wantErr bool
		errMsg  string
	}{
		{
			name: "valid local target",
			target: coredb.Target{
				Name: ("local-target"),
				Path: ("/valid/path"),
			},
			wantErr: false,
		},
		{
			name: "valid agent target",
			target: coredb.Target{
				Name: ("agent-target"),
				Path: ("agent://192.168.1.100/C"),
			},
			wantErr: false,
		},
		{
			name: "invalid agent URL",
			target: coredb.Target{
				Name: ("invalid-agent"),
				Path: ("agent:/invalid-url"),
			},
			wantErr: true,
			errMsg:  "invalid target path",
		},
		{
			name: "empty path",
			target: coredb.Target{
				Name: ("empty-path"),
				Path: (""),
			},
			wantErr: true,
			errMsg:  "empty",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := store.CoreDB.CreateTarget(nil, tt.target)
			if tt.wantErr {
				assert.Error(t, err)
				if tt.errMsg != "" && err != nil {
					assert.Contains(t, err.Error(), tt.errMsg)
				}
			} else {
				assert.NoError(t, err)
			}
		})
	}
}

func TestExclusionPatternValidation(t *testing.T) {
	store := setupTestStore(t)

	tests := []struct {
		name      string
		exclusion coredb.Exclusion
		wantErr   bool
	}{
		{
			name: "valid glob pattern",
			exclusion: coredb.Exclusion{
				Path:    "*.tmp",
				Comment: "Temporary files",
			},
			wantErr: false,
		},
		{
			name: "valid regex pattern",
			exclusion: coredb.Exclusion{
				Path:    "^.*\\.bak$",
				Comment: "Backup files",
			},
			wantErr: false,
		},
		{
			name: "invalid pattern syntax",
			exclusion: coredb.Exclusion{
				Path:    "[invalid[pattern",
				Comment: "Invalid pattern",
			},
			wantErr: true,
		},
		{
			name: "empty pattern",
			exclusion: coredb.Exclusion{
				Path:    "",
				Comment: "Empty pattern",
			},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := store.CoreDB.CreateExclusion(nil, tt.exclusion)
			if tt.wantErr {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
			}
		})
	}
}

func TestConcurrentOperations(t *testing.T) {
	store := setupTestStore(t)
	var wg sync.WaitGroup

	t.Run("Concurrent Target Operations", func(t *testing.T) {
		targetCount := 10
		for i := range targetCount {
			wg.Add(1)
			go func(idx int) {
				defer wg.Done()
				target := coredb.Target{
					Name: (fmt.Sprintf("concurrent-target-%d", idx)),
					Path: (fmt.Sprintf("/path/to/target-%d", idx)),
				}
				err := store.CoreDB.CreateTarget(nil, target)
				assert.NoError(t, err)
			}(i)
		}
		wg.Wait()

		// Verify all targets were created
		targets, err := store.CoreDB.GetAllTargets()
		assert.NoError(t, err)
		assert.Len(t, targets, targetCount)
	})

	t.Run("Concurrent Read/Write Operations", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()

		const opCount = 100
		readyCh := make(chan struct{})
		doneCh := make(chan struct{})

		// Writer goroutine
		go func() {
			<-readyCh
			for i := range opCount {
				select {
				case <-ctx.Done():
					return
				default:
					target := coredb.Target{
						Name: fmt.Sprintf("concurrent-target-%d", i),
						Path: fmt.Sprintf("/path/to/target-%d", i),
					}
					_ = store.CoreDB.CreateTarget(nil, target)
				}
			}
			doneCh <- struct{}{}
		}()

		// Reader goroutine
		go func() {
			<-readyCh
			for range opCount {
				select {
				case <-ctx.Done():
					return
				default:
					_, _ = store.CoreDB.GetAllTargets()
				}
			}
			doneCh <- struct{}{}
		}()

		close(readyCh)

		// Wait with timeout
		for range 2 {
			select {
			case <-doneCh:
				continue
			case <-ctx.Done():
				t.Fatal("Test timed out")
			}
		}
	})
}
