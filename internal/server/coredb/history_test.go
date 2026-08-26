//go:build linux

package coredb

import (
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/pbs-plus/pbs-plus/internal/conf"
	"github.com/pbs-plus/pbs-plus/internal/proxmox/tasklog"
)

func TestGetAllUPIDs_RemovesLegacyQueueTasks(t *testing.T) {
	oldBackupLogsPath, oldRestoreLogsPath := conf.BackupLogsBasePath, conf.RestoreLogsBasePath
	root := t.TempDir()
	conf.BackupLogsBasePath = filepath.Join(root, "backups")
	conf.RestoreLogsBasePath = filepath.Join(root, "restores")
	t.Cleanup(func() {
		conf.BackupLogsBasePath, conf.RestoreLogsBasePath = oldBackupLogsPath, oldRestoreLogsPath
	})

	queued := tasklog.NewTask("pbsplusgen-queue", "backup", "queued").UPID
	actual := tasklog.NewTask("pbsplus", "backup", "actual").UPID
	cases := []struct {
		name string
		path string
		list func() []Tasks
	}{
		{
			name: "backup",
			path: filepath.Join(conf.BackupLogsBasePath, "backup"),
			list: func() []Tasks { return (&Backup{ID: "backup"}).GetAllUPIDs() },
		},
		{
			name: "restore",
			path: filepath.Join(conf.RestoreLogsBasePath, "restore"),
			list: func() []Tasks { return (&Restore{ID: "restore"}).GetAllUPIDs() },
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if err := os.MkdirAll(tc.path, 0o755); err != nil {
				t.Fatal(err)
			}
			for _, upid := range []string{queued, actual} {
				if err := os.WriteFile(filepath.Join(tc.path, upid), nil, 0o600); err != nil {
					t.Fatal(err)
				}
			}

			got := tc.list()
			if len(got) != 1 || got[0].UPID != actual {
				t.Fatalf("GetAllUPIDs() = %#v, want only %q", got, actual)
			}
			if _, err := os.Stat(filepath.Join(tc.path, queued)); !errors.Is(err, os.ErrNotExist) {
				t.Fatalf("queued task link still exists: %v", err)
			}
		})
	}
}
