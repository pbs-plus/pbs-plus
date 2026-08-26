package proxmox

import (
	"fmt"
	"os"
	"time"
)

// PBS's backup user (uid/gid 34 on Debian). Every file the
// proxmox-backup daemon must read or write - task logs, the task list,
// datastore dirs, owner files - belongs to it.
const (
	BackupUID = 34
	BackupGID = 34
)

// ChownBackupUser transfers a path to the backup user like PBS's
// CreateOptions::owner. Fatal when running as root; a no-op error for
// unprivileged dev/test runs.
func ChownBackupUser(path string) error {
	if err := os.Chown(path, BackupUID, BackupGID); err != nil && os.Geteuid() == 0 {
		return fmt.Errorf("chown %s to backup user: %w", path, err)
	}
	return nil
}

// FormatLogLine renders a PBS FileLogger line: "<rfc3339>: <msg>".
func FormatLogLine(t time.Time, msg string) string {
	return t.Format(time.RFC3339) + ": " + msg
}
