//go:build linux

package backup

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"sort"
	"strings"
	"time"

	"golang.org/x/sys/unix"

	"github.com/pbs-plus/pbs-plus/internal/proxmox"
	"github.com/pbs-plus/pbs-plus/internal/proxmox/cli"
	"github.com/pbs-plus/pbs-plus/internal/server/application"
	"github.com/pbs-plus/pbs-plus/internal/server/coredb"
	"github.com/pbs-plus/pbs-plus/internal/validate"
)

const datastoreLocksDir = "/run/proxmox-backup/locks"

func migrateLegacyBackupGroup(backup coredb.Backup, app *application.Runtime) (bool, error) {
	legacyID, err := legacyBackupID(backup.Target)
	if err != nil {
		return false, err
	}
	legacyID = proxmox.NormalizeHostname(legacyID)

	info, err := cli.GetDatastoreInfo(backup.Store)
	if err != nil {
		return false, fmt.Errorf("get datastore %q: %w", backup.Store, err)
	}
	source := filepath.Join(backupNamespacePath(info.Path, backup.Namespace), "host", legacyID)
	if exists, err := pathExists(source); err != nil || !exists {
		return false, err
	}
	if chunks, err := pathExists(filepath.Join(info.Path, ".chunks")); err != nil {
		return false, err
	} else if !chunks {
		return false, fmt.Errorf("legacy group migration is supported only for filesystem datastores")
	}
	backups, err := app.CoreDB.GetAllBackups()
	if err != nil {
		return false, fmt.Errorf("list backups sharing legacy group: %w", err)
	}
	members, err := legacyGroupMembers(backups, backup, legacyID)
	if err != nil {
		return false, err
	}
	if len(members) > 1 {
		return false, fmt.Errorf("legacy group %q is shared by backup jobs %s and cannot be split automatically", source, strings.Join(members, ", "))
	}

	return migrateLegacyBackupGroupAt(backup, info.Path, datastoreLocksDir, legacyID)
}

func legacyGroupMembers(backups []coredb.Backup, current coredb.Backup, legacyID string) ([]string, error) {
	var members []string
	for _, backup := range backups {
		if backup.Store != current.Store || backup.Namespace != current.Namespace {
			continue
		}
		candidateID, err := legacyBackupID(backup.Target)
		if err != nil {
			return nil, err
		}
		if proxmox.NormalizeHostname(candidateID) == legacyID {
			members = append(members, backup.ID)
		}
	}
	sort.Strings(members)
	return members, nil
}

func legacyBackupID(target coredb.Target) (string, error) {
	if target.IsAgent() {
		if target.Name == "" {
			return "", fmt.Errorf("target name is required for agent backup")
		}
		return target.GetHostname(), nil
	}

	hostname, err := os.Hostname()
	if err == nil && hostname != "" {
		return hostname, nil
	}
	data, readErr := os.ReadFile("/etc/hostname")
	if readErr == nil && strings.TrimSpace(string(data)) != "" {
		return strings.TrimSpace(string(data)), nil
	}
	return "localhost", nil
}

func migrateLegacyBackupGroupAt(backup coredb.Backup, datastorePath, lockRoot, legacyID string) (bool, error) {
	backupID, err := getBackupId(backup)
	if err != nil {
		return false, err
	}
	if err := validate.ValidateJobId(legacyID); err != nil {
		return false, fmt.Errorf("invalid legacy backup ID: %w", err)
	}
	if legacyID == backupID {
		return false, nil
	}

	groupRoot := backupNamespacePath(datastorePath, backup.Namespace)
	source := filepath.Join(groupRoot, "host", legacyID)
	target := filepath.Join(groupRoot, "host", backupID)
	sourceExists, err := pathExists(source)
	if err != nil || !sourceExists {
		return false, err
	}
	targetExists, err := pathExists(target)
	if err != nil {
		return false, err
	}
	if targetExists {
		return false, fmt.Errorf("legacy group %q and per-job group %q both exist", source, target)
	}

	groupLocks, err := lockPBSPaths([]string{
		pbsLockPath(lockRoot, backup.Store, backup.Namespace, "host", legacyID),
		pbsLockPath(lockRoot, backup.Store, backup.Namespace, "host", backupID),
	})
	if err != nil {
		return false, fmt.Errorf("lock backup groups: %w", err)
	}
	defer closePBSLocks(groupLocks)

	if exists, err := pathExists(source); err != nil || !exists {
		return false, err
	}
	if exists, err := pathExists(target); err != nil {
		return false, err
	} else if exists {
		return false, fmt.Errorf("per-job group %q appeared during migration", target)
	}

	indices, snapshots, err := inspectLegacyGroup(source, target, proxmox.NormalizeHostname(backup.Target.Name))
	if err != nil {
		return false, err
	}
	snapshotLockPaths := make([]string, 0, len(snapshots))
	for _, snapshot := range snapshots {
		snapshotLockPaths = append(snapshotLockPaths,
			pbsLockPath(lockRoot, backup.Store, backup.Namespace, "host", legacyID, snapshot))
	}
	snapshotLocks, err := lockPBSPaths(snapshotLockPaths)
	if err != nil {
		return false, fmt.Errorf("lock legacy snapshots: %w", err)
	}
	defer closePBSLocks(snapshotLocks)

	if err := appendMoveJournal(filepath.Join(lockRoot, backup.Store, "move-journal"), indices); err != nil {
		return false, err
	}
	if err := unix.Renameat2(unix.AT_FDCWD, source, unix.AT_FDCWD, target, unix.RENAME_NOREPLACE); err != nil {
		return false, fmt.Errorf("rename legacy backup group: %w", err)
	}
	if dir, err := os.Open(filepath.Dir(target)); err == nil {
		_ = dir.Sync()
		_ = dir.Close()
	}

	return true, nil
}

func backupNamespacePath(datastorePath, namespace string) string {
	path := datastorePath
	for part := range strings.SplitSeq(namespace, "/") {
		if part != "" {
			path = filepath.Join(path, "ns", part)
		}
	}
	return path
}

func inspectLegacyGroup(source, target, archiveBase string) ([]string, []string, error) {
	entries, err := os.ReadDir(source)
	if err != nil {
		return nil, nil, fmt.Errorf("read legacy group: %w", err)
	}
	allowed := map[string]struct{}{
		archiveBase + ".pxar.didx":  {},
		archiveBase + ".mpxar.didx": {},
		archiveBase + ".ppxar.didx": {},
	}
	var indices []string
	var snapshots []string
	for _, entry := range entries {
		if !entry.IsDir() {
			continue
		}
		if _, err := time.Parse(time.RFC3339, entry.Name()); err != nil {
			continue
		}
		files, err := os.ReadDir(filepath.Join(source, entry.Name()))
		if err != nil {
			return nil, nil, fmt.Errorf("read legacy snapshot %q: %w", entry.Name(), err)
		}
		for _, file := range files {
			if file.IsDir() || (!strings.HasSuffix(file.Name(), ".didx") && !strings.HasSuffix(file.Name(), ".fidx")) {
				continue
			}
			if _, ok := allowed[file.Name()]; !ok {
				return nil, nil, fmt.Errorf("legacy group contains archive %q from another job", file.Name())
			}
			indices = append(indices, filepath.Join(target, entry.Name(), file.Name()))
		}
		snapshots = append(snapshots, entry.Name())
	}
	sort.Strings(indices)
	sort.Strings(snapshots)
	return indices, snapshots, nil
}

func pathExists(path string) (bool, error) {
	_, err := os.Stat(path)
	if err == nil {
		return true, nil
	}
	if os.IsNotExist(err) {
		return false, nil
	}
	return false, err
}

func pbsLockPath(lockRoot, store, namespace string, parts ...string) string {
	path := filepath.Join(lockRoot, store)
	if namespace != "" {
		path = filepath.Join(path, strings.ReplaceAll(namespace, "/", ":"))
	}
	rpath := strings.Join(parts, "/")
	encoded := escapeLockUnit(rpath)
	if len(encoded) >= 255 {
		digest := sha256.Sum256([]byte(rpath))
		encoded = encoded[:80] + "..." + encoded[len(encoded)-80:] + "-" + hex.EncodeToString(digest[:])
		path = filepath.Join(path, "hashed")
	}
	return filepath.Join(path, encoded)
}

func escapeLockUnit(value string) string {
	var escaped strings.Builder
	for i, b := range []byte(value) {
		if b == '/' {
			escaped.WriteByte('-')
		} else if (i != 0 || b != '.') && (b >= 'a' && b <= 'z' || b >= 'A' && b <= 'Z' || b >= '0' && b <= '9' || b == '_' || b == '.') {
			escaped.WriteByte(b)
		} else {
			fmt.Fprintf(&escaped, `\x%02x`, b)
		}
	}
	return escaped.String()
}

func lockPBSPaths(paths []string) ([]*os.File, error) {
	locks := make([]*os.File, 0, len(paths))
	for _, path := range paths {
		lock, err := lockPBSPath(path)
		if err != nil {
			closePBSLocks(locks)
			return nil, err
		}
		locks = append(locks, lock)
	}
	return locks, nil
}

func lockPBSPath(path string) (*os.File, error) {
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return nil, err
	}
	file, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR, 0o660)
	if err != nil {
		return nil, err
	}
	if err := proxmox.ChownBackupUser(path); err != nil {
		_ = file.Close()
		return nil, err
	}
	if err := unix.Flock(int(file.Fd()), unix.LOCK_EX|unix.LOCK_NB); err != nil {
		_ = file.Close()
		return nil, fmt.Errorf("%s is in use: %w", path, err)
	}
	opened, err := file.Stat()
	if err != nil {
		_ = file.Close()
		return nil, err
	}
	current, err := os.Stat(path)
	if err != nil || !os.SameFile(opened, current) {
		_ = file.Close()
		return nil, fmt.Errorf("lock file %s changed while acquiring it", path)
	}
	return file, nil
}

func closePBSLocks(locks []*os.File) {
	for _, lock := range slices.Backward(locks) {
		_ = unix.Flock(int(lock.Fd()), unix.LOCK_UN)
		_ = lock.Close()
	}
}

func appendMoveJournal(path string, indices []string) error {
	if len(indices) == 0 {
		return nil
	}
	lock, err := lockPBSPath(path)
	if err != nil {
		return fmt.Errorf("lock PBS move journal: %w", err)
	}
	defer closePBSLocks([]*os.File{lock})
	if _, err := lock.Seek(0, 2); err != nil {
		return err
	}
	for _, index := range indices {
		if !filepath.IsAbs(index) || strings.ContainsRune(index, '\n') {
			return fmt.Errorf("invalid move journal path %q", index)
		}
		if _, err := lock.WriteString(index + "\n"); err != nil {
			return err
		}
	}
	return lock.Sync()
}
