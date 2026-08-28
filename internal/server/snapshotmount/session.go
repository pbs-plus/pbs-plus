//go:build linux

package snapshotmount

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/pbs-plus/pbs-plus/internal/conf"
	"github.com/pbs-plus/pbs-plus/internal/server/systemd"
	"github.com/pbs-plus/pbs-plus/internal/validate"
)

const (
	ModeRO = "ro"
	ModeRW = "rw"

	dirTimeLayout = "2006-01-02T15:04:05Z"
)

type Session struct {
	Datastore  string `json:"datastore"`
	Namespace  string `json:"namespace"`
	BackupType string `json:"backup_type"`
	BackupID   string `json:"backup_id"`
	BackupTime string `json:"backup_time"`
	FileName   string `json:"file_name"`
	Mode       string `json:"mode"`
	MountPoint string `json:"mount_point"`
	OverlayDir string `json:"overlay_dir,omitempty"`
	SocketPath string `json:"socket_path,omitempty"`
	ServiceKey string `json:"service_key"`
	CreatedAt  int64  `json:"created_at"`
}

func sessionsDir() string { return filepath.Join(conf.StatePrefix, "mount-sessions") }

func OverlayDir(key string) string {
	return filepath.Join(conf.StatePrefix, "mount-overlays", key)
}

func SocketPath(key string) string {
	return filepath.Join("/var/run/pbs-plus-mounts", key+".sock")
}

func Key(datastore, ns, backupType, backupID, safeTime string) string {
	return systemd.MountServiceKey(datastore, ns, backupType, backupID, safeTime)
}

func DirTime(t time.Time) string {
	return t.UTC().Format(dirTimeLayout)
}

func (s Session) ServiceName() string {
	return "pbs-plus-snapshot-mount-" + s.ServiceKey + ".service"
}

func (s Session) CommitCapable() bool {
	if s.Mode != ModeRW || s.SocketPath == "" {
		return false
	}
	_, err := os.Stat(s.SocketPath)
	return err == nil
}

func (s Session) ResourceLock() string { return "snapshot-mount:" + s.ServiceKey }

func SaveSession(s Session) error {
	if err := os.MkdirAll(sessionsDir(), 0o700); err != nil {
		return fmt.Errorf("create sessions dir: %w", err)
	}
	data, err := json.Marshal(s)
	if err != nil {
		return err
	}
	return os.WriteFile(filepath.Join(sessionsDir(), s.ServiceKey+".json"), data, 0o600)
}

func LoadSession(key string) (Session, error) {
	data, err := os.ReadFile(filepath.Join(sessionsDir(), key+".json"))
	if err != nil {
		return Session{}, err
	}
	var s Session
	if err := json.Unmarshal(data, &s); err != nil {
		return Session{}, fmt.Errorf("decode session %s: %w", key, err)
	}
	return s, nil
}

func DeleteSession(key string) error {
	err := os.Remove(filepath.Join(sessionsDir(), key+".json"))
	if err != nil && !os.IsNotExist(err) {
		return err
	}
	return nil
}

func ListSessions() ([]Session, error) {
	entries, err := os.ReadDir(sessionsDir())
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil
		}
		return nil, err
	}
	sessions := make([]Session, 0, len(entries))
	for _, e := range entries {
		if e.IsDir() || !strings.HasSuffix(e.Name(), ".json") {
			continue
		}
		s, err := LoadSession(strings.TrimSuffix(e.Name(), ".json"))
		if err != nil {
			continue
		}
		sessions = append(sessions, s)
	}
	return sessions, nil
}

func FindSessionByMountPoint(mountPoint string) (Session, bool, error) {
	sessions, err := ListSessions()
	if err != nil {
		return Session{}, false, err
	}
	clean := filepath.Clean(mountPoint)
	for _, s := range sessions {
		if filepath.Clean(s.MountPoint) == clean {
			return s, true, nil
		}
	}
	return Session{}, false, nil
}

func DefaultMountPoint(datastore, ns, backupType, backupID string, parsedTime time.Time) string {
	return filepath.Clean(filepath.Join(
		conf.RestoreMountBasePath,
		datastore,
		ns,
		fmt.Sprintf("%s-%s", backupType, backupID),
		defaultMountPointLeaf(parsedTime),
	))
}

func defaultMountPointLeaf(parsedTime time.Time) string {
	if parsedTime.IsZero() {
		return "init"
	}
	return parsedTime.Format(dirTimeLayout)
}

func ValidateMountPath(path string) error {
	if path == "" {
		return nil
	}
	clean := filepath.Clean(path)
	if clean == "/mnt" {
		return fmt.Errorf("mount path cannot be /mnt itself")
	}
	if !strings.HasPrefix(clean, "/mnt/") {
		return fmt.Errorf("custom mount paths must live under /mnt")
	}
	if err := validate.SanitizeMountPoint(clean, "/mnt"); err != nil {
		return err
	}
	return nil
}
