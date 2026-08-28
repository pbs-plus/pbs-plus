//go:build linux

package snapshotmount

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"github.com/pbs-plus/pbs-plus/internal/calendar"
	"github.com/pbs-plus/pbs-plus/internal/conf"
	"github.com/pbs-plus/pbs-plus/internal/proxmox/cli"
	"github.com/pbs-plus/pbs-plus/internal/validate"
)

type Profile struct {
	Datastore  string `json:"datastore"`
	Namespace  string `json:"namespace"`
	BackupType string `json:"backup_type"`
	BackupID   string `json:"backup_id"`
	Mode       string `json:"mode"`
	MountPath  string `json:"mount_path"`
	Schedule   string `json:"schedule"`
	AutoMount  bool   `json:"auto_mount"`
	CreatedAt  int64  `json:"created_at"`
	UpdatedAt  int64  `json:"updated_at"`
}

func (p Profile) ID() string {
	return Key(p.Datastore, p.Namespace, p.BackupType, p.BackupID, "latest")
}

func profilesDir() string { return filepath.Join(conf.StatePrefix, "mount-profiles") }

func ValidateProfile(p Profile) error {
	if err := validate.ValidateDatastore(p.Datastore); err != nil {
		return fmt.Errorf("invalid datastore: %w", err)
	}
	if err := validate.ValidateNamespace(p.Namespace); err != nil {
		return err
	}
	if err := validate.ValidateBackupType(p.BackupType); err != nil {
		return err
	}
	if err := validate.ValidateBackupID(p.BackupID); err != nil {
		return err
	}
	if p.Mode == "" {
		p.Mode = ModeRO
	}
	if p.Mode != ModeRO && p.Mode != ModeRW {
		return fmt.Errorf("invalid mode %q", p.Mode)
	}
	if p.Schedule != "" {
		if _, err := calendar.Parse(p.Schedule); err != nil {
			return fmt.Errorf("invalid schedule %q: %w", p.Schedule, err)
		}
	}
	return ValidateMountPath(p.MountPath)
}

func SaveProfile(p Profile) error {
	if err := ValidateProfile(p); err != nil {
		return err
	}
	if err := os.MkdirAll(profilesDir(), 0o700); err != nil {
		return fmt.Errorf("create profiles dir: %w", err)
	}
	data, err := json.Marshal(p)
	if err != nil {
		return err
	}
	return os.WriteFile(filepath.Join(profilesDir(), p.ID()+".json"), data, 0o600)
}

func LoadProfile(id string) (Profile, bool, error) {
	if strings.ContainsAny(id, "/\\") || strings.Contains(id, "..") {
		return Profile{}, false, fmt.Errorf("invalid profile id")
	}
	data, err := os.ReadFile(filepath.Join(profilesDir(), id+".json"))
	if err != nil {
		if os.IsNotExist(err) {
			return Profile{}, false, nil
		}
		return Profile{}, false, err
	}
	var p Profile
	if err := json.Unmarshal(data, &p); err != nil {
		return Profile{}, false, fmt.Errorf("decoding profile %s: %w", id, err)
	}
	return p, true, nil
}

func DeleteProfile(id string) error {
	if strings.ContainsAny(id, "/\\") || strings.Contains(id, "..") {
		return fmt.Errorf("invalid profile id")
	}
	return os.Remove(filepath.Join(profilesDir(), id+".json"))
}

func ListProfiles() ([]Profile, error) {
	entries, err := os.ReadDir(profilesDir())
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil
		}
		return nil, err
	}
	profiles := make([]Profile, 0, len(entries))
	for _, e := range entries {
		if e.IsDir() || !strings.HasSuffix(e.Name(), ".json") {
			continue
		}
		p, ok, err := LoadProfile(strings.TrimSuffix(e.Name(), ".json"))
		if err != nil {
			return nil, err
		}
		if ok {
			profiles = append(profiles, p)
		}
	}
	return profiles, nil
}

func LatestSnapshot(p Profile) (backupTime string, fileName string, err error) {
	dsInfo, err := cli.GetDatastoreInfo(p.Datastore)
	if err != nil {
		return "", "", err
	}
	if dsInfo.Path == "" {
		return "", "", fmt.Errorf("datastore %s has no path", p.Datastore)
	}
	return LatestSnapshotIn(dsInfo.Path, p.Namespace, p.BackupType, p.BackupID)
}

func LatestSnapshotIn(storeRoot, ns, backupType, backupID string) (string, string, error) {
	groupDir := groupParentDir(storeRoot, ns, backupType, backupID)
	entries, err := os.ReadDir(groupDir)
	if err != nil {
		return "", "", fmt.Errorf("reading backup group %s: %w", groupDir, err)
	}
	var times []time.Time
	for _, e := range entries {
		if !e.IsDir() {
			continue
		}
		t, err := time.Parse(dirTimeLayout, e.Name())
		if err != nil {
			continue
		}
		times = append(times, t)
	}
	sort.Slice(times, func(i, j int) bool { return times[i].After(times[j]) })
	for _, t := range times {
		dirTime := DirTime(t)
		snapDir := filepath.Join(groupDir, dirTime)
		files, err := os.ReadDir(snapDir)
		if err != nil {
			continue
		}
		var pxar string
		for _, f := range files {
			name := f.Name()
			if strings.HasSuffix(name, ".mpxar.didx") {
				pxar = name
				break
			}
			if strings.HasSuffix(name, ".pxar.didx") && pxar == "" {
				pxar = name
			}
		}
		if pxar != "" {
			return t.Format(time.RFC3339), pxar, nil
		}
	}
	return "", "", fmt.Errorf("no snapshot with a mountable archive in %s", groupDir)
}

func groupParentDir(storeRoot, ns, backupType, backupID string) string {
	parts := []string{storeRoot}
	for part := range strings.SplitSeq(ns, "/") {
		if part != "" {
			parts = append(parts, "ns", part)
		}
	}
	parts = append(parts, backupType, backupID)
	return filepath.Join(parts...)
}
