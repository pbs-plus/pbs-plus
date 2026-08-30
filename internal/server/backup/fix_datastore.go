//go:build linux

package backup

import (
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"time"

	"github.com/pbs-plus/pbs-plus/internal/log"
	"github.com/pbs-plus/pbs-plus/internal/proxmox"
	"github.com/pbs-plus/pbs-plus/internal/proxmox/cli"
	"github.com/pbs-plus/pbs-plus/internal/server/application"
	"github.com/pbs-plus/pbs-plus/internal/server/coredb"
)

type NamespaceReq struct {
	Name   string `json:"name"`
	Parent string `json:"parent"`
}

type PBSStoreGroups struct {
	Owner string `json:"owner"`
}

type PBSStoreGroupsResponse struct {
	Data PBSStoreGroups `json:"data"`
}

func CreateNamespace(namespace string, backup coredb.Backup, app *application.Runtime) error {
	if app == nil {
		return fmt.Errorf("CreateNamespace: store is required")
	}

	if err := cli.EnsureNamespace(backup.Store, namespace); err != nil {
		return fmt.Errorf("CreateNamespace: %w", err)
	}

	backup.Namespace = namespace
	err := app.CoreDB.UpdateBackup(nil, backup)
	if err != nil {
		return fmt.Errorf("CreateNamespace: error updating backup to namespace -> %w", err)
	}

	return nil
}

func GetOwnerFilePath(backup coredb.Backup, app *application.Runtime) (string, error) {
	if app == nil {
		return "", fmt.Errorf("GetCurrentOwner: store is required")
	}

	backupID, err := getBackupId(backup)
	if err != nil {
		return "", fmt.Errorf("GetCurrentOwner: failed to get backup ID: %w", err)
	}
	backupID = proxmox.NormalizeHostname(backupID)

	datastoreInfo, err := cli.GetDatastoreInfo(backup.Store)
	if err != nil {
		return "", fmt.Errorf("GetCurrentOwner: failed to get datastore; %w", err)
	}

	namespaceSplit := strings.Split(backup.Namespace, "/")

	fullNamespacePath := datastoreInfo.Path

	for _, ns := range namespaceSplit {
		fullNamespacePath = filepath.Join(fullNamespacePath, "ns", ns)
	}

	ownerFilePath := filepath.Join(fullNamespacePath, "host", backupID, "owner")

	return ownerFilePath, nil
}

func GetCurrentOwner(backup coredb.Backup, app *application.Runtime) (string, error) {
	filePath, err := GetOwnerFilePath(backup, app)
	if err != nil {
		return "", err
	}

	owner, err := os.ReadFile(filePath)
	if err != nil {
		return "", err
	}

	return strings.TrimSpace(string(owner)), nil
}

func SetDatastoreOwner(backup coredb.Backup, app *application.Runtime, owner string) error {
	filePath, err := GetOwnerFilePath(backup, app)
	if err != nil {
		return err
	}

	datastoreInfo, err := cli.GetDatastoreInfo(backup.Store)
	if err != nil {
		return fmt.Errorf("SetDatastoreOwner: failed to get datastore; %w", err)
	}

	backupID, err := getBackupId(backup)
	if err != nil {
		return fmt.Errorf("SetDatastoreOwner: failed to get backup ID: %w", err)
	}
	backupID = proxmox.NormalizeHostname(backupID)

	if err := proxmox.EnsureGroupPath(datastoreInfo.Path, backup.Namespace, "host", backupID); err != nil {
		return fmt.Errorf("SetDatastoreOwner: ensure group path: %w", err)
	}

	if err := os.WriteFile(filePath, []byte(owner), os.FileMode(0644)); err != nil {
		return fmt.Errorf("SetDatastoreOwner: failed to write owner file -> %w", err)
	}

	if err := proxmox.ChownBackupUser(filePath); err != nil {
		return fmt.Errorf("SetDatastoreOwner: error changing filesystem owner -> %w", err)
	}

	return nil
}

func FixDatastore(backup coredb.Backup, app *application.Runtime) error {
	return SetDatastoreOwner(backup, app, proxmox.AuthID)
}

func parseSnapshotTimestamp(input string) (time.Time, error) {
	parsedTime, err := time.Parse(time.RFC3339, input)
	if err != nil {
		return time.Time{}, err
	}
	return parsedTime, nil
}

func CleanUnfinishedSnapshot(backup coredb.Backup, backupID string) error {
	if backupID == "" {
		return fmt.Errorf("CleanUnfinishedSnapshot: backupID is required")
	}

	datastoreInfo, err := cli.GetDatastoreInfo(backup.Store)
	if err != nil {
		return fmt.Errorf("CleanUnfinishedSnapshot: failed to get datastore; %w", err)
	}

	namespaceSplit := strings.Split(backup.Namespace, "/")

	fullNamespacePath := datastoreInfo.Path
	parentNamespacePath := datastoreInfo.Path

	for i, ns := range namespaceSplit {
		fullNamespacePath = filepath.Join(fullNamespacePath, "ns", ns)
		if i == 0 {
			parentNamespacePath = filepath.Join(parentNamespacePath, "ns", ns)
		}
	}

	pathWithBackupId := filepath.Join(fullNamespacePath, "host", backupID)

	existingSnapshots, err := os.ReadDir(pathWithBackupId)
	if len(existingSnapshots) == 0 || err != nil {
		return nil
	}

	var latestSnapshot string
	for _, existingSnapshot := range slices.Backward(existingSnapshots) {
		name := existingSnapshot.Name()
		if name == "owner" {
			continue
		}
		if _, err := parseSnapshotTimestamp(name); err == nil {
			latestSnapshot = name
			break
		}
	}

	if latestSnapshot == "" {
		return nil
	}

	snapshotPath := filepath.Join(pathWithBackupId, latestSnapshot)
	entries, err := os.ReadDir(snapshotPath)
	if err != nil {
		return nil
	}

	expectedPxarName := proxmox.NormalizeHostname(backup.Target.Name)
	tmpSuffixes := map[string]struct{}{
		expectedPxarName + ".mpxar.tmp_didx": {},
		expectedPxarName + ".ppxar.tmp_didx": {},
		expectedPxarName + ".pxar.tmp_didx":  {},
	}

	for _, e := range entries {
		if _, ok := tmpSuffixes[e.Name()]; ok {
			if err := os.RemoveAll(snapshotPath); err != nil && !os.IsNotExist(err) {
				log.Error(err, "")
			}
			break
		}
	}

	return nil
}
