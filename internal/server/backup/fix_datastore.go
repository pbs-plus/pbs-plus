//go:build linux

package backup

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
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
		return errors.New("creating a namespace: store is required")
	}

	if err := cli.EnsureNamespace(backup.Store, namespace); err != nil {
		return fmt.Errorf("creating a namespace: %w", err)
	}

	return nil
}

func GetOwnerFilePath(backup coredb.Backup, app *application.Runtime) (string, error) {
	if app == nil {
		return "", errors.New("getting the current owner: store is required")
	}

	backupID, err := getBackupId(backup)
	if err != nil {
		return "", fmt.Errorf("getting the current owner: getting the backup ID: %w", err)
	}
	datastoreInfo, err := cli.GetDatastoreInfo(backup.Store)
	if err != nil {
		return "", fmt.Errorf("getting the current owner: getting the datastore: %w", err)
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
		return fmt.Errorf("setting the datastore owner: getting the datastore: %w", err)
	}

	backupID, err := getBackupId(backup)
	if err != nil {
		return fmt.Errorf("setting the datastore owner: getting the backup ID: %w", err)
	}
	if err := proxmox.EnsureGroupPath(datastoreInfo.Path, backup.Namespace, "host", backupID); err != nil {
		return fmt.Errorf("setting the datastore owner: ensuring the group path: %w", err)
	}

	if err := os.WriteFile(filePath, []byte(owner), os.FileMode(0644)); err != nil {
		return fmt.Errorf("setting the datastore owner: writing the owner file: %w", err)
	}

	if err := proxmox.ChownBackupUser(filePath); err != nil {
		return fmt.Errorf("setting the datastore owner: changing the filesystem owner: %w", err)
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
		return errors.New("cleaning an unfinished snapshot: backup ID is required")
	}

	datastoreInfo, err := cli.GetDatastoreInfo(backup.Store)
	if err != nil {
		return fmt.Errorf("cleaning an unfinished snapshot: getting the datastore: %w", err)
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

	pruneIncompleteSnapshots(pathWithBackupId)

	return nil
}

// incompleteSnapshotGrace keeps a snapshot a live client is still writing out of reach of the pruner; a finished run replaces its tmp index within milliseconds, so anything older is abandoned.
var incompleteSnapshotGrace = 5 * time.Minute

func snapshotIsIncomplete(snapshotPath string) bool {
	entries, err := os.ReadDir(snapshotPath)
	if err != nil {
		return false
	}

	hasTemporaryIndex := false
	for _, e := range entries {
		name := e.Name()
		if name == "index.json.blob" {
			return false
		}
		if strings.HasSuffix(name, ".tmp_didx") || strings.HasSuffix(name, ".tmp_fidx") {
			hasTemporaryIndex = true
		}
	}

	return hasTemporaryIndex
}

func pruneIncompleteSnapshots(groupPath string) int {
	entries, err := os.ReadDir(groupPath)
	if err != nil {
		if !os.IsNotExist(err) {
			log.Error(err, "failed to read the backup group directory", "path", groupPath)
		}
		return 0
	}

	removed := 0
	for _, entry := range entries {
		if !entry.IsDir() {
			continue
		}
		if _, err := parseSnapshotTimestamp(entry.Name()); err != nil {
			continue
		}

		snapshotPath := filepath.Join(groupPath, entry.Name())
		if !snapshotIsIncomplete(snapshotPath) {
			continue
		}

		info, err := entry.Info()
		if err != nil {
			log.Error(err, "failed to stat an unfinished snapshot", "path", snapshotPath)
			continue
		}
		if time.Since(info.ModTime()) < incompleteSnapshotGrace {
			continue
		}

		if err := os.RemoveAll(snapshotPath); err != nil && !os.IsNotExist(err) {
			log.Error(err, "failed to remove an unfinished snapshot", "path", snapshotPath)
			continue
		}
		log.Info("removed an unfinished snapshot left by an earlier run", "path", snapshotPath)
		removed++
	}

	return removed
}
