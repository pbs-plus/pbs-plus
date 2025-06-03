//go:build linux

package backup

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/pbs-plus/pbs-plus/internal/store"
	"github.com/pbs-plus/pbs-plus/internal/store/proxmox"
	"github.com/pbs-plus/pbs-plus/internal/store/types"
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

func CreateNamespace(namespace string, job types.Job, storeInstance *store.Store) error {
	if storeInstance == nil {
		return fmt.Errorf("CreateNamespace: store is required")
	}

	datastoreInfo, err := proxmox.GetDatastoreInfo(job.Store)
	if err != nil {
		return fmt.Errorf("CreateNamespace: failed to get datastore; %w", err)
	}

	namespaceSplit := strings.Split(namespace, "/")

	fullNamespacePath := datastoreInfo.Path
	parentNamespacePath := datastoreInfo.Path

	for i, ns := range namespaceSplit {
		fullNamespacePath = filepath.Join(fullNamespacePath, "ns", ns)
		if i == 0 {
			parentNamespacePath = filepath.Join(parentNamespacePath, "ns", ns)
		}
	}

	err = os.MkdirAll(fullNamespacePath, os.FileMode(0755))
	if err != nil {
		return fmt.Errorf("CreateNamespace: error creating namespace -> %w", err)
	}

	err = os.Chown(parentNamespacePath, 34, 34)
	if err != nil {
		return fmt.Errorf("CreateNamespace: error changing filesystem owner -> %w", err)
	}

	job.Namespace = namespace
	err = storeInstance.Database.UpdateJob(nil, job)
	if err != nil {
		return fmt.Errorf("CreateNamespace: error updating job to namespace -> %w", err)
	}

	return nil
}

func GetOwnerFilePath(job types.Job, storeInstance *store.Store) (string, error) {
	if storeInstance == nil {
		return "", fmt.Errorf("GetCurrentOwner: store is required")
	}

	target, err := storeInstance.Database.GetTarget(job.Target)
	if err != nil {
		if os.IsNotExist(err) {
			return "", fmt.Errorf("GetCurrentOwner: Target '%s' does not exist.", job.Target)
		}
		return "", fmt.Errorf("GetCurrentOwner -> %w", err)
	}

	if !target.ConnectionStatus {
		return "", fmt.Errorf("GetCurrentOwner: Target '%s' is unreachable or does not exist.", job.Target)
	}

	isAgent := strings.HasPrefix(target.Path, "agent://")
	backupId, err := getBackupId(isAgent, job.Target)
	if err != nil {
		return "", fmt.Errorf("GetCurrentOwner: failed to get backup ID: %w", err)
	}
	backupId = proxmox.NormalizeHostname(backupId)

	datastoreInfo, err := proxmox.GetDatastoreInfo(job.Store)
	if err != nil {
		return "", fmt.Errorf("GetCurrentOwner: failed to get datastore; %w", err)
	}

	namespaceSplit := strings.Split(job.Namespace, "/")

	fullNamespacePath := datastoreInfo.Path

	for _, ns := range namespaceSplit {
		fullNamespacePath = filepath.Join(fullNamespacePath, "ns", ns)
	}

	ownerFilePath := filepath.Join(fullNamespacePath, "host", backupId)

	return ownerFilePath, nil
}

func GetCurrentOwner(job types.Job, storeInstance *store.Store) (string, error) {
	filePath, err := GetOwnerFilePath(job, storeInstance)
	if err != nil {
		return "", err
	}

	owner, err := os.ReadFile(filePath)
	if err != nil {
		return "", err
	}

	return strings.TrimSpace(string(owner)), nil
}

func SetDatastoreOwner(job types.Job, storeInstance *store.Store, owner string) error {
	filePath, err := GetOwnerFilePath(job, storeInstance)
	if err != nil {
		return err
	}

	dirPath := filepath.Dir(filePath)

	_ = os.MkdirAll(dirPath, os.FileMode(0755))

	err = os.WriteFile(filePath, []byte(owner), os.FileMode(0644))
	if err != nil {
		return fmt.Errorf("SetDatastoreOwner: failed to write owner file -> %w", err)
	}

	err = os.Chown(dirPath, 34, 34)
	if err != nil {
		return fmt.Errorf("SetDatastoreOwner: error changing filesystem owner -> %w", err)
	}

	err = os.Chown(filePath, 34, 34)
	if err != nil {
		return fmt.Errorf("SetDatastoreOwner: error changing filesystem owner -> %w", err)
	}

	return nil
}

func FixDatastore(job types.Job, storeInstance *store.Store) error {
	return SetDatastoreOwner(job, storeInstance, proxmox.AUTH_ID)
}
