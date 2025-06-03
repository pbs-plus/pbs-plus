//go:build linux

package backup

import (
	"fmt"
	"os"
	"os/exec"
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

func GetCurrentOwner(job types.Job, storeInstance *store.Store) (string, error) {
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
	ownerBytes, err := os.ReadFile(ownerFilePath)
	if err != nil {
		return "", fmt.Errorf("GetCurrentOwner: failed to read file: %w", err)
	}

	return strings.TrimSpace(string(ownerBytes)), nil
}

func SetDatastoreOwner(job types.Job, storeInstance *store.Store, owner string) error {
	if storeInstance == nil {
		return fmt.Errorf("SetDatastoreOwner: store is required")
	}

	target, err := storeInstance.Database.GetTarget(job.Target)
	if err != nil {
		if os.IsNotExist(err) {
			return fmt.Errorf("SetDatastoreOwner: Target '%s' does not exist.", job.Target)
		}
		return fmt.Errorf("SetDatastoreOwner -> %w", err)
	}

	if !target.ConnectionStatus {
		return fmt.Errorf("SetDatastoreOwner: Target '%s' is unreachable or does not exist.", job.Target)
	}

	jobStore := fmt.Sprintf(
		"%s@localhost:%s",
		proxmox.AUTH_ID,
		job.Store,
	)

	hostname, err := os.Hostname()
	if err != nil {
		hostnameFile, err := os.ReadFile("/etc/hostname")
		if err != nil {
			hostname = "localhost"
		}

		hostname = strings.TrimSpace(string(hostnameFile))
	}

	isAgent := strings.HasPrefix(target.Path, "agent://")
	backupId := hostname
	if isAgent {
		backupId = strings.TrimSpace(strings.Split(target.Name, " - ")[0])
	}
	backupId = proxmox.NormalizeHostname(backupId)

	cmdArgs := []string{
		"change-owner",
		fmt.Sprintf("host/%s", backupId),
		owner,
		"--repository",
		jobStore,
	}

	if job.Namespace != "" {
		cmdArgs = append(cmdArgs, "--ns")
		cmdArgs = append(cmdArgs, job.Namespace)
	} else if isAgent && job.Namespace == "" {
		cmdArgs = append(cmdArgs, "--ns")
		cmdArgs = append(cmdArgs, strings.ReplaceAll(job.Target, " - ", "/"))
	}

	cmd := exec.Command("/usr/bin/proxmox-backup-client", cmdArgs...)
	cmd.Env = os.Environ()
	cmd.Env = append(cmd.Env, fmt.Sprintf("PBS_PASSWORD=%s", proxmox.GetToken()))

	pbsStatus, err := proxmox.GetProxmoxCertInfo()
	if err == nil {
		cmd.Env = append(cmd.Env, fmt.Sprintf("PBS_FINGERPRINT=%s", pbsStatus.FingerprintSHA256))
	}

	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr

	err = cmd.Run()
	if err != nil {
		return fmt.Errorf("SetDatastoreOwner: proxmox-backup-client change-owner error (%s) -> %w", cmd.String(), err)
	}

	return nil
}

func FixDatastore(job types.Job, storeInstance *store.Store) error {
	return SetDatastoreOwner(job, storeInstance, proxmox.AUTH_ID)
}
