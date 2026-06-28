//go:build linux

package cli

import (
	"encoding/json"
	"fmt"
	"os/exec"

	"github.com/pbs-plus/pbs-plus/internal/proxmox"
)

type DatastoreInfo struct {
	Name             string `json:"name"`
	Path             string `json:"path"`
	Comment          string `json:"comment"`
	NotificationMode string `json:"notification-mode"`
	Tuning           string `json:"tuning"`
}

func GetDatastoreInfo(datastoreName string) (DatastoreInfo, error) {
	cmd := exec.Command(
		"proxmox-backup-manager",
		"datastore", "show",
		datastoreName,
		"--output-format", "json",
	)
	output, err := cmd.CombinedOutput()
	if err != nil {
		return DatastoreInfo{}, fmt.Errorf("%w: %s", err, output)
	}
	var info DatastoreInfo
	if err := json.Unmarshal(output, &info); err != nil {
		return DatastoreInfo{}, err
	}
	return info, nil
}

func EnsureNamespace(datastore, namespace string) error {
	ds, err := GetDatastoreInfo(datastore)
	if err != nil {
		return fmt.Errorf("get datastore info for %q: %w", datastore, err)
	}
	return proxmox.EnsureNamespacePath(ds.Path, namespace)
}
