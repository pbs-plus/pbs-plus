//go:build linux

package proxmox

import (
	"encoding/json"
	"fmt"
	"os/exec"
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
