//go:build linux

package proxmox

import (
	"fmt"
	"os"
	"os/exec"
	"regexp"
	"strings"
)

type DatastoreInfo struct {
	Name             string
	Path             string
	Comment          string
	GcSchedule       string
	NotificationMode string
}

func parseDatastoreOutput(output string) (DatastoreInfo, error) {
	info := DatastoreInfo{}
	lines := strings.Split(output, "\n")
	dataLineRegex := regexp.MustCompile(`^│\s*([^│]+?)\s*│\s*([^│]+?)\s*│$`)

	for _, line := range lines {
		matches := dataLineRegex.FindStringSubmatch(line)
		if len(matches) == 3 {
			key := strings.TrimSpace(matches[1])
			value := strings.TrimSpace(matches[2])

			switch key {
			case "name":
				info.Name = value
			case "path":
				info.Path = value
			case "comment":
				info.Comment = value
			case "gc-schedule":
				info.GcSchedule = value
			case "notification-mode":
				info.NotificationMode = value
			}
		}
	}

	if info.Name == "" && info.Path == "" {
		return info, fmt.Errorf("failed to parse any valid data")
	}

	return info, nil
}

func GetDatastoreInfo(datastoreName string) (DatastoreInfo, error) {
	cmd := exec.Command(
		"proxmox-backup-manager",
		"datastore",
		"show",
		datastoreName,
	)
	cmd.Env = os.Environ()

	outputBytes, err := cmd.CombinedOutput()
	outputStr := string(outputBytes)
	if err != nil {
		return DatastoreInfo{}, fmt.Errorf("%w: %s", err, outputStr)
	}

	return parseDatastoreOutput(outputStr)
}
