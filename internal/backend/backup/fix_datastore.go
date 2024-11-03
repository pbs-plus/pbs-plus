//go:build linux

package backup

import (
	"fmt"
	"os"
	"os/exec"
	"strings"

	"github.com/sonroyaalmerol/pbs-d2d-backup/internal/store"
)

func FixDatastore(job *store.Job, storeInstance *store.Store) error {
	if storeInstance == nil {
		return fmt.Errorf("FixDatastore: store is required")
	}

	if storeInstance.APIToken == nil && storeInstance.LastToken == nil {
		return fmt.Errorf("FixDatastore: token is required")
	}

	jobStore := job.Store

	if storeInstance.APIToken != nil {
		jobStore = fmt.Sprintf(
			"%s@localhost:%s",
			storeInstance.APIToken.TokenId,
			job.Store,
		)
	}

	hostname, err := os.Hostname()
	if err != nil {
		hostnameFile, err := os.ReadFile("/etc/hostname")
		if err != nil {
			hostname = "localhost"
		}

		hostname = strings.TrimSpace(string(hostnameFile))
	}

	newOwner := ""
	if storeInstance.APIToken != nil {
		newOwner = storeInstance.APIToken.TokenId
	} else {
		newOwner = storeInstance.LastToken.Username
	}

	cmdArgs := []string{
		"change-owner",
		fmt.Sprintf("host/%s", hostname),
		newOwner,
		"--repository",
		jobStore,
	}

	if job.Namespace != "" {
		cmdArgs = append(cmdArgs, "--ns")
		cmdArgs = append(cmdArgs, job.Namespace)
	}

	cmd := exec.Command("/usr/bin/proxmox-backup-client", cmdArgs...)
	cmd.Env = os.Environ()
	if storeInstance.APIToken != nil {
		cmd.Env = append(cmd.Env, fmt.Sprintf("PBS_PASSWORD=%s", storeInstance.APIToken.Value))
	} else {
		cmd.Env = append(cmd.Env, fmt.Sprintf("PBS_PASSWORD=%s", storeInstance.LastToken.Ticket))
	}

	pbsStatus, err := store.GetPBSStatus(storeInstance.LastToken, storeInstance.APIToken)
	if err == nil {
		if fingerprint, ok := pbsStatus.Info["fingerprint"]; ok {
			cmd.Env = append(cmd.Env, fmt.Sprintf("PBS_FINGERPRINT=%s", fingerprint))
		}
	}

	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr

	err = cmd.Run()
	if err != nil {
		return fmt.Errorf("FixDatastore: proxmox-backup-client change-owner error (%s) -> %w", cmd.String(), err)
	}
	return nil
}
