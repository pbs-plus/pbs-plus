//go:build linux

package backup

import (
	"fmt"
	"os"
	"os/exec"
	"strings"

	"github.com/sonroyaalmerol/pbs-plus/internal/store"
)

func FixDatastore(job *store.Job, storeInstance *store.Store) error {
	if storeInstance == nil {
		return fmt.Errorf("FixDatastore: store is required")
	}

	if storeInstance.APIToken == nil {
		return fmt.Errorf("FixDatastore: api token is required")
	}

	target, err := storeInstance.GetTarget(job.Target)
	if err != nil {
		return fmt.Errorf("FixDatastore -> %w", err)
	}

	if target == nil {
		return fmt.Errorf("FixDatastore: Target '%s' does not exist.", job.Target)
	}

	if !target.ConnectionStatus {
		return fmt.Errorf("FixDatastore: Target '%s' is unreachable or does not exist.", job.Target)
	}

	jobStore := fmt.Sprintf(
		"%s@localhost:%s",
		storeInstance.APIToken.TokenId,
		job.Store,
	)

	newOwner := ""
	if storeInstance.APIToken != nil {
		newOwner = storeInstance.APIToken.TokenId
	} else {
		newOwner = storeInstance.LastToken.Username
	}

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

	cmdArgs := []string{
		"change-owner",
		fmt.Sprintf("host/%s", backupId),
		newOwner,
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
	cmd.Env = append(cmd.Env, fmt.Sprintf("PBS_PASSWORD=%s", storeInstance.APIToken.Value))

	pbsStatus, err := storeInstance.GetPBSStatus()
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
