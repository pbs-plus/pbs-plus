//go:build linux

package backup

import (
	"fmt"
	"io"
	"os"
	"os/exec"
	"strings"

	"github.com/sonroyaalmerol/pbs-plus/internal/store"
	"github.com/sonroyaalmerol/pbs-plus/internal/store/proxmox"
	"github.com/sonroyaalmerol/pbs-plus/internal/store/types"
)

func prepareBackupCommand(job *types.Job, storeInstance *store.Store, srcPath string, isAgent bool) (*exec.Cmd, error) {
	if srcPath == "" {
		return nil, fmt.Errorf("RunBackup: source path is required")
	}

	backupId, err := getBackupId(isAgent, job.Target)
	if err != nil {
		return nil, fmt.Errorf("RunBackup: failed to get backup ID: %w", err)
	}

	jobStore := fmt.Sprintf("%s@localhost:%s", proxmox.Session.APIToken.TokenId, job.Store)
	if jobStore == "@localhost:" {
		return nil, fmt.Errorf("RunBackup: invalid job store configuration")
	}

	cmdArgs := buildCommandArgs(storeInstance, job, srcPath, jobStore, backupId, isAgent)
	if len(cmdArgs) == 0 {
		return nil, fmt.Errorf("RunBackup: failed to build command arguments")
	}

	cmd := exec.Command("/usr/bin/prlimit", cmdArgs...)
	cmd.Env = buildCommandEnv(storeInstance)

	return cmd, nil
}

func getBackupId(isAgent bool, targetName string) (string, error) {
	if !isAgent {
		hostname, err := os.Hostname()
		if err != nil {
			hostnameBytes, err := os.ReadFile("/etc/hostname")
			if err != nil {
				return "localhost", nil
			}
			return strings.TrimSpace(string(hostnameBytes)), nil
		}
		return hostname, nil
	}
	if targetName == "" {
		return "", fmt.Errorf("target name is required for agent backup")
	}
	return strings.TrimSpace(strings.Split(targetName, " - ")[0]), nil
}

func buildCommandArgs(storeInstance *store.Store, job *types.Job, srcPath string, jobStore string, backupId string, isAgent bool) []string {
	if srcPath == "" || jobStore == "" || backupId == "" {
		return nil
	}

	cmdArgs := []string{
		"--nofile=1024:1024",
		"/usr/bin/proxmox-backup-client",
		"backup",
		fmt.Sprintf("%s.pxar:%s", strings.ReplaceAll(job.Target, " ", "-"), srcPath),
		"--repository", jobStore,
		"--change-detection-mode=metadata",
		"--backup-id", backupId,
		"--crypt-mode=none",
		"--skip-e2big-xattr", "true",
		"--skip-lost-and-found", "true",
	}

	// Add exclusions
	for _, exclusion := range job.Exclusions {
		if isAgent && exclusion.JobID != job.ID {
			continue
		}
		cmdArgs = append(cmdArgs, "--exclude", exclusion.Path)
	}

	// Add namespace if specified
	if job.Namespace != "" {
		_ = CreateNamespace(job.Namespace, job, storeInstance)
		cmdArgs = append(cmdArgs, "--ns", job.Namespace)
	}

	return cmdArgs
}

func buildCommandEnv(storeInstance *store.Store) []string {
	if storeInstance == nil || proxmox.Session.APIToken == nil {
		return os.Environ()
	}

	env := append(os.Environ(),
		fmt.Sprintf("PBS_PASSWORD=%s", proxmox.Session.APIToken.Value))
	// env = append(env, "PBS_LOG=debug")

	// Add fingerprint if available
	if pbsStatus, err := proxmox.Session.GetPBSStatus(); err == nil {
		if fingerprint, ok := pbsStatus.Info["fingerprint"]; ok {
			env = append(env, fmt.Sprintf("PBS_FINGERPRINT=%s", fingerprint))
		}
	}

	return env
}

func setupCommandPipes(cmd *exec.Cmd) (io.ReadCloser, io.ReadCloser, error) {
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		return nil, nil, fmt.Errorf("error creating stdout pipe: %w", err)
	}

	stderr, err := cmd.StderrPipe()
	if err != nil {
		stdout.Close() // Clean up stdout if stderr fails
		return nil, nil, fmt.Errorf("error creating stderr pipe: %w", err)
	}

	return stdout, stderr, nil
}
