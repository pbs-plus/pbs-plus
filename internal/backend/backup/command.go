//go:build linux

package backup

import (
	"context"
	"fmt"
	"io"
	"os"
	"os/exec"
	"strings"

	"github.com/pbs-plus/pbs-plus/internal/store"
	"github.com/pbs-plus/pbs-plus/internal/store/proxmox"
	"github.com/pbs-plus/pbs-plus/internal/store/types"
)

func prepareBackupCommand(ctx context.Context, job types.Job, storeInstance *store.Store, srcPath string, isAgent bool, extraExclusions []string) (*exec.Cmd, error) {
	if srcPath == "" {
		return nil, fmt.Errorf("RunBackup: source path is required")
	}

	backupId, err := getBackupId(isAgent, job.Target)
	if err != nil {
		return nil, fmt.Errorf("RunBackup: failed to get backup ID: %w", err)
	}

	backupId = proxmox.NormalizeHostname(backupId)

	jobStore := fmt.Sprintf("%s@localhost:%s", proxmox.AUTH_ID, job.Store)
	if jobStore == "@localhost:" {
		return nil, fmt.Errorf("RunBackup: invalid job store configuration")
	}

	cmdArgs := buildCommandArgs(storeInstance, job, srcPath, jobStore, backupId, extraExclusions)
	if len(cmdArgs) == 0 {
		return nil, fmt.Errorf("RunBackup: failed to build command arguments")
	}

	cmd := exec.CommandContext(ctx, "/usr/bin/prlimit", cmdArgs...)
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

func buildCommandArgs(storeInstance *store.Store, job types.Job, srcPath string, jobStore string, backupId string, extraExclusions []string) []string {
	if srcPath == "" || jobStore == "" || backupId == "" {
		return nil
	}

	detectionMode := "--change-detection-mode=metadata"
	switch job.Mode {
	case "legacy":
		detectionMode = "--change-detection-mode=legacy"
	case "data":
		detectionMode = "--change-detection-mode=data"
	}

	cmdArgs := []string{
		"--nofile=1024:1024",
		"/usr/bin/proxmox-backup-client",
		"backup",
		fmt.Sprintf("%s.pxar:%s", proxmox.NormalizeHostname(job.Target), srcPath),
		"--repository", jobStore,
		detectionMode,
		"--entries-max", fmt.Sprintf("%d", job.MaxDirEntries),
		"--backup-id", backupId,
		"--crypt-mode=none",
	}

	// Add exclusions
	for _, extraExclusion := range extraExclusions {
		path := extraExclusion
		if !strings.HasPrefix(extraExclusion, "/") && !strings.HasPrefix(extraExclusion, "!") && !strings.HasPrefix(extraExclusion, "**/") {
			path = "**/" + path
		}

		cmdArgs = append(cmdArgs, "--exclude", path)
	}

	for _, exclusion := range job.Exclusions {
		path := exclusion.Path
		if !strings.HasPrefix(exclusion.Path, "/") && !strings.HasPrefix(exclusion.Path, "!") && !strings.HasPrefix(exclusion.Path, "**/") {
			path = "**/" + path
		}

		cmdArgs = append(cmdArgs, "--exclude", path)
	}

	// Get global exclusions
	globalExclusions, err := storeInstance.Database.GetAllGlobalExclusions()
	if err == nil && globalExclusions != nil {
		for _, exclusion := range globalExclusions {
			path := exclusion.Path
			if !strings.HasPrefix(exclusion.Path, "/") && !strings.HasPrefix(exclusion.Path, "!") && !strings.HasPrefix(exclusion.Path, "**/") {
				path = "**/" + path
			}

			cmdArgs = append(cmdArgs, "--exclude", path)
		}
	}

	// Add namespace if specified
	if job.Namespace != "" {
		_ = CreateNamespace(job.Namespace, job, storeInstance)
		cmdArgs = append(cmdArgs, "--ns", job.Namespace)
	}

	return cmdArgs
}

func buildCommandEnv(storeInstance *store.Store) []string {
	if storeInstance == nil {
		return os.Environ()
	}

	env := append(os.Environ(),
		fmt.Sprintf("PBS_PASSWORD=%s", proxmox.GetToken()))

	if pbsStatus, err := proxmox.GetProxmoxCertInfo(); err == nil {
		env = append(env, fmt.Sprintf("PBS_FINGERPRINT=%s", pbsStatus.FingerprintSHA256))
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
