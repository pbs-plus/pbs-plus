//go:build linux

package backup

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"strings"

	"github.com/pbs-plus/pbs-plus/internal/store"
	"github.com/pbs-plus/pbs-plus/internal/store/proxmox"
	"github.com/pbs-plus/pbs-plus/internal/store/types"
	"github.com/pbs-plus/pbs-plus/internal/syslog"
)

func getBackupId(isAgent bool, targetName string) (string, error) {
	if isAgent {
		if targetName == "" {
			return "", fmt.Errorf("target name is required for agent backup")
		}
		return strings.TrimSpace(strings.Split(targetName, " - ")[0]), nil
	}

	hostname, err := os.Hostname()
	if err != nil {
		hostnameBytes, _ := os.ReadFile("/etc/hostname")
		hostname = strings.TrimSpace(string(hostnameBytes))
		if hostname == "" {
			hostname = "localhost"
		}
	}
	return hostname, nil
}

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

	detectionMode := "--change-detection-mode=metadata"
	switch job.Mode {
	case "legacy":
		detectionMode = "--change-detection-mode=legacy"
	case "data":
		detectionMode = "--change-detection-mode=data"
	}

	cmdArgs := []string{
		"/usr/bin/proxmox-backup-client",
		"backup",
		fmt.Sprintf("%s.pxar:%s", proxmox.NormalizeHostname(job.Target), srcPath),
		"--repository", jobStore,
		detectionMode,
		"--entries-max", fmt.Sprintf("%d", job.MaxDirEntries+1024),
		"--backup-id", backupId,
		"--crypt-mode=none",
	}

	if nofile := os.Getenv("PBS_PLUS_CLIENT_NOFILE"); nofile != "" {
		cmdArgs = append(cmdArgs, fmt.Sprintf("--nofile=%s", nofile))
	} else {
		cmdArgs = append(cmdArgs, "--nofile=1024:1024")
	}

	addExclusion := func(path string) {
		if !strings.HasPrefix(path, "/") && !strings.HasPrefix(path, "!") && !strings.HasPrefix(path, "**/") {
			path = "**/" + path
		}
		cmdArgs = append(cmdArgs, "--exclude", path)
	}

	for _, exclusion := range extraExclusions {
		addExclusion(exclusion)
	}

	for _, exclusion := range job.Exclusions {
		addExclusion(exclusion.Path)
	}

	if globalExclusions, err := storeInstance.Database.GetAllGlobalExclusions(); err == nil {
		for _, exclusion := range globalExclusions {
			addExclusion(exclusion.Path)
		}
	}

	if job.Namespace != "" {
		_ = CreateNamespace(job.Namespace, job, storeInstance)
		cmdArgs = append(cmdArgs, "--ns", job.Namespace)
	}

	env := append(os.Environ(), fmt.Sprintf("PBS_PASSWORD=%s", proxmox.GetToken()))
	if pbsStatus, err := proxmox.GetProxmoxCertInfo(); err == nil {
		env = append(env, fmt.Sprintf("PBS_FINGERPRINT=%s", pbsStatus.FingerprintSHA256))
	}

	cmd := exec.CommandContext(ctx, "/usr/bin/prlimit", cmdArgs...)
	cmd.Env = env

	if err := CleanUnfinishedSnapshot(job, backupId); err != nil {
		syslog.L.Error(err).WithJob(job.ID).WithField("backupId", backupId).Write()
	}

	return cmd, nil
}
