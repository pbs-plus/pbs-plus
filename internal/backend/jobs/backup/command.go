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

func getBackupId(isAgent bool, target types.TargetName) (string, error) {
	if isAgent {
		if target.String() == "" {
			return "", fmt.Errorf("target name is required for agent backup")
		}
		return target.GetHostname(), nil
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

func prepareBackupCommand(ctx context.Context, backup types.Backup, storeInstance *store.Store, srcPath string, isAgent bool, extraExclusions []string) (*exec.Cmd, error) {
	if srcPath == "" {
		return nil, fmt.Errorf("RunBackup: source path is required")
	}

	backupId, err := getBackupId(isAgent, backup.Target)
	if err != nil {
		return nil, fmt.Errorf("RunBackup: failed to get backup ID: %w", err)
	}
	backupId = proxmox.NormalizeHostname(backupId)

	backupStore := fmt.Sprintf("%s@localhost:%s", proxmox.AUTH_ID, backup.Store)
	if backupStore == "@localhost:" {
		return nil, fmt.Errorf("RunBackup: invalid backup store configuration")
	}

	detectionMode := "--change-detection-mode=metadata"
	switch backup.Mode {
	case "legacy":
		detectionMode = "--change-detection-mode=legacy"
	case "data":
		detectionMode = "--change-detection-mode=data"
	}

	cmdArgs := []string{}
	if nofile := os.Getenv("PBS_PLUS_CLIENT_NOFILE"); nofile != "" {
		cmdArgs = append(cmdArgs, fmt.Sprintf("--nofile=%s", nofile))
	} else {
		cmdArgs = append(cmdArgs, "--nofile=1024:1024")
	}

	cmdArgs = append(cmdArgs, []string{
		"/usr/bin/proxmox-backup-client",
		"backup",
		fmt.Sprintf("%s.pxar:%s", proxmox.NormalizeHostname(backup.Target.String()), srcPath),
		"--repository", backupStore,
		detectionMode,
		"--entries-max", fmt.Sprintf("%d", backup.MaxDirEntries+1024),
		"--backup-id", backupId,
		"--crypt-mode=none",
	}...)

	addExclusion := func(path string) {
		if !strings.HasPrefix(path, "/") && !strings.HasPrefix(path, "!") && !strings.HasPrefix(path, "**/") {
			path = "**/" + path
		}
		cmdArgs = append(cmdArgs, "--exclude", path)
	}

	for _, exclusion := range extraExclusions {
		addExclusion(exclusion)
	}

	for _, exclusion := range backup.Exclusions {
		addExclusion(exclusion.Path)
	}

	if globalExclusions, err := storeInstance.Database.GetAllGlobalExclusions(); err == nil {
		for _, exclusion := range globalExclusions {
			addExclusion(exclusion.Path)
		}
	}

	if backup.Namespace != "" {
		_ = CreateNamespace(backup.Namespace, backup, storeInstance)
		cmdArgs = append(cmdArgs, "--ns", backup.Namespace)
	}

	env := append(os.Environ(), fmt.Sprintf("PBS_PASSWORD=%s", proxmox.GetToken()))
	if pbsStatus, err := proxmox.GetProxmoxCertInfo(); err == nil {
		env = append(env, fmt.Sprintf("PBS_FINGERPRINT=%s", pbsStatus.FingerprintSHA256))
	}

	cmd := exec.CommandContext(ctx, "/usr/bin/prlimit", cmdArgs...)
	cmd.Env = env

	if err := CleanUnfinishedSnapshot(backup, backupId); err != nil {
		syslog.L.Error(err).WithJob(backup.ID).WithField("backupId", backupId).Write()
	}

	return cmd, nil
}
