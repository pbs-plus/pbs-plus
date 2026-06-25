//go:build linux

package backup

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"strings"

	"github.com/pbs-plus/pbs-plus/internal/conf"
	"github.com/pbs-plus/pbs-plus/internal/log"
	"github.com/pbs-plus/pbs-plus/internal/proxmox"
	"github.com/pbs-plus/pbs-plus/internal/proxmox/cli"
	"github.com/pbs-plus/pbs-plus/internal/server/database"
	"github.com/pbs-plus/pbs-plus/internal/server/store"
)

func getBackupId(target database.Target) (string, error) {
	if target.IsAgent() {
		if target.Name == "" {
			return "", fmt.Errorf("target name is required for agent backup")
		}
		return target.GetHostname(), nil
	}

	hostname, err := os.Hostname()
	if err != nil {
		hostnameBytes, err := os.ReadFile("/etc/hostname")
		if err != nil {
			log.Error(err, "")
		}
		hostname = strings.TrimSpace(string(hostnameBytes))
		if hostname == "" {
			hostname = "localhost"
		}
	}
	return hostname, nil
}

func prepareBackupCommand(ctx context.Context, backup database.Backup, storeInstance *store.Store, srcPath string, isAgent bool, extraExclusions []string, logger *log.Logger) (*exec.Cmd, error) {
	if srcPath == "" {
		return nil, fmt.Errorf("RunBackup: source path is required")
	}

	backupID, err := getBackupId(backup.Target)
	if err != nil {
		return nil, fmt.Errorf("RunBackup: failed to get backup ID: %w", err)
	}
	backupID = proxmox.NormalizeHostname(backupID)

	backupStore := fmt.Sprintf("%s@localhost:%s", proxmox.AuthID, backup.Store)
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
	if nofile := conf.Env.ClientNofile; nofile != "" {
		cmdArgs = append(cmdArgs, fmt.Sprintf("--nofile=%s", nofile))
	} else {
		cmdArgs = append(cmdArgs, "--nofile=1024:1024")
	}

	cmdArgs = append(cmdArgs, []string{
		"/usr/bin/proxmox-backup-client",
		"backup",
		fmt.Sprintf("%s.pxar:%s", proxmox.NormalizeHostname(backup.Target.Name), srcPath),
		"--repository", backupStore,
		detectionMode,
		"--entries-max", fmt.Sprintf("%d", backup.MaxDirEntries+1024),
		"--backup-id", backupID,
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
		if err := CreateNamespace(backup.Namespace, backup, storeInstance); err != nil {
			logger.Error(err, "")
		}
		cmdArgs = append(cmdArgs, "--ns", backup.Namespace)
	}

	env := append(os.Environ(), fmt.Sprintf("PBS_PASSWORD=%s", cli.GetToken()))
	if pbsStatus, err := cli.GetProxmoxCertInfo(); err == nil {
		env = append(env, fmt.Sprintf("PBS_FINGERPRINT=%s", pbsStatus.FingerprintSHA256))
	}

	cmd := exec.CommandContext(ctx, "/usr/bin/prlimit", cmdArgs...)
	cmd.Env = env

	if err := CleanUnfinishedSnapshot(backup, backupID); err != nil {
		logger.Error(err, "")
	}

	return cmd, nil
}
