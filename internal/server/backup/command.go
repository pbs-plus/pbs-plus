//go:build linux

package backup

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"strings"
	"syscall"

	"github.com/pbs-plus/pbs-plus/internal/conf"
	"github.com/pbs-plus/pbs-plus/internal/log"
	"github.com/pbs-plus/pbs-plus/internal/proxmox"
	"github.com/pbs-plus/pbs-plus/internal/proxmox/cli"
	"github.com/pbs-plus/pbs-plus/internal/server/application"
	"github.com/pbs-plus/pbs-plus/internal/server/coredb"
	"github.com/pbs-plus/pbs-plus/internal/validate"
)

// getBackupId returns the PBS backup ID for a job; it is per-job rather than per-target so concurrent backups of one target land in separate snapshot groups with distinct PBS worker IDs.
func getBackupId(backup coredb.Backup) (string, error) {
	if err := validate.ValidateJobId(backup.ID); err != nil {
		return "", fmt.Errorf("invalid job id for backup ID: %w", err)
	}
	return backup.ID, nil
}

func prepareBackupCommand(ctx context.Context, backup coredb.Backup, app *application.Runtime, srcPath string, isAgent bool, extraExclusions []string, logger *log.Logger) (*exec.Cmd, error) {
	if srcPath == "" {
		return nil, fmt.Errorf("RunBackup: source path is required")
	}

	backupID, err := getBackupId(backup)
	if err != nil {
		return nil, fmt.Errorf("RunBackup: failed to get backup ID: %w", err)
	}

	backupStore := fmt.Sprintf("%s@localhost:%s", proxmox.AuthID, backup.Store)
	if backupStore == "@localhost:" {
		return nil, fmt.Errorf("RunBackup: invalid backup store configuration")
	}

	detectionMode, useExclusions := backupCommandPolicy(backup)

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
		"--backup-type", "host",
		"--backup-id", backupID,
		"--crypt-mode=none",
	}...)

	addExclusion := func(path string) {
		if !strings.HasPrefix(path, "/") && !strings.HasPrefix(path, "!") && !strings.HasPrefix(path, "**/") {
			path = "**/" + path
		}
		cmdArgs = append(cmdArgs, "--exclude", path)
	}

	if useExclusions {
		for _, exclusion := range extraExclusions {
			addExclusion(exclusion)
		}

		for _, exclusion := range backup.Exclusions {
			addExclusion(exclusion.Path)
		}

		if globalExclusions, err := app.CoreDB.GetAllGlobalExclusions(); err == nil {
			for _, exclusion := range globalExclusions {
				addExclusion(exclusion.Path)
			}
		}
	}

	if backup.Namespace != "" {
		if err := CreateNamespace(backup.Namespace, backup, app); err != nil {
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
	cmd.SysProcAttr = &syscall.SysProcAttr{Pdeathsig: syscall.SIGTERM}

	if err := CleanUnfinishedSnapshot(backup, backupID); err != nil {
		logger.Error(err, "")
	}

	return cmd, nil
}

func backupCommandPolicy(backup coredb.Backup) (string, bool) {
	if backup.Target.IsDatabase() {
		return "--change-detection-mode=metadata", false
	}
	switch backup.Mode {
	case "legacy":
		return "--change-detection-mode=legacy", true
	case "data":
		return "--change-detection-mode=data", true
	default:
		return "--change-detection-mode=metadata", true
	}
}
