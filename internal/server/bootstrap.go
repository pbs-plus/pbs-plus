//go:build linux

package server

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"time"

	"github.com/pbs-plus/pbs-plus/internal/conf"
	"github.com/pbs-plus/pbs-plus/internal/crypto"
	"github.com/pbs-plus/pbs-plus/internal/mtls"
	"github.com/pbs-plus/pbs-plus/internal/proxmox"
	"github.com/pbs-plus/pbs-plus/internal/server/backup"
	"github.com/pbs-plus/pbs-plus/internal/server/jobs"
	"github.com/pbs-plus/pbs-plus/internal/server/mtf"
	"github.com/pbs-plus/pbs-plus/internal/server/restore"
	job "github.com/pbs-plus/pbs-plus/internal/server/rpc"
	rpcmount "github.com/pbs-plus/pbs-plus/internal/server/rpc"
	"github.com/pbs-plus/pbs-plus/internal/server/scheduler"
	"github.com/pbs-plus/pbs-plus/internal/server/store"
	"github.com/pbs-plus/pbs-plus/internal/syslog"
)

// and cleanup of stale mount points and queued backups
func Bootstrap(mainCtx context.Context, storeInstance *store.Store) (*scheduler.Scheduler, *jobs.Manager, error) {
	if err := cleanupQueuedBackups(storeInstance); err != nil {
		syslog.L.Error(err).WithMessage("failed to cleanup queued backups").Write()
	}

	secKeyPath := "/etc/proxmox-backup/pbs-plus/.key"

	if _, err := os.Lstat(secKeyPath); err != nil {
		key, err := crypto.SecureRandomString(48)
		if err != nil {
			syslog.L.Error(err).Write()
		} else {
			if err := os.WriteFile(secKeyPath, []byte(key), 0640); err != nil {
				syslog.L.Error(err).Write()
			}
		}
	}

	secKey, err := os.ReadFile(secKeyPath)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to read .key: %w", err)
	}

	err = storeInstance.CertManager.Validate()
	if err != nil {
		return nil, nil, fmt.Errorf("failed to generate local CA and server cert: %w", err)
	}

	// Initialize token manager
	tokenManager, err := mtls.NewTokenManager(mtls.TokenConfig{
		TokenExpiration: conf.AuthTokenExpiration,
		SecretKey:       string(secKey),
	})
	if err != nil {
		return nil, nil, fmt.Errorf("failed to initialize token manager: %w", err)
	}
	storeInstance.Database.TokenManager = tokenManager

	// Stale mount cleanup - unmount and remove all stale mount points
	if err := cleanupStaleMounts(); err != nil {
		syslog.L.Error(err).WithMessage("failed to cleanup stale mounts").Write()
	}

	go func() {
		backoff := 100 * time.Millisecond
		const maxBackoff = 30 * time.Second
		for {
			select {
			case <-mainCtx.Done():
				syslog.L.Error(mainCtx.Err()).WithMessage("mount rpc server cancelled").Write()
				return
			default:
				if err := rpcmount.RunRPCServer(mainCtx, conf.MountSocketPath, storeInstance); err != nil {
					syslog.L.Error(err).WithMessage("mount rpc server failed, restarting").Write()
					time.Sleep(backoff)
					backoff *= 2
					if backoff > maxBackoff {
						backoff = maxBackoff
					}
				} else {
					backoff = 100 * time.Millisecond
				}
			}
		}
	}()

	manager := jobs.NewManager(mainCtx, conf.MaxConcurrentClients, func() int {
		n, err := storeInstance.Database.JobCount(mainCtx)
		if err != nil || n < 1 {
			return 100
		}
		return n
	})
	storeInstance.Manager = manager
	s := scheduler.NewScheduler(mainCtx, storeInstance, manager)
	s.Start()
	storeInstance.OnBackupComplete = s.TriggerPendingVerifications

	go func() {
		backoff := 100 * time.Millisecond
		const maxBackoff = 30 * time.Second
		for {
			select {
			case <-mainCtx.Done():
				syslog.L.Error(mainCtx.Err()).WithMessage("backup rpc server cancelled").Write()
				return
			default:
				job.BackupJobFactory = backup.NewBackupJob
				job.RestoreJobFactory = restore.NewRestoreJob
				job.MtfJobFactory = mtf.NewJob
				if err := job.RunJobRPCServer(mainCtx, conf.JobMutateSocketPath, manager, storeInstance); err != nil {
					syslog.L.Error(err).WithMessage("backup rpc server failed, restarting").Write()
					time.Sleep(backoff)
					backoff *= 2
					if backoff > maxBackoff {
						backoff = maxBackoff
					}
				} else {
					backoff = 100 * time.Millisecond
				}
			}
		}
	}()

	return s, manager, nil
}

func cleanupQueuedBackups(storeInstance *store.Store) error {
	queuedBackups, err := storeInstance.Database.GetAllQueuedBackups()
	if err != nil {
		return fmt.Errorf("failed to get all queued backups: %w", err)
	}

	tx, err := storeInstance.Database.NewTransaction()
	if err != nil {
		return fmt.Errorf("failed to create transaction: %w", err)
	}

	for _, queuedBackup := range queuedBackups {
		task, err := backup.GenerateBackupTaskErrorFile(queuedBackup, fmt.Errorf("server was restarted before backup started during queue"), nil)
		if err != nil {
			continue
		}

		queueTaskPath, err := proxmox.GetLogPath(queuedBackup.History.LastRunUpid)
		if err == nil {
			if err := os.Remove(queueTaskPath); err != nil && !os.IsNotExist(err) {
				syslog.L.Error(err).Write()
			}
		}

		queuedBackup.History.LastRunUpid = task.UPID
		err = storeInstance.Database.UpdateBackup(tx, queuedBackup)
		if err != nil {
			continue
		}
	}

	if err := tx.Commit(); err != nil {
		syslog.L.Error(err).Write()
	}
	return nil
}

func cleanupStaleMounts() error {
	mountPoints, err := filepath.Glob(filepath.Join(conf.AgentMountBasePath, "*"))
	if err != nil {
		return fmt.Errorf("failed to find agent mount base path: %w", err)
	}

	for _, mountPoint := range mountPoints {
		umount := exec.Command("umount", "-lf", mountPoint)
		umount.Env = os.Environ()
		if err := umount.Run(); err != nil {
			syslog.L.Error(err).WithMessage("failed to unmount some mounted agents").Write()
		}
	}

	if err := os.RemoveAll(conf.AgentMountBasePath); err != nil {
		return fmt.Errorf("failed to remove directory: %w", err)
	}

	if err := os.Mkdir(conf.AgentMountBasePath, 0700); err != nil {
		return fmt.Errorf("failed to recreate directory: %w", err)
	}

	return nil
}
