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
	"github.com/pbs-plus/pbs-plus/internal/log"
	"github.com/pbs-plus/pbs-plus/internal/mtls"
	"github.com/pbs-plus/pbs-plus/internal/server/backup"
	"github.com/pbs-plus/pbs-plus/internal/server/jobs"
	jobsstore "github.com/pbs-plus/pbs-plus/internal/server/jobs/store"
	"github.com/pbs-plus/pbs-plus/internal/server/mtf"
	"github.com/pbs-plus/pbs-plus/internal/server/restore"
	rpcmount "github.com/pbs-plus/pbs-plus/internal/server/rpc"
	"github.com/pbs-plus/pbs-plus/internal/server/scheduler"
	"github.com/pbs-plus/pbs-plus/internal/server/store"
	"github.com/pbs-plus/pbs-plus/internal/server/verification"
)

// and cleanup of stale mount points and queued backups
func Bootstrap(mainCtx context.Context, storeInstance *store.Store) (*scheduler.Scheduler, *jobs.Engine, error) {
	secKeyPath := "/etc/proxmox-backup/pbs-plus/.key"

	if _, err := os.Lstat(secKeyPath); err != nil {
		key, err := crypto.SecureRandomString(48)
		if err != nil {
			log.Error(err, "")
		} else {
			if err := os.WriteFile(secKeyPath, []byte(key), 0o600); err != nil {
				log.Error(err, "")
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
		log.Error(err, "failed to cleanup stale mounts")
	}

	go func() {
		backoff := 100 * time.Millisecond
		const maxBackoff = 30 * time.Second
		for {
			select {
			case <-mainCtx.Done():
				log.Error(mainCtx.Err(), "mount rpc server cancelled")
				return
			default:
				if err := rpcmount.RunRPCServer(mainCtx, conf.MountSocketPath, storeInstance); err != nil {
					log.Error(err, "mount rpc server failed, restarting")
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

	engineDB, err := jobsstore.Open("")
	if err != nil {
		return nil, nil, fmt.Errorf("opening workflow engine database: %w", err)
	}
	engine, err := jobs.NewEngine(engineDB, jobs.EngineConfig{MaxConcurrent: conf.MaxConcurrentClients})
	if err != nil {
		return nil, nil, fmt.Errorf("creating workflow engine: %w", err)
	}
	if err := registerWorkflows(engine, storeInstance); err != nil {
		return nil, nil, fmt.Errorf("registering workflow runners: %w", err)
	}
	if err := engine.Start(mainCtx); err != nil {
		return nil, nil, fmt.Errorf("starting workflow engine: %w", err)
	}
	storeInstance.Engine = engine
	s := scheduler.NewScheduler(mainCtx, storeInstance)
	s.Start()
	storeInstance.OnBackupComplete = s.TriggerPendingVerifications

	go func() {
		backoff := 100 * time.Millisecond
		const maxBackoff = 30 * time.Second
		for {
			select {
			case <-mainCtx.Done():
				log.Error(mainCtx.Err(), "backup rpc server cancelled")
				return
			default:
				if err := rpcmount.RunJobRPCServer(mainCtx, conf.JobMutateSocketPath, engine, storeInstance); err != nil {
					log.Error(err, "backup rpc server failed, restarting")
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

	return s, engine, nil
}

func registerWorkflows(engine *jobs.Engine, storeInstance *store.Store) error {
	if err := backup.Register(engine, storeInstance); err != nil {
		return fmt.Errorf("registering backup workflow: %w", err)
	}
	if err := restore.Register(engine, storeInstance); err != nil {
		return fmt.Errorf("registering restore workflow: %w", err)
	}
	if err := verification.Register(engine, storeInstance); err != nil {
		return fmt.Errorf("registering verification workflow: %w", err)
	}
	if err := mtf.RegisterMigration(engine, storeInstance); err != nil {
		return fmt.Errorf("registering mtf migration workflow: %w", err)
	}
	if err := mtf.RegisterScan(engine, storeInstance); err != nil {
		return fmt.Errorf("registering mtf scan workflow: %w", err)
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
			log.Error(err, "failed to unmount some mounted agents")
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
