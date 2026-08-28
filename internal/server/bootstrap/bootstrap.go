//go:build linux

package bootstrap

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
	"github.com/pbs-plus/pbs-plus/internal/server/application"
	"github.com/pbs-plus/pbs-plus/internal/server/backup"
	"github.com/pbs-plus/pbs-plus/internal/server/jobs"
	"github.com/pbs-plus/pbs-plus/internal/server/jobs/jobdb"
	"github.com/pbs-plus/pbs-plus/internal/server/mtf"
	"github.com/pbs-plus/pbs-plus/internal/server/restore"
	"github.com/pbs-plus/pbs-plus/internal/server/rpc/jobrpc"
	"github.com/pbs-plus/pbs-plus/internal/server/rpc/mountrpc"
	"github.com/pbs-plus/pbs-plus/internal/server/scheduler"
	"github.com/pbs-plus/pbs-plus/internal/server/snapshotmount"
	"github.com/pbs-plus/pbs-plus/internal/server/verification"
)

// and cleanup of stale mount points and queued backups
func Run(mainCtx context.Context, app *application.Runtime) (*scheduler.Scheduler, *jobs.Engine, error) {
	setMemLimit()
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

	err = app.CertManager.Validate()
	if err != nil {
		return nil, nil, fmt.Errorf("failed to generate local CA and server cert: %w", err)
	}

	// Initialize token manager
	tokenManager, err := crypto.NewTokenManager(crypto.TokenConfig{
		TokenExpiration: conf.AuthTokenExpiration,
		SecretKey:       string(secKey),
	})
	if err != nil {
		return nil, nil, fmt.Errorf("failed to initialize token manager: %w", err)
	}
	app.CoreDB.TokenManager = tokenManager

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
				if err := mountrpc.RunServer(mainCtx, conf.MountSocketPath, app); err != nil {
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

	engineDB, err := jobdb.Open("")
	if err != nil {
		return nil, nil, fmt.Errorf("opening workflow engine database: %w", err)
	}
	engine, err := jobs.NewEngine(engineDB, jobs.EngineConfig{MaxConcurrent: conf.MaxConcurrentClients})
	if err != nil {
		return nil, nil, fmt.Errorf("creating workflow engine: %w", err)
	}
	if err := registerWorkflows(engine, app); err != nil {
		return nil, nil, fmt.Errorf("registering workflow runners: %w", err)
	}
	if err := engine.Start(mainCtx); err != nil {
		return nil, nil, fmt.Errorf("starting workflow engine: %w", err)
	}
	app.Engine = engine
	go snapshotmount.AutoMountProfiles(mainCtx, engine)
	s := scheduler.NewScheduler(mainCtx, app)
	s.Start()
	app.OnBackupComplete = s.TriggerPendingVerifications

	go func() {
		backoff := 100 * time.Millisecond
		const maxBackoff = 30 * time.Second
		for {
			select {
			case <-mainCtx.Done():
				log.Error(mainCtx.Err(), "backup rpc server cancelled")
				return
			default:
				if err := jobrpc.RunServer(mainCtx, conf.JobMutateSocketPath, engine, app); err != nil {
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

func registerWorkflows(engine *jobs.Engine, app *application.Runtime) error {
	if err := backup.Register(engine, app); err != nil {
		return fmt.Errorf("registering backup workflow: %w", err)
	}
	if err := restore.Register(engine, app); err != nil {
		return fmt.Errorf("registering restore workflow: %w", err)
	}
	if err := verification.Register(engine, app); err != nil {
		return fmt.Errorf("registering verification workflow: %w", err)
	}
	if err := mtf.RegisterMigration(engine, app); err != nil {
		return fmt.Errorf("registering mtf migration workflow: %w", err)
	}
	if err := mtf.RegisterScan(engine, app); err != nil {
		return fmt.Errorf("registering mtf scan workflow: %w", err)
	}
	if err := snapshotmount.Register(engine); err != nil {
		return fmt.Errorf("registering snapshot mount workflows: %w", err)
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
