//go:build linux

package store

import (
	"context"
	"fmt"

	"github.com/pbs-plus/pbs-plus/internal/arpc"
	"github.com/pbs-plus/pbs-plus/internal/mtls"
	"github.com/pbs-plus/pbs-plus/internal/safemap"
	"github.com/pbs-plus/pbs-plus/internal/server/application"
	sqlite "github.com/pbs-plus/pbs-plus/internal/server/database"
	"github.com/pbs-plus/pbs-plus/internal/server/jobs"
	arpcfs "github.com/pbs-plus/pbs-plus/internal/server/vfs/arpcfs"
	"github.com/pbs-plus/pbs-plus/internal/syslog"

	_ "modernc.org/sqlite"
)

type Store struct {
	Ctx               context.Context
	Database          *sqlite.Database
	BackupSvc         *application.BackupService
	RestoreSvc        *application.RestoreService
	ExclusionSvc      *application.ExclusionService
	AgentHostSvc      *application.AgentHostService
	TokenSvc          *application.TokenService
	ScriptSvc         *application.ScriptService
	TargetSvc         *application.TargetService
	ARPCAgentsManager *arpc.AgentsManager
	Manager           *jobs.Manager
	arpcFS            *safemap.Map[string, *arpcfs.ARPCFS]
	CertManager       *mtls.CertManager
}

func Initialize(ctx context.Context, paths map[string]string) (*Store, error) {
	sqlitePath := ""
	if paths != nil {
		sqlitePathTmp, ok := paths["sqlite"]
		if ok {
			sqlitePath = sqlitePathTmp
		}
	}

	db, err := sqlite.Initialize(ctx, sqlitePath)
	if err != nil {
		return nil, fmt.Errorf("Initialize: error initializing database -> %w", err)
	}

	agentsManager := arpc.NewAgentsManager()
	backupSvc := application.NewBackupService(db)
	restoreSvc := application.NewRestoreService(db)
	exclusionSvc := application.NewExclusionService(db)
	agentHostSvc := application.NewAgentHostService(db)
	tokenSvc := application.NewTokenService(db)
	scriptSvc := application.NewScriptService(db)
	targetSvc := application.NewTargetService(db, agentsManager)

	go func() {
		defer func() {
			if r := recover(); r != nil {
				syslog.L.Error(fmt.Errorf("store initialization panic: %v", r)).
					WithMessage("Initialize: GetAllBackups panicked").Write()
			}
		}()
		_, _ = db.GetAllBackups()
	}()

	store := &Store{
		Ctx:               ctx,
		Database:          db,
		BackupSvc:         backupSvc,
		RestoreSvc:        restoreSvc,
		ExclusionSvc:      exclusionSvc,
		AgentHostSvc:      agentHostSvc,
		TokenSvc:          tokenSvc,
		ScriptSvc:         scriptSvc,
		TargetSvc:         targetSvc,
		arpcFS:            safemap.New[string, *arpcfs.ARPCFS](),
		ARPCAgentsManager: agentsManager,
		CertManager:       mtls.NewCertManager(),
	}

	return store, nil
}
