//go:build linux

package application

import (
	"context"
	"fmt"
	"time"

	"github.com/pbs-plus/pbs-plus/internal/arpc"
	"github.com/pbs-plus/pbs-plus/internal/log"
	"github.com/pbs-plus/pbs-plus/internal/mtls"
	"github.com/pbs-plus/pbs-plus/internal/safemap"
	"github.com/pbs-plus/pbs-plus/internal/server/coredb"
	"github.com/pbs-plus/pbs-plus/internal/server/jobs"
	"github.com/pbs-plus/pbs-plus/internal/server/mtf/mtfdb"
	"github.com/pbs-plus/pbs-plus/internal/server/notification"
	arpcfs "github.com/pbs-plus/pbs-plus/internal/server/vfs/arpcfs"

	_ "modernc.org/sqlite"
)

type Runtime struct {
	Ctx              context.Context
	CoreDB           *coredb.Store
	MtfDB            *mtfdb.Store
	MtfMapper        *mtfdb.Mapper
	Backup           *BackupService
	Restore          *RestoreService
	Exclusion        *ExclusionService
	AgentHost        *AgentHostService
	Token            *TokenService
	Script           *ScriptService
	Target           *TargetService
	Verification     *VerificationService
	Agents           *arpc.AgentsManager
	Engine           *jobs.Engine
	BatchTracker     *notification.BatchTracker
	AlertScanner     *notification.AlertScanner
	OnBackupComplete func(backupJobID string) // called after backup completion to trigger pending verifications
	arpcFS           *safemap.Map[string, *arpcfs.ARPCFS]
	CertManager      *mtls.CertManager
}

func New(ctx context.Context, paths map[string]string) (*Runtime, error) {
	sqlitePath := ""
	if paths != nil {
		sqlitePathTmp, ok := paths["sqlite"]
		if ok {
			sqlitePath = sqlitePathTmp
		}
	}

	db, err := coredb.Initialize(ctx, sqlitePath)
	if err != nil {
		return nil, fmt.Errorf("Initialize: error initializing database -> %w", err)
	}

	agentsManager := arpc.NewAgentsManager()
	backupSvc := NewBackupService(db)
	restoreSvc := NewRestoreService(db)
	exclusionSvc := NewExclusionService(db)
	agentHostSvc := NewAgentHostService(db)
	tokenSvc := NewTokenService(db)
	scriptSvc := NewScriptService(db)
	targetSvc := NewTargetService(db, agentsManager)
	verificationSvc := NewVerificationService(db)

	mtfDB, err := mtfdb.Initialize(ctx, "")
	if err != nil {
		log.Error(err, "Initialize: mtf store")
	}
	var mtfMapper *mtfdb.Mapper
	if mtfDB != nil {
		mtfMapper = mtfdb.NewMapper(mtfDB)
	}

	go func() {
		defer func() {
			if r := recover(); r != nil {
				log.Error(fmt.Errorf("store initialization panic: %v", r),
					"Initialize: GetAllBackups panicked")
			}
		}()
		if _, err := db.GetAllBackups(); err != nil {
			log.Error(err, "Initialize: GetAllBackups failed")
		}
	}()

	store := &Runtime{
		Ctx:          ctx,
		CoreDB:       db,
		MtfDB:        mtfDB,
		MtfMapper:    mtfMapper,
		Backup:       backupSvc,
		Restore:      restoreSvc,
		Exclusion:    exclusionSvc,
		AgentHost:    agentHostSvc,
		Token:        tokenSvc,
		Script:       scriptSvc,
		Target:       targetSvc,
		Verification: verificationSvc,
		arpcFS:       safemap.New[string, *arpcfs.ARPCFS](),
		Agents:       agentsManager,
		CertManager:  mtls.NewCertManager(),
	}

	store.BatchTracker = notification.NewBatchTracker(db)
	go store.BatchTracker.Run(ctx, 30*time.Second)

	notification.InstallTemplates()

	store.AlertScanner = notification.NewAlertScanner(db)
	go store.AlertScanner.Start(ctx, 1*time.Hour)

	return store, nil
}

func (s *Runtime) Close() error {
	if s.Engine != nil {
		s.Engine.Close()
	}
	if s.CoreDB != nil {
		return s.CoreDB.Close()
	}
	return nil
}
