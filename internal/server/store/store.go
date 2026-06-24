//go:build linux

package store

import (
	"context"
	"fmt"
	"time"

	"github.com/pbs-plus/pbs-plus/internal/arpc"
	"github.com/pbs-plus/pbs-plus/internal/mtls"
	"github.com/pbs-plus/pbs-plus/internal/safemap"
	"github.com/pbs-plus/pbs-plus/internal/server/application"
	sqlite "github.com/pbs-plus/pbs-plus/internal/server/database"
	"github.com/pbs-plus/pbs-plus/internal/server/jobs"
	mtfdb "github.com/pbs-plus/pbs-plus/internal/server/mtf/store"
	"github.com/pbs-plus/pbs-plus/internal/server/notification"
	arpcfs "github.com/pbs-plus/pbs-plus/internal/server/vfs/arpcfs"
	"github.com/pbs-plus/pbs-plus/internal/syslog"

	_ "modernc.org/sqlite"
)

type Store struct {
	Ctx               context.Context
	Database          *sqlite.Database
	MtfStore          *mtfdb.Database
	MtfMapper         *mtfdb.Mapper
	BackupSvc         *application.BackupService
	RestoreSvc        *application.RestoreService
	ExclusionSvc      *application.ExclusionService
	AgentHostSvc      *application.AgentHostService
	TokenSvc          *application.TokenService
	ScriptSvc         *application.ScriptService
	TargetSvc         *application.TargetService
	VerificationSvc   *application.VerificationService
	ARPCAgentsManager *arpc.AgentsManager
	Manager           *jobs.Manager
	BatchTracker      *notification.BatchTracker
	AlertScanner      *notification.AlertScanner
	OnBackupComplete  func(backupJobID string) // called after backup completion to trigger pending verifications
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
	verificationSvc := application.NewVerificationService(db)

	mtfDB, err := mtfdb.Initialize(ctx, "")
	if err != nil {
		syslog.L.Error(err).WithMessage("Initialize: mtf store").Write()
	}
	var mtfMapper *mtfdb.Mapper
	if mtfDB != nil {
		mtfMapper = mtfdb.NewMapper(mtfDB)
	}

	go func() {
		defer func() {
			if r := recover(); r != nil {
				syslog.L.Error(fmt.Errorf("store initialization panic: %v", r)).
					WithMessage("Initialize: GetAllBackups panicked").Write()
			}
		}()
		if _, err := db.GetAllBackups(); err != nil {
			syslog.L.Error(err).WithMessage("Initialize: GetAllBackups failed").Write()
		}
	}()

	store := &Store{
		Ctx:               ctx,
		Database:          db,
		MtfStore:          mtfDB,
		MtfMapper:         mtfMapper,
		BackupSvc:         backupSvc,
		RestoreSvc:        restoreSvc,
		ExclusionSvc:      exclusionSvc,
		AgentHostSvc:      agentHostSvc,
		TokenSvc:          tokenSvc,
		ScriptSvc:         scriptSvc,
		TargetSvc:         targetSvc,
		VerificationSvc:   verificationSvc,
		arpcFS:            safemap.New[string, *arpcfs.ARPCFS](),
		ARPCAgentsManager: agentsManager,
		CertManager:       mtls.NewCertManager(),
	}

	store.BatchTracker = notification.NewBatchTracker(db)
	go store.BatchTracker.StartCleanup(ctx, 10*time.Minute)

	notification.InstallTemplates()

	store.AlertScanner = notification.NewAlertScanner(db)
	go store.AlertScanner.Start(ctx, 1*time.Hour)

	return store, nil
}
