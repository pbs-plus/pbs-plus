//go:build linux

package store

import (
	"fmt"
	"os"

	"github.com/sonroyaalmerol/pbs-plus/internal/arpc"
	"github.com/sonroyaalmerol/pbs-plus/internal/auth/certificates"
	arpcfs "github.com/sonroyaalmerol/pbs-plus/internal/backend/arpc"
	"github.com/sonroyaalmerol/pbs-plus/internal/store/database"
	"github.com/sonroyaalmerol/pbs-plus/internal/store/sqlite"
	"github.com/sonroyaalmerol/pbs-plus/internal/syslog"
	"github.com/sonroyaalmerol/pbs-plus/internal/utils/safemap"

	_ "modernc.org/sqlite"
)

// Store holds the configuration system.
type Store struct {
	CertGenerator      *certificates.Generator
	LegacyDatabase     *database.Database
	Database           *sqlite.Database
	ARPCSessionManager *arpc.SessionManager
	arpcFS             *safemap.Map[string, *arpcfs.ARPCFS]
}

func Initialize(paths map[string]string) (*Store, error) {
	sqlitePath := ""
	if paths != nil {
		sqlitePathTmp, ok := paths["sqlite"]
		if ok {
			sqlitePath = sqlitePathTmp
		}
	}

	db, err := sqlite.Initialize(sqlitePath)
	if err != nil {
		return nil, fmt.Errorf("Initialize: error initializing database -> %w", err)
	}

	legacy, err := database.Initialize(paths)
	if err != nil {
		return nil, fmt.Errorf("Initialize: error initializing database -> %w", err)
	}

	if legacy != nil {
		syslog.L.Info().WithMessage("Legacy database format detected, attempting to migrate automatically...").Write()

		if err = migrateLegacyData(legacy, db); err != nil {
			return nil, fmt.Errorf("Initialize: error migrating legacy database -> %w", err)
		}

		syslog.L.Info().WithMessage("PBS Plus has successfully migrated your legacy database to the newer model. Legacy databases has been deleted: /etc/proxmox-backup/pbs-plus/[jobs.d, targets.d, exclusions.d, tokens.d]").Write()
	}

	store := &Store{
		Database:           db,
		arpcFS:             safemap.New[string, *arpcfs.ARPCFS](),
		ARPCSessionManager: arpc.NewSessionManager(),
	}

	return store, nil
}

func migrateLegacyData(legacy *database.Database, newDb *sqlite.Database) error {
	tx, err := newDb.NewTransaction()
	if err != nil {
		return fmt.Errorf("MigrateLegacyData: error creating transaction: %w", err)
	}

	// Migrate Jobs
	legacyJobs, err := legacy.GetAllJobs()
	if err != nil {
		return fmt.Errorf("MigrateLegacyData: error retrieving legacy jobs: %w", err)
	}
	for _, job := range legacyJobs {
		if err := newDb.CreateJob(tx, job); err != nil {
			syslog.L.Error(err).WithField("job", job.ID).Write()
		}
	}

	// Migrate Global Exclusions
	legacyGlobals, err := legacy.GetAllGlobalExclusions()
	if err != nil {
		return fmt.Errorf("MigrateLegacyData: error retrieving legacy global exclusions: %w", err)
	}
	for _, excl := range legacyGlobals {
		if err := newDb.CreateExclusion(tx, excl); err != nil {
			syslog.L.Error(err).WithField("exclusion", excl.Path).Write()
		}
	}

	// Migrate Targets
	legacyTargets, err := legacy.GetAllTargets()
	if err != nil {
		return fmt.Errorf("MigrateLegacyData: error retrieving legacy targets: %w", err)
	}
	for _, target := range legacyTargets {
		if err := newDb.CreateTarget(tx, target); err != nil {
			syslog.L.Error(err).WithField("target", target.Name).Write()
		}
	}

	// Migrate Tokens
	legacyTokens, err := legacy.GetAllTokens()
	if err != nil {
		return fmt.Errorf("MigrateLegacyData: error retrieving legacy tokens: %w", err)
	}
	for _, token := range legacyTokens {
		if err := newDb.MigrateToken(tx, token); err != nil {
			syslog.L.Error(err).WithField("token", token.Token).Write()
		}
	}

	err = tx.Commit()
	if err != nil {
		return err
	}

	newJobs, err := newDb.GetAllJobs()
	if err != nil {
		return fmt.Errorf("MigrateLegacyData: error retrieving new jobs: %w", err)
	}

	if len(legacyJobs) != len(newJobs) {
		return fmt.Errorf("MigrateLegacyData: legacyJobs != newJobs: %d != %d", len(legacyJobs), len(newJobs))
	}

	newGlobals, err := newDb.GetAllGlobalExclusions()
	if err != nil {
		return fmt.Errorf("MigrateLegacyData: error retrieving new globals: %w", err)
	}

	if len(legacyGlobals) != len(newGlobals) {
		return fmt.Errorf("MigrateLegacyData: legacyGlobals != newGlobals: %d != %d", len(legacyGlobals), len(newGlobals))
	}

	newTargets, err := newDb.GetAllTargets()
	if err != nil {
		return fmt.Errorf("MigrateLegacyData: error retrieving new targets: %w", err)
	}

	if len(legacyTargets) != len(newTargets) {
		return fmt.Errorf("MigrateLegacyData: legacyTargets != newTargets : %d != %d", len(legacyTargets), len(newTargets))
	}

	newTokens, err := newDb.GetAllTokens()
	if err != nil {
		return fmt.Errorf("MigrateLegacyData: error retrieving new tokens: %w", err)
	}

	if len(legacyTokens) != len(newTokens) {
		return fmt.Errorf("MigrateLegacyData: legacyTokens != newTokens: %d != %d", len(legacyTokens), len(newTokens))
	}

	_ = os.RemoveAll("/etc/proxmox-backup/pbs-plus/jobs.d")
	_ = os.RemoveAll("/etc/proxmox-backup/pbs-plus/targets.d")
	_ = os.RemoveAll("/etc/proxmox-backup/pbs-plus/exclusions.d")
	_ = os.RemoveAll("/etc/proxmox-backup/pbs-plus/tokens.d")

	return nil
}
