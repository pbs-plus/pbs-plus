//go:build linux

package store

import (
	"fmt"

	"github.com/sonroyaalmerol/pbs-plus/internal/arpc"
	"github.com/sonroyaalmerol/pbs-plus/internal/auth/certificates"
	arpcfs "github.com/sonroyaalmerol/pbs-plus/internal/backend/arpc"
	"github.com/sonroyaalmerol/pbs-plus/internal/store/constants"
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

	store := &Store{
		LegacyDatabase:     legacy,
		Database:           db,
		arpcFS:             safemap.New[string, *arpcfs.ARPCFS](),
		ARPCSessionManager: arpc.NewSessionManager(),
	}

	return store, nil
}

func (s *Store) MigrateLegacyData() error {
	if s.LegacyDatabase == nil {
		return nil
	}

	syslog.L.Info().WithMessage("Legacy database format detected, attempting to migrate automatically...").Write()

	syslog.L.Info().WithMessage("Migrating legacy jobs...").Write()
	tx, err := s.Database.NewTransaction()
	if err != nil {
		return fmt.Errorf("MigrateLegacyData: error creating transaction: %w", err)
	}

	// Migrate Jobs
	legacyJobs, err := s.LegacyDatabase.GetAllJobs()
	if err != nil {
		return fmt.Errorf("MigrateLegacyData: error retrieving legacy jobs: %w", err)
	}
	for _, job := range legacyJobs {
		if err := s.Database.CreateJob(tx, job); err != nil {
			syslog.L.Error(err).WithField("job", job.ID).Write()
		}
	}

	err = tx.Commit()
	if err != nil {
		return err
	}

	syslog.L.Info().WithMessage("Migrating legacy exclusions...").Write()
	tx, err = s.Database.NewTransaction()
	if err != nil {
		return fmt.Errorf("MigrateLegacyData: error creating transaction: %w", err)
	}

	defaultExclusionsMap := make(map[string]struct{})
	for _, defaultExc := range constants.DefaultExclusions {
		defaultExclusionsMap[defaultExc] = struct{}{}
	}

	// Migrate Global Exclusions
	legacyGlobals, err := s.LegacyDatabase.GetAllGlobalExclusions()
	if err != nil {
		return fmt.Errorf("MigrateLegacyData: error retrieving legacy global exclusions: %w", err)
	}
	for _, excl := range legacyGlobals {
		if _, ok := defaultExclusionsMap[excl.Path]; ok {
			continue
		}

		if err := s.Database.CreateExclusion(tx, excl); err != nil {
			syslog.L.Error(err).WithField("exclusion", excl.Path).Write()
		}
	}

	err = tx.Commit()
	if err != nil {
		return err
	}

	syslog.L.Info().WithMessage("Migrating legacy targets...").Write()
	tx, err = s.Database.NewTransaction()
	if err != nil {
		return fmt.Errorf("MigrateLegacyData: error creating transaction: %w", err)
	}

	// Migrate Targets
	legacyTargets, err := s.LegacyDatabase.GetAllTargets()
	if err != nil {
		return fmt.Errorf("MigrateLegacyData: error retrieving legacy targets: %w", err)
	}
	for _, target := range legacyTargets {
		if err := s.Database.CreateTarget(tx, target); err != nil {
			syslog.L.Error(err).WithField("target", target.Name).Write()
		}
	}

	err = tx.Commit()
	if err != nil {
		return err
	}

	syslog.L.Info().WithMessage("Verifying jobs migration...").Write()
	newJobs, err := s.Database.GetAllJobs()
	if err != nil {
		return fmt.Errorf("MigrateLegacyData: error retrieving new jobs: %w", err)
	}

	if len(legacyJobs) != len(newJobs) {
		return fmt.Errorf("MigrateLegacyData: legacyJobs != newJobs: %d != %d", len(legacyJobs), len(newJobs))
	}

	syslog.L.Info().WithMessage("Verifying exclusions migration...").Write()
	newGlobals, err := s.Database.GetAllGlobalExclusions()
	if err != nil {
		return fmt.Errorf("MigrateLegacyData: error retrieving new globals: %w", err)
	}

	if len(legacyGlobals) != len(newGlobals) {
		return fmt.Errorf("MigrateLegacyData: legacyGlobals != newGlobals: %d != %d", len(legacyGlobals), len(newGlobals))
	}

	syslog.L.Info().WithMessage("Verifying targets migration...").Write()
	newTargets, err := s.Database.GetAllTargets()
	if err != nil {
		return fmt.Errorf("MigrateLegacyData: error retrieving new targets: %w", err)
	}

	if len(legacyTargets) != len(newTargets) {
		return fmt.Errorf("MigrateLegacyData: legacyTargets != newTargets : %d != %d", len(legacyTargets), len(newTargets))
	}

	syslog.L.Info().WithMessage("Deleting legacy database directories...").Write()

	syslog.L.Info().WithMessage("PBS Plus has successfully migrated your legacy database to the newer model. Legacy databases has been deleted: /etc/proxmox-backup/pbs-plus/[jobs.d, targets.d, exclusions.d, tokens.d]").Write()

	return nil
}
