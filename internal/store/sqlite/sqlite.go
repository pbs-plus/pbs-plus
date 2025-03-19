//go:build linux

package sqlite

import (
	"database/sql"
	"errors"
	"fmt"

	_ "modernc.org/sqlite"

	"github.com/sonroyaalmerol/pbs-plus/internal/auth/token"
	"github.com/sonroyaalmerol/pbs-plus/internal/store/constants"
	"github.com/sonroyaalmerol/pbs-plus/internal/store/types"
	"github.com/sonroyaalmerol/pbs-plus/internal/syslog"
)

const maxAttempts = 100

// Database is our SQLite-backed store.
type Database struct {
	db           *sql.DB
	dbPath       string
	TokenManager *token.Manager
}

// Initialize opens (or creates) the SQLite database at dbPath,
// creates all necessary tables if they do not exist,
// and then (optionally) fills any default items.
// It returns a pointer to a Database instance.
func Initialize(dbPath string) (*Database, error) {
	if dbPath == "" {
		dbPath = "/etc/proxmox-backup/pbs-plus/plus.db"
	}

	// Open (or create) the SQLite database.
	db, err := sql.Open("sqlite", dbPath)
	if err != nil {
		return nil, fmt.Errorf("Initialize: error opening DB: %w", err)
	}

	database := &Database{
		db:     db,
		dbPath: dbPath,
	}

	// Auto migrate on initialization
	if err := database.Migrate(); err != nil {
		return nil, fmt.Errorf("Initialize: error migrating tables: %w", err)
	}

	// Insert default (global) exclusions if they are not present.
	for _, exclusion := range constants.DefaultExclusions {
		err = database.CreateExclusion(types.Exclusion{
			Path:    exclusion,
			Comment: "Generated exclusion from default list",
		})
		if err != nil && !errors.Is(err, sql.ErrNoRows) {
			syslog.L.Error(err).WithField("path", exclusion).Write()
		}
	}

	return database, nil
}
