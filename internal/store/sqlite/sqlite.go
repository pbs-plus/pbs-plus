//go:build linux

package sqlite

import (
	"context"
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
	readDb       *sql.DB
	writeDb      *sql.DB
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

	readDb, err := sql.Open("sqlite", dbPath+"?mode=ro")
	if err != nil {
		return nil, fmt.Errorf("Initialize: error opening DB: %w", err)
	}

	writeDb, err := sql.Open("sqlite", dbPath+"?mode=rw")
	if err != nil {
		return nil, fmt.Errorf("Initialize: error opening DB: %w", err)
	}
	writeDb.SetMaxOpenConns(1)

	_, err = writeDb.Exec("PRAGMA journal_mode=WAL;")
	if err != nil {
		return nil, fmt.Errorf("Initialize: error DB: %w", err)
	}

	database := &Database{
		dbPath:  dbPath,
		readDb:  readDb,
		writeDb: writeDb,
	}

	// Auto migrate on initialization
	if err := database.Migrate(); err != nil {
		return nil, fmt.Errorf("Initialize: error migrating tables: %w", err)
	}

	tx, err := writeDb.Begin()
	if err != nil {
		return nil, fmt.Errorf("Initialize: error migrating tables: %w", err)
	}

	// Insert default (global) exclusions if they are not present.
	for _, exclusion := range constants.DefaultExclusions {
		err = database.CreateExclusion(tx, types.Exclusion{
			Path:    exclusion,
			Comment: "Generated exclusion from default list",
		})
		if err != nil && !errors.Is(err, sql.ErrNoRows) {
			syslog.L.Error(err).WithField("path", exclusion).Write()
		}
	}

	err = tx.Commit()
	if err != nil {
		return nil, fmt.Errorf("Initialize: error migrating tables: %w", err)
	}

	return database, nil
}

func (d *Database) NewTransaction() (*sql.Tx, error) {
	return d.writeDb.BeginTx(context.Background(), &sql.TxOptions{})
}
