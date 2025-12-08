//go:build linux

package sqlite

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"

	_ "modernc.org/sqlite"

	"github.com/golang-migrate/migrate/v4"
	"github.com/pbs-plus/pbs-plus/internal/mtls"
	"github.com/pbs-plus/pbs-plus/internal/store/constants"
	"github.com/pbs-plus/pbs-plus/internal/store/types"
	"github.com/pbs-plus/pbs-plus/internal/syslog"
)

const maxAttempts = 100

// Database is our SQLite-backed store.
type Database struct {
	ctx          context.Context
	readDb       *sql.DB
	writeDb      *sql.DB
	writeMu      sync.Mutex
	dbPath       string
	TokenManager *mtls.TokenManager
}

// Initialize opens (or creates) the SQLite database at dbPath,
// creates all necessary tables if they do not exist,
// and then (optionally) fills any default items.
// It returns a pointer to a Database instance.
func Initialize(ctx context.Context, dbPath string) (*Database, error) {
	if dbPath == "" {
		dbPath = "/etc/proxmox-backup/pbs-plus/plus.db"
	}

	_ = os.MkdirAll(filepath.Dir(dbPath), 0755)

	initialized := false
	_, err := os.Stat(dbPath)
	if err == nil {
		initialized = true
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
		ctx:     ctx,
		dbPath:  dbPath,
		readDb:  readDb,
		writeDb: writeDb,
	}

	// Auto migrate on initialization
	if err := database.Migrate(); err != nil && !errors.Is(err, migrate.ErrNoChange) {
		return nil, fmt.Errorf("Initialize: error migrating tables: %w", err)
	}

	if !initialized {
		tx, err := writeDb.Begin()
		if err != nil {
			return nil, fmt.Errorf("Initialize: error migrating tables: %w", err)
		}

		for _, exclusion := range constants.DefaultExclusions {
			err = database.CreateExclusion(tx, types.Exclusion{
				Path:    exclusion,
				Comment: "Generated exclusion from default list",
			})
			if err != nil && !errors.Is(err, sql.ErrNoRows) {
				syslog.L.Error(err).WithField("path", exclusion).Write()
			}
		}

		_ = tx.Commit()
	}
	return database, nil
}

func (d *Database) NewTransaction() (*sql.Tx, error) {
	return d.writeDb.BeginTx(context.Background(), &sql.TxOptions{})
}
