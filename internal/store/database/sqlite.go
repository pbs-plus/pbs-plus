//go:build linux

package database

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
	"github.com/pbs-plus/pbs-plus/internal/store/database/sqlc"
	"github.com/pbs-plus/pbs-plus/internal/syslog"
)

const maxAttempts = 100

type Database struct {
	ctx          context.Context
	readDb       *sql.DB
	writeDb      *sql.DB
	queries      *sqlc.Queries
	readQueries  *sqlc.Queries
	writeMu      sync.Mutex
	dbPath       string
	TokenManager *mtls.TokenManager
}

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
		ctx:         ctx,
		dbPath:      dbPath,
		readDb:      readDb,
		writeDb:     writeDb,
		queries:     sqlc.New(writeDb),
		readQueries: sqlc.New(readDb),
	}

	if err := database.Migrate(); err != nil && !errors.Is(err, migrate.ErrNoChange) {
		return nil, fmt.Errorf("Initialize: error migrating tables: %w", err)
	}

	if !initialized {
		tx, err := writeDb.Begin()
		if err != nil {
			return nil, fmt.Errorf("Initialize: error migrating tables: %w", err)
		}

		qtx := database.queries.WithTx(tx)
		for _, exclusion := range constants.DefaultExclusions {
			err = qtx.CreateExclusion(ctx, sqlc.CreateExclusionParams{
				JobID:   sql.NullString{},
				Path:    exclusion,
				Comment: sql.NullString{String: "Generated exclusion from default list", Valid: true},
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
