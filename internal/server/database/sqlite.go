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
	"github.com/pbs-plus/pbs-plus/internal/conf"
	"github.com/pbs-plus/pbs-plus/internal/mtls"
	"github.com/pbs-plus/pbs-plus/internal/server/database/sqlc"
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

	writeDb, err := sql.Open("sqlite", dbPath+"?mode=rw&_txlock=immediate")
	if err != nil {
		return nil, fmt.Errorf("Initialize: error opening DB: %w", err)
	}
	writeDb.SetMaxOpenConns(1)

	_, err = writeDb.Exec("PRAGMA journal_mode=WAL;PRAGMA foreign_keys=ON;")
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
		for _, exclusion := range conf.DefaultExclusions {
			err = qtx.CreateExclusion(ctx, sqlc.CreateExclusionParams{
				JobID:   "",
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

type Transaction struct {
	*sql.Tx
	database *Database
	released bool
}

func (t *Transaction) Commit() error {
	err := t.Tx.Commit()
	t.release()
	return err
}

func (t *Transaction) Rollback() error {
	err := t.Tx.Rollback()
	t.release()
	return err
}

func (t *Transaction) release() {
	if !t.released {
		t.database.writeMu.Unlock()
		t.released = true
	}
}

func (d *Database) NewTransaction() (*Transaction, error) {
	d.writeMu.Lock()

	tx, err := d.writeDb.BeginTx(d.ctx, nil)
	if err != nil {
		d.writeMu.Unlock()
		return nil, err
	}

	return &Transaction{Tx: tx, database: d}, nil
}

// Ping checks the database connection health.
func (d *Database) Ping(ctx context.Context) error {
	return d.readDb.PingContext(ctx)
}

// RunInTransaction executes fn within a database transaction.
// If fn returns an error, the transaction is rolled back.
// If fn panics, the panic is re-thrown after rollback.
func (d *Database) RunInTransaction(ctx context.Context, fn func(tx *Transaction, q *sqlc.Queries) error) error {
	tx, err := d.NewTransaction()
	if err != nil {
		return fmt.Errorf("RunInTransaction: %w", err)
	}
	defer func() {
		if p := recover(); p != nil {
			_ = tx.Rollback()
			panic(p)
		}
	}()

	q := d.queries.WithTx(tx.Tx)
	if err := fn(tx, q); err != nil {
		if rbErr := tx.Rollback(); rbErr != nil {
			syslog.L.Error(fmt.Errorf("RunInTransaction: rollback error: %w", rbErr)).Write()
		}
		return err
	}

	if err := tx.Commit(); err != nil {
		return fmt.Errorf("RunInTransaction: commit error: %w", err)
	}

	return nil
}
