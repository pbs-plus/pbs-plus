//go:build linux

package coredb

import (
	"context"
	"database/sql"
	"embed"
	"errors"
	"fmt"
	"os"

	"github.com/pbs-plus/pbs-plus/internal/conf"
	"github.com/pbs-plus/pbs-plus/internal/log"
	"github.com/pbs-plus/pbs-plus/internal/mtls"
	"github.com/pbs-plus/pbs-plus/internal/server/coredb/corequery"
	"github.com/pbs-plus/pbs-plus/internal/sqldb"
)

//go:embed migrations/*.sql
var migrations embed.FS

const maxAttempts = 100

type DB struct {
	*sqldb.DB
	queries      *corequery.Queries
	readQueries  *corequery.Queries
	ctx          context.Context
	TokenManager *mtls.TokenManager
}

// Transaction releases the writer lock on commit or rollback.
type Transaction = sqldb.Tx

func Initialize(ctx context.Context, dbPath string) (*DB, error) {
	if dbPath == "" {
		dbPath = "/etc/proxmox-backup/pbs-plus/plus.db"
	}

	initialized := false
	if _, err := os.Stat(dbPath); err == nil {
		initialized = true
	}

	db, err := sqldb.Open(dbPath, migrations, "migrations")
	if err != nil {
		return nil, fmt.Errorf("Initialize: %w", err)
	}

	d := &DB{
		DB:          db,
		queries:     corequery.New(db.Writer()),
		readQueries: corequery.New(db.Reader()),
		ctx:         ctx,
	}

	if err := d.MigrateSecrets(); err != nil {
		log.Error(err, "Initialize: error migrating secrets")
	}

	if !initialized {
		if err := d.RunInTransaction(ctx, func(_ *Transaction, q *corequery.Queries) error {
			for _, exclusion := range conf.DefaultExclusions {
				err := q.CreateExclusion(ctx, corequery.CreateExclusionParams{
					JobID:   "",
					Path:    exclusion,
					Comment: sql.NullString{String: "Generated exclusion from default list", Valid: true},
				})
				if err != nil && !errors.Is(err, sql.ErrNoRows) {
					log.Error(err, "", "path", exclusion)
				}
			}
			return nil
		}); err != nil {
			log.Error(err, "")
		}
	}
	return d, nil
}

func (d *DB) NewTransaction() (*Transaction, error) {
	return d.DB.Begin(d.ctx)
}

func (d *DB) Ping(ctx context.Context) error {
	return d.DB.Ping(ctx)
}

// JobCount returns at least 1 so the queue never has a zero-size buffer.
func (d *DB) JobCount(ctx context.Context) (int, error) {
	backupCount, err := d.readQueries.CountBackups(ctx)
	if err != nil {
		return 1, fmt.Errorf("JobCount: count backups: %w", err)
	}
	restoreCount, err := d.readQueries.CountRestores(ctx)
	if err != nil {
		return 1, fmt.Errorf("JobCount: count restores: %w", err)
	}
	return max(int(backupCount+restoreCount), 1), nil
}

// RunInTransaction runs fn in a write transaction; error rolls back,
// panic rolls back and re-panics.
func (d *DB) RunInTransaction(ctx context.Context, fn func(tx *Transaction, q *corequery.Queries) error) error {
	return d.DB.RunInTransaction(ctx, func(tx *Transaction) error {
		return fn(tx, d.queries.WithTx(tx.Tx))
	})
}
