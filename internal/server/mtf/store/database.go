package store

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"

	"github.com/golang-migrate/migrate/v4"
	_ "modernc.org/sqlite"

	"github.com/pbs-plus/pbs-plus/internal/server/database"
	"github.com/pbs-plus/pbs-plus/internal/server/mtf/store/mtfquery"
	"github.com/pbs-plus/pbs-plus/internal/syslog"
)

type Database struct {
	ctx         context.Context
	readDb      *sql.DB
	writeDb     *sql.DB
	queries     *mtfquery.Queries
	readQueries *mtfquery.Queries
	writeMu     sync.Mutex
	dbPath      string
}

func Initialize(ctx context.Context, dbPath string) (*Database, error) {
	if dbPath == "" {
		dbPath = "/etc/proxmox-backup/pbs-plus/tapes.db"
	}

	if err := os.MkdirAll(filepath.Dir(dbPath), 0755); err != nil {
		syslog.L.Error(err).Write()
	}

	readDb, err := sql.Open("sqlite", dbPath+"?mode=ro")
	if err != nil {
		return nil, fmt.Errorf("tapestore: open DB: %w", err)
	}

	writeDb, err := sql.Open("sqlite", dbPath+"?mode=rw&_txlock=immediate")
	if err != nil {
		return nil, fmt.Errorf("tapestore: open DB: %w", err)
	}
	writeDb.SetMaxOpenConns(1)

	if _, err := writeDb.Exec("PRAGMA journal_mode=WAL;PRAGMA foreign_keys=ON;"); err != nil {
		return nil, fmt.Errorf("tapestore: pragmas: %w", err)
	}

	d := &Database{
		ctx:         ctx,
		dbPath:      dbPath,
		readDb:      readDb,
		writeDb:     writeDb,
		queries:     mtfquery.New(writeDb),
		readQueries: mtfquery.New(readDb),
	}

	if err := d.Migrate(); err != nil && !errors.Is(err, migrate.ErrNoChange) {
		return nil, fmt.Errorf("tapestore: migrate: %w", err)
	}

	if n, err := d.readQueries.CountMappings(ctx); err == nil && n == 0 {
		if _, err := d.queries.CreateMapping(ctx, mtfquery.CreateMappingParams{
			Name:      sql.NullString{String: "Default", Valid: true},
			Priority:  sql.NullInt64{Int64: 9999, Valid: true},
			Template:  "tape/{machine}/{drive}",
			IsDefault: sql.NullInt64{Int64: 1, Valid: true},
			Enabled:   sql.NullInt64{Int64: 1, Valid: true},
			Comment:   sql.NullString{String: "Fallback mapping for unmatched volumes", Valid: true},
		}); err != nil {
			syslog.L.Error(err).Write()
		}
	}

	return d, nil
}

func (d *Database) Ping(ctx context.Context) error {
	return d.readDb.PingContext(ctx)
}

func (d *Database) Close() error {
	var errs []error
	if err := d.readDb.Close(); err != nil {
		errs = append(errs, err)
	}
	if err := d.writeDb.Close(); err != nil {
		errs = append(errs, err)
	}
	if len(errs) > 0 {
		return fmt.Errorf("tapestore: close: %v", errs)
	}
	return nil
}

func (d *Database) Queries() *mtfquery.Queries { return d.queries }

type Transaction struct {
	*sql.Tx
	db       *Database
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
		t.db.writeMu.Unlock()
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
	return &Transaction{Tx: tx, db: d}, nil
}

func (d *Database) RunInTransaction(ctx context.Context, fn func(tx *Transaction, q *mtfquery.Queries) error) error {
	tx, err := d.NewTransaction()
	if err != nil {
		return fmt.Errorf("tapestore.RunInTransaction: %w", err)
	}
	defer func() {
		if p := recover(); p != nil {
			if err := tx.Rollback(); err != nil {
				syslog.L.Error(err).Write()
			}
			panic(p)
		}
	}()

	q := d.queries.WithTx(tx.Tx)
	if err := fn(tx, q); err != nil {
		if rbErr := tx.Rollback(); rbErr != nil {
			return fmt.Errorf("tapestore.RunInTransaction: %w (rollback: %v)", err, rbErr)
		}
		return err
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("tapestore.RunInTransaction: commit: %w", err)
	}
	return nil
}

type JobStatus = database.JobStatus
