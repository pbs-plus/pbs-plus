//go:build linux

// Package sqldb provides the shared SQLite plumbing used by every PBS Plus
// database: a read-only reader, a single serialized writer with WAL and
// busy-timeout pragmas, and context-aware transactions.
package sqldb

import (
	"context"
	"database/sql"
	"embed"
	"errors"
	"fmt"
	"os"
	"path/filepath"

	"github.com/golang-migrate/migrate/v4"
	"github.com/golang-migrate/migrate/v4/database/sqlite"
	"github.com/golang-migrate/migrate/v4/source/iofs"
	_ "modernc.org/sqlite"
)

// DB is one SQLite database with a read-only pool and a single writer.
type DB struct {
	reader *sql.DB
	writer *sql.DB
	lock   chan struct{}
}

// Tx releases the writer lock on commit or rollback.
type Tx struct {
	*sql.Tx
	db *DB
}

func (t *Tx) Commit() error {
	err := t.Tx.Commit()
	t.db.lock <- struct{}{}
	return err
}

func (t *Tx) Rollback() error {
	err := t.Tx.Rollback()
	t.db.lock <- struct{}{}
	return err
}

// Begin starts a write transaction; the caller must commit or roll it
// back, which releases the writer.
func (d *DB) Begin(ctx context.Context) (*Tx, error) {
	select {
	case <-d.lock:
	case <-ctx.Done():
		return nil, fmt.Errorf("sqldb: acquire writer: %w", ctx.Err())
	}

	tx, err := d.writer.BeginTx(ctx, nil)
	if err != nil {
		d.lock <- struct{}{}
		return nil, fmt.Errorf("sqldb: begin: %w", err)
	}
	return &Tx{Tx: tx, db: d}, nil
}

// RunInTransaction runs fn in a write transaction; error rolls back,
// panic rolls back and re-panics.
func (d *DB) RunInTransaction(ctx context.Context, fn func(*Tx) error) error {
	t, err := d.Begin(ctx)
	if err != nil {
		return err
	}

	panicked := true
	defer func() {
		if panicked {
			_ = t.Rollback()
		}
	}()

	if err := fn(t); err != nil {
		panicked = false
		if rbErr := t.Rollback(); rbErr != nil {
			return fmt.Errorf("sqldb: rollback (%v) after error: %w", rbErr, err)
		}
		return err
	}
	panicked = false

	if err := t.Commit(); err != nil {
		return fmt.Errorf("sqldb: commit: %w", err)
	}
	return nil
}

// Writer returns the single-writer handle, for sqlc query binding.
func (d *DB) Writer() *sql.DB { return d.writer }

// Reader returns the read-only handle, for sqlc query binding.
func (d *DB) Reader() *sql.DB { return d.reader }

func (d *DB) Ping(ctx context.Context) error {
	return d.reader.PingContext(ctx)
}

func (d *DB) Close() error {
	return errors.Join(d.reader.Close(), d.writer.Close())
}

// Migrate applies the embedded up migrations from subdir of fs.
func (d *DB) Migrate(fs embed.FS, subdir string) error {
	driver, err := sqlite.WithInstance(d.writer, &sqlite.Config{})
	if err != nil {
		return fmt.Errorf("sqldb: migrate driver: %w", err)
	}

	source, err := iofs.New(fs, subdir)
	if err != nil {
		return fmt.Errorf("sqldb: migrate source: %w", err)
	}

	m, err := migrate.NewWithInstance("iofs", source, "sqlite", driver)
	if err != nil {
		return fmt.Errorf("sqldb: migrate init: %w", err)
	}

	if err := m.Up(); err != nil && !errors.Is(err, migrate.ErrNoChange) {
		return fmt.Errorf("sqldb: migrate up: %w", err)
	}
	return nil
}

// Open opens or creates the SQLite database at path and applies migrations.
func Open(path string, fs embed.FS, subdir string) (*DB, error) {
	if path == "" {
		return nil, errors.New("sqldb: path is required")
	}

	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return nil, fmt.Errorf("sqldb: create dir: %w", err)
	}

	reader, err := sql.Open("sqlite", path+"?mode=ro&_pragma=busy_timeout%3d5000")
	if err != nil {
		return nil, fmt.Errorf("sqldb: open reader: %w", err)
	}

	writer, err := sql.Open("sqlite", path+"?mode=rw&_txlock=immediate&_pragma=busy_timeout%3d5000")
	if err != nil {
		return nil, fmt.Errorf("sqldb: open writer: %w", err)
	}
	writer.SetMaxOpenConns(1)

	if _, err := writer.Exec("PRAGMA journal_mode=WAL;PRAGMA foreign_keys=ON;"); err != nil {
		return nil, fmt.Errorf("sqldb: pragmas: %w", err)
	}

	lock := make(chan struct{}, 1)
	lock <- struct{}{}
	db := &DB{reader: reader, writer: writer, lock: lock}
	if err := db.Migrate(fs, subdir); err != nil {
		return nil, err
	}
	return db, nil
}
