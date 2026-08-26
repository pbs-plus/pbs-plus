//go:build linux

package store

import (
	"context"
	"database/sql"
	"embed"
	"fmt"

	"github.com/pbs-plus/pbs-plus/internal/log"
	"github.com/pbs-plus/pbs-plus/internal/server/coredb"
	"github.com/pbs-plus/pbs-plus/internal/server/mtf/store/mtfquery"
	"github.com/pbs-plus/pbs-plus/internal/sqldb"
)

//go:embed migrations/*.sql
var migrations embed.FS

type Database struct {
	*sqldb.DB
	queries     *mtfquery.Queries
	readQueries *mtfquery.Queries
}

// Transaction releases the writer lock on commit or rollback.
type Transaction = sqldb.Tx

func Initialize(ctx context.Context, dbPath string) (*Database, error) {
	if dbPath == "" {
		dbPath = "/etc/proxmox-backup/pbs-plus/tapes.db"
	}

	db, err := sqldb.Open(dbPath, migrations, "migrations")
	if err != nil {
		return nil, fmt.Errorf("tapestore: %w", err)
	}

	d := &Database{
		DB:          db,
		queries:     mtfquery.New(db.Writer()),
		readQueries: mtfquery.New(db.Reader()),
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
			log.Error(err, "")
		}
	}

	return d, nil
}

func (d *Database) Ping(ctx context.Context) error {
	return d.DB.Ping(ctx)
}

func (d *Database) Queries() *mtfquery.Queries { return d.queries }

func (d *Database) NewTransaction() (*Transaction, error) {
	return d.Begin(context.Background())
}

// RunInTransaction runs fn in a write transaction; error rolls back,
// panic rolls back and re-panics.
func (d *Database) RunInTransaction(ctx context.Context, fn func(tx *Transaction, q *mtfquery.Queries) error) error {
	return d.DB.RunInTransaction(ctx, func(tx *Transaction) error {
		return fn(tx, d.queries.WithTx(tx.Tx))
	})
}

type JobStatus = coredb.JobStatus
