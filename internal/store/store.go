//go:build linux

package store

import (
	"context"
	"fmt"

	"github.com/pbs-plus/pbs-plus/internal/arpc"
	arpcfs "github.com/pbs-plus/pbs-plus/internal/backend/arpc"
	sqlite "github.com/pbs-plus/pbs-plus/internal/store/database"
	"github.com/pbs-plus/pbs-plus/internal/utils/safemap"

	_ "modernc.org/sqlite"
)

// Store holds the configuration system.
type Store struct {
	Ctx      context.Context
	Database *sqlite.Database
	Node     *arpc.Node
	arpcFS   *safemap.Map[string, *arpcfs.ARPCFS]
}

func Initialize(ctx context.Context, paths map[string]string) (*Store, error) {
	sqlitePath := ""
	if paths != nil {
		sqlitePathTmp, ok := paths["sqlite"]
		if ok {
			sqlitePath = sqlitePathTmp
		}
	}

	db, err := sqlite.Initialize(ctx, sqlitePath)
	if err != nil {
		return nil, fmt.Errorf("Initialize: error initializing database -> %w", err)
	}

	store := &Store{
		Ctx:      ctx,
		Database: db,
		arpcFS:   safemap.New[string, *arpcfs.ARPCFS](),
	}

	return store, nil
}
