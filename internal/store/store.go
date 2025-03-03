//go:build linux

package store

import (
	"fmt"

	"github.com/alphadose/haxmap"
	"github.com/sonroyaalmerol/pbs-plus/internal/arpc"
	"github.com/sonroyaalmerol/pbs-plus/internal/auth/certificates"
	arpcfs "github.com/sonroyaalmerol/pbs-plus/internal/backend/arpc"
	"github.com/sonroyaalmerol/pbs-plus/internal/store/database"
	"github.com/sonroyaalmerol/pbs-plus/internal/utils/hashmap"
)

// Store holds the configuration system.
type Store struct {
	CertGenerator      *certificates.Generator
	Database           *database.Database
	ARPCSessionManager *arpc.SessionManager
	aRPCs              *haxmap.Map[string, *arpc.Session]
	arpcFS             *haxmap.Map[string, *arpcfs.ARPCFS]
}

func Initialize(paths map[string]string) (*Store, error) {
	db, err := database.Initialize(paths)
	if err != nil {
		return nil, fmt.Errorf("Initialize: error initializing database -> %w", err)
	}

	store := &Store{
		Database:           db,
		aRPCs:              hashmap.New[*arpc.Session](),
		arpcFS:             hashmap.New[*arpcfs.ARPCFS](),
		ARPCSessionManager: arpc.NewSessionManager(),
	}

	return store, nil
}
