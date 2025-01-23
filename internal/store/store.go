//go:build linux

package store

import (
	"fmt"

	"github.com/sonroyaalmerol/pbs-plus/internal/auth/certificates"
	"github.com/sonroyaalmerol/pbs-plus/internal/store/database"
	"github.com/sonroyaalmerol/pbs-plus/internal/websockets"
)

// Store holds the configuration system
type Store struct {
	WSHub         *websockets.Server
	CertGenerator *certificates.Generator
	Database      *database.Database
}

func Initialize(wsHub *websockets.Server, paths map[string]string) (*Store, error) {
	database, err := database.Initialize(paths)
	if err != nil {
		return nil, fmt.Errorf("Initialize: error initializing database -> %w", err)
	}

	store := &Store{
		WSHub:    wsHub,
		Database: database,
	}

	return store, nil
}
