//go:build linux

package store

import (
	"fmt"

	"github.com/alphadose/haxmap"
	"github.com/sonroyaalmerol/pbs-plus/internal/arpc"
	"github.com/sonroyaalmerol/pbs-plus/internal/auth/certificates"
	arpcfs "github.com/sonroyaalmerol/pbs-plus/internal/backend/arpc"
	"github.com/sonroyaalmerol/pbs-plus/internal/store/database"
	"github.com/sonroyaalmerol/pbs-plus/internal/syslog"
	"github.com/sonroyaalmerol/pbs-plus/internal/utils/hashmap"
)

// Store holds the configuration system.
type Store struct {
	CertGenerator *certificates.Generator
	Database      *database.Database
	aRPCs         *haxmap.Map[string, *arpc.Session]
	arpcFS        *haxmap.Map[string, *arpcfs.ARPCFS]
}

func Initialize(paths map[string]string) (*Store, error) {
	db, err := database.Initialize(paths)
	if err != nil {
		return nil, fmt.Errorf("Initialize: error initializing database -> %w", err)
	}

	store := &Store{
		Database: db,
		aRPCs:    hashmap.New[*arpc.Session](),
		arpcFS:   hashmap.New[*arpcfs.ARPCFS](),
	}

	return store, nil
}

func (s *Store) AddARPC(client string, session *arpc.Session) {
	s.aRPCs.Set(client, session)
	syslog.L.Infof("Client %s added via aRPC", client)

	count := s.aRPCs.Len()
	syslog.L.Infof("Total aRPC clients: %d", count)
}

func (s *Store) GetARPC(client string) *arpc.Session {
	if session, ok := s.aRPCs.Get(client); ok {
		return session
	}
	return nil
}

func (s *Store) RemoveARPC(client string) {
	if session, ok := s.aRPCs.Get(client); ok {
		_ = session.Close()
		s.aRPCs.Del(client)
		syslog.L.Infof("Client %s removed via aRPC", client)
	}
}

func (s *Store) AddARPCFS(client string, fs *arpcfs.ARPCFS) {
	s.arpcFS.Set(client, fs)
}

func (s *Store) GetARPCFS(client string) *arpcfs.ARPCFS {
	if fs, ok := s.arpcFS.Get(client); ok {
		return fs
	}
	return nil
}

func (s *Store) RemoveARPCFS(client string) {
	if fs, ok := s.arpcFS.Get(client); ok {
		fs.Unmount()
		s.arpcFS.Del(client)
	}
}
