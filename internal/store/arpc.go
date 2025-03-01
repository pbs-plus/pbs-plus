//go:build linux

package store

import (
	"github.com/sonroyaalmerol/pbs-plus/internal/arpc"
	arpcfs "github.com/sonroyaalmerol/pbs-plus/internal/backend/arpc"
	"github.com/sonroyaalmerol/pbs-plus/internal/syslog"
)

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
