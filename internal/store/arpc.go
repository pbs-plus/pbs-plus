//go:build linux

package store

import (
	arpcfs "github.com/sonroyaalmerol/pbs-plus/internal/backend/arpc"
)

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
