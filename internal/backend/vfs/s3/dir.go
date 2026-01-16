//go:build linux

package s3fs

import (
	"syscall"

	"github.com/hanwen/go-fuse/v2/fuse"
	agentTypes "github.com/pbs-plus/pbs-plus/internal/agent/agentfs/types"
)

type S3DirStream struct {
	entries agentTypes.ReadDirEntries
	idx     int
}

func (s *S3DirStream) HasNext() bool {
	return s.idx < len(s.entries)
}

func (s *S3DirStream) Next() (fuse.DirEntry, syscall.Errno) {
	if !s.HasNext() {
		return fuse.DirEntry{}, syscall.ENOENT
	}
	e := s.entries[s.idx]
	s.idx++

	return fuse.DirEntry{
		Name: e.Name,
		Mode: e.Mode,
	}, 0
}

func (s *S3DirStream) Close() {}
