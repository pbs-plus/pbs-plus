//go:build linux

package s3fs

import (
	"strconv"
	"sync/atomic"
	"syscall"

	"github.com/bradfitz/gomemcache/memcache"
	"github.com/hanwen/go-fuse/v2/fuse"
)

func (s *S3DirStream) HasNext() bool {
	return s.idx < len(s.entries)
}

func (s *S3DirStream) Next() (fuse.DirEntry, syscall.Errno) {
	if !s.HasNext() {
		return fuse.DirEntry{}, syscall.ENOENT
	}
	e := s.entries[s.idx]
	s.idx++
	atomic.AddUint64(&s.total, 1)
	tr := atomic.LoadUint64(&s.total)
	_ = s.fs.Memcache.Set(&memcache.Item{Key: "stats:dirEntriesReturned", Value: []byte(strconv.FormatUint(tr, 10)), Expiration: 0})
	return fuse.DirEntry{Name: e.Name, Mode: e.Mode}, 0
}

func (s *S3DirStream) Close() {}
