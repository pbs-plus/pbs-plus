package vfs

import (
	"context"
	"fmt"
	"strconv"
	"time"

	"github.com/bradfitz/gomemcache/memcache"
	"github.com/hanwen/go-fuse/v2/fuse"
	"github.com/pbs-plus/pbs-plus/internal/store/types"
)

type VFSBase struct {
	Ctx      context.Context
	Cancel   context.CancelFunc
	Job      types.Job
	Fuse     *fuse.Server
	BasePath string

	Memcache *memcache.Client

	FileCount     int64
	FolderCount   int64
	TotalBytes    int64
	StatCacheHits int64
}

func (v *VFSBase) GetStats() (VFSStats, error) {
	var r VFSStats

	if v.Memcache == nil {
		return r, fmt.Errorf("memcache has not been initialized yet")
	}

	if it, err := v.Memcache.Get("stats:filesAccessed"); err == nil {
		if v, e := strconv.ParseInt(string(it.Value), 10, 64); e == nil {
			r.FilesAccessed = v
		}
	}
	if it, err := v.Memcache.Get("stats:foldersAccessed"); err == nil {
		if v, e := strconv.ParseInt(string(it.Value), 10, 64); e == nil {
			r.FoldersAccessed = v
		}
	}
	r.TotalAccessed = r.FilesAccessed + r.FoldersAccessed

	if it, err := v.Memcache.Get("stats:totalBytes"); err == nil {
		if v, e := strconv.ParseUint(string(it.Value), 10, 64); e == nil {
			r.TotalBytes = v
		}
	}
	if it, err := v.Memcache.Get("stats:statCacheHits"); err == nil {
		if v, e := strconv.ParseInt(string(it.Value), 10, 64); e == nil {
			r.StatCacheHits = v
		}
	}

	now := time.Now().UnixNano()
	nowStr := strconv.FormatInt(now, 10)

	var prevBytes uint64
	if it, err := v.Memcache.Get("stats:totalBytes:prev"); err == nil {
		if v, e := strconv.ParseUint(string(it.Value), 10, 64); e == nil {
			prevBytes = v
		}
	}
	var prevBytesTs int64
	if it, err := v.Memcache.Get("stats:totalBytes:prev_ts"); err == nil {
		if v, e := strconv.ParseInt(string(it.Value), 10, 64); e == nil {
			prevBytesTs = v
		}
	}

	var prevAccess int64
	if it, err := v.Memcache.Get("stats:totalAccessed:prev"); err == nil {
		if v, e := strconv.ParseInt(string(it.Value), 10, 64); e == nil {
			prevAccess = v
		}
	}
	var prevAccessTs int64
	if it, err := v.Memcache.Get("stats:totalAccessed:prev_ts"); err == nil {
		if v, e := strconv.ParseInt(string(it.Value), 10, 64); e == nil {
			prevAccessTs = v
		}
	}

	_ = v.Memcache.Set(&memcache.Item{Key: "stats:totalBytes:prev", Value: []byte(strconv.FormatUint(r.TotalBytes, 10))})
	_ = v.Memcache.Set(&memcache.Item{Key: "stats:totalBytes:prev_ts", Value: []byte(nowStr)})
	_ = v.Memcache.Set(&memcache.Item{Key: "stats:totalAccessed:prev", Value: []byte(strconv.FormatInt(r.TotalAccessed, 10))})
	_ = v.Memcache.Set(&memcache.Item{Key: "stats:totalAccessed:prev_ts", Value: []byte(nowStr)})

	if prevBytesTs > 0 && now > prevBytesTs && r.TotalBytes >= prevBytes {
		sec := float64(now-prevBytesTs) / 1e9
		if sec > 0 {
			r.ByteReadSpeed = float64(r.TotalBytes-prevBytes) / sec
		}
	}

	if prevAccessTs > 0 && now > prevAccessTs && r.TotalAccessed >= prevAccess {
		sec := float64(now-prevAccessTs) / 1e9
		if sec > 0 {
			r.FileAccessSpeed = float64(r.TotalAccessed-prevAccess) / sec
		}
	}

	return r, nil
}

type VFSStats struct {
	ByteReadSpeed   float64
	FileAccessSpeed float64
	FilesAccessed   int64
	FoldersAccessed int64
	TotalAccessed   int64
	TotalBytes      uint64
	StatCacheHits   int64
}
