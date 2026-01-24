//go:build linux

package pxar

import (
	"bufio"
	"bytes"
	"context"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"strconv"
	"sync"
	"time"

	"github.com/pbs-plus/pbs-plus/internal/agent/agentfs/types"
	"golang.org/x/sys/unix"
)

var localCopyBufPool = sync.Pool{
	New: func() any {
		b := make([]byte, 1<<20)
		return &b
	},
}

type localJob struct {
	dest string
	info EntryInfo
}

func LocalRestore(
	ctx context.Context,
	pr *PxarReader,
	sourceDirs []string,
	destDir string,
	errChan chan error,
) {
	if err := os.MkdirAll(destDir, 0o755); err != nil {
		errChan <- fmt.Errorf("mkdir root: %w", err)
		return
	}

	numWorkers := runtime.NumCPU() * 2
	jobs := make(chan localJob, 512)
	var wg sync.WaitGroup

	for i := 0; i < numWorkers; i++ {
		go func() {
			for {
				select {
				case <-ctx.Done():
					return
				case job, ok := <-jobs:
					if !ok {
						return
					}
					processLocalJob(ctx, pr, job, jobs, &wg, errChan)
					wg.Done()
				}
			}
		}()
	}

	for _, source := range sourceDirs {
		sourceAttr, err := pr.LookupByPath(source)
		if err != nil {
			errChan <- err
			continue
		}

		path := filepath.Join(destDir, sourceAttr.Name())
		wg.Add(1)
		select {
		case jobs <- localJob{dest: path, info: *sourceAttr}:
		case <-ctx.Done():
			wg.Done()
			return
		}
	}

	wg.Wait()
	close(jobs)
}

func processLocalJob(ctx context.Context, pr *PxarReader, job localJob, jobs chan<- localJob, wg *sync.WaitGroup, errChan chan error) {
	select {
	case <-ctx.Done():
		return
	default:
	}

	var err error
	switch {
	case job.info.IsDir():
		err = localRestoreDir(ctx, pr, job.dest, &job.info, jobs, wg, errChan)
	case job.info.IsSymlink():
		err = localRestoreSymlink(ctx, pr, job.dest, &job.info)
	case job.info.IsFile():
		err = localRestoreFile(ctx, pr, job.dest, &job.info)
	}

	if err != nil {
		errChan <- err
	}
}

func restoreSpecialFile(pr *PxarReader, target string, e EntryInfo) error {
	var opErr error
	mode := uint32(e.Mode & 0777)
	if e.FileType == FileTypeFifo {
		opErr = unix.Mkfifo(target, mode)
	} else {
		opErr = unix.Mknod(target, unix.S_IFSOCK|mode, 0)
	}

	if opErr == nil || os.IsExist(opErr) {
		if f, openErr := os.OpenFile(target, os.O_RDONLY, 0); openErr == nil {
			return localApplyMeta(pr, f, &e)
		}
	}
	return opErr
}

func localRestoreFile(ctx context.Context, pr *PxarReader, path string, e *EntryInfo) error {
	f, err := os.OpenFile(path, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, os.FileMode(e.Mode&0777))
	if err != nil {
		return fmt.Errorf("create file %q: %w", path, err)
	}
	defer f.Close()

	if e.Size > 0 && e.ContentRange != nil {
		bw := bufio.NewWriterSize(f, 1<<18)
		bufPtr := localCopyBufPool.Get().(*[]byte)
		defer localCopyBufPool.Put(bufPtr)
		buf := *bufPtr

		start, end := e.ContentRange[0], e.ContentRange[1]
		var off uint64
		for off < e.Size {
			if err := ctx.Err(); err != nil {
				return err
			}
			size := uint(len(buf))
			if remain := e.Size - off; remain < uint64(size) {
				size = uint(remain)
			}
			data, err := pr.Read(start, end, off, size)
			if err != nil || len(data) == 0 {
				break
			}
			if _, err := bw.Write(data); err != nil {
				return err
			}
			off += uint64(len(data))
		}
		if err := bw.Flush(); err != nil {
			return err
		}
	}

	return localApplyMeta(pr, f, e)
}

func localApplyMeta(pr *PxarReader, f *os.File, e *EntryInfo) error {
	defer f.Close()
	fd := int(f.Fd())

	uid, gid := int(e.UID), int(e.GID)
	atime := time.Unix(e.MtimeSecs, int64(e.MtimeNsecs))
	mtime := atime

	xattrs, err := pr.ListXAttrs(e.EntryRangeStart, e.EntryRangeEnd)
	if err == nil && len(xattrs) > 0 {
		if d, ok := xattrs["user.owner"]; ok {
			if id, err := strconv.Atoi(string(d)); err == nil {
				uid = id
			}
			delete(xattrs, "user.owner")
		}
		if d, ok := xattrs["user.group"]; ok {
			if id, err := strconv.Atoi(string(d)); err == nil {
				gid = id
			}
			delete(xattrs, "user.group")
		}
		if d, ok := xattrs["user.acls"]; ok {
			var entries []types.PosixACL
			if json.Unmarshal(d, &entries) == nil {
				applyUnixACLsFd(fd, entries)
			}
			delete(xattrs, "user.acls")
		}
		if d, ok := xattrs["user.lastaccesstime"]; ok {
			if ts, err := binary.ReadVarint(bytes.NewReader(d)); err == nil {
				atime = time.Unix(ts, 0)
			}
			delete(xattrs, "user.lastaccesstime")
		}
		if d, ok := xattrs["user.lastwritetime"]; ok {
			if ts, err := binary.ReadVarint(bytes.NewReader(d)); err == nil {
				mtime = time.Unix(ts, 0)
			}
			delete(xattrs, "user.lastwritetime")
		}

		for name, val := range xattrs {
			if name == "user.creationtime" || name == "user.fileattributes" {
				continue
			}
			_ = unix.Fsetxattr(fd, name, val, 0)
		}
	}

	_ = unix.Fchown(fd, uid, gid)
	_ = unix.Fchmod(fd, uint32(e.Mode&0777))

	ts := []unix.Timespec{
		unix.NsecToTimespec(atime.UnixNano()),
		unix.NsecToTimespec(mtime.UnixNano()),
	}
	_ = unix.UtimesNanoAt(fd, "", ts, 0)

	return nil
}

func localRestoreSymlink(ctx context.Context, pr *PxarReader, path string, e *EntryInfo) error {
	target, err := pr.ReadLink(e.EntryRangeStart, e.EntryRangeEnd)
	if err != nil {
		return err
	}
	if err := os.Symlink(string(target), path); err != nil {
		if !os.IsExist(err) {
			return err
		}
	}
	return localApplyMetaSymlink(pr, path, e)
}

func localApplyMetaSymlink(pr *PxarReader, path string, e *EntryInfo) error {
	uid, gid := int(e.UID), int(e.GID)
	xattrs, err := pr.ListXAttrs(e.EntryRangeStart, e.EntryRangeEnd)

	if err == nil && len(xattrs) > 0 {
		if d, ok := xattrs["user.owner"]; ok {
			if id, err := strconv.Atoi(string(d)); err == nil {
				uid = id
			}
		}
		if d, ok := xattrs["user.group"]; ok {
			if id, err := strconv.Atoi(string(d)); err == nil {
				gid = id
			}
		}
	}

	_ = unix.Lchown(path, uid, gid)

	if err == nil {
		for name, val := range xattrs {
			switch name {
			case "user.owner", "user.group", "user.acls", "user.fileattributes",
				"user.lastaccesstime", "user.lastwritetime", "user.creationtime":
				continue
			default:
				_ = unix.Lsetxattr(path, name, val, 0)
			}
		}
	}
	return nil
}
