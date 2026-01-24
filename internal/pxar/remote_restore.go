package pxar

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"sync"
)

var (
	copyBufPool = sync.Pool{New: func() any { return make([]byte, 1<<20) }}
)

type restoreJob struct {
	dest string
	info EntryInfo
}

func RemoteRestore(ctx context.Context, client *RemoteClient, sources []string, destDir string) error {
	if err := os.MkdirAll(destDir, 0o755); err != nil {
		return fmt.Errorf("mkdir root: %w", err)
	}

	numWorkers := runtime.NumCPU() * 2
	jobs := make(chan restoreJob, 512)
	var wg sync.WaitGroup

	reportErr := func(err error) {
		_ = client.SendError(ctx, err)
	}

	for i := 0; i < numWorkers; i++ {
		wg.Go(func() {
			for job := range jobs {
				select {
				case <-ctx.Done():
					return
				default:
				}

				var err error
				if job.info.IsDir() {
					err = remoteRestoreDir(ctx, client, job.dest, job.info, jobs, &wg)
				} else if job.info.IsSymlink() {
					err = remoteRestoreSymlink(ctx, client, job.dest, job.info)
				} else if job.info.IsFile() {
					err = remoteRestoreFile(ctx, client, job.dest, job.info)
				}

				if err != nil {
					reportErr(err)
				}
			}
		})
	}

	for _, source := range sources {
		sourceAttr, err := client.LookupByPath(ctx, source)
		if err != nil {
			reportErr(err)
			continue
		}
		path := filepath.Join(destDir, sourceAttr.Name())

		wg.Add(1)
		jobs <- restoreJob{dest: path, info: sourceAttr}
	}

	wg.Wait()
	close(jobs)

	return nil
}

func remoteRestoreFile(ctx context.Context, client *RemoteClient, path string, e EntryInfo) error {
	f, err := os.OpenFile(path, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0666)
	if err != nil {
		return fmt.Errorf("create file %q: %w", path, err)
	}

	if e.Size > 0 && e.ContentRange != nil {
		buf := copyBufPool.Get().([]byte)
		defer copyBufPool.Put(buf)

		var off uint64
		start, end := e.ContentRange[0], e.ContentRange[1]
		for off < e.Size {
			if ctx.Err() != nil {
				f.Close()
				return ctx.Err()
			}

			readSize := uint(len(buf))
			if remain := e.Size - off; remain < uint64(readSize) {
				readSize = uint(remain)
			}

			n, err := client.Read(ctx, start, end, off, readSize, buf)
			if err != nil {
				f.Close()
				return fmt.Errorf("read remote: %w", err)
			}
			if n == 0 {
				break
			}

			if _, err := f.Write(buf[:n]); err != nil {
				f.Close()
				return err
			}
			off += uint64(n)
		}
	}

	return remoteApplyMeta(ctx, client, f, e)
}

func remoteRestoreSymlink(ctx context.Context, client *RemoteClient, path string, e EntryInfo) error {
	target, err := client.ReadLink(ctx, e.EntryRangeStart, e.EntryRangeEnd)
	if err != nil {
		return fmt.Errorf("readlink data %q: %w", path, err)
	}
	if err := os.Symlink(string(target), path); err != nil {
		return fmt.Errorf("symlink %q: %w", path, err)
	}
	return remoteApplyMetaSymlink(ctx, client, path, e)
}
