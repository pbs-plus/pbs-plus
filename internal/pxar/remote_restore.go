package pxar

import (
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"runtime"
	"sync"
)

const copyBufSize = 1 << 20

type copyBuffer [copyBufSize]byte

var copyBufPool = sync.Pool{
	New: func() any {
		return new(copyBuffer)
	},
}

type rangeReader struct {
	ctx          context.Context
	client       *RemoteClient
	contentStart uint64
	contentEnd   uint64
	totalSize    uint64
	offset       uint64
}

func (r *rangeReader) Read(p []byte) (int, error) {
	if r.offset >= r.totalSize {
		return 0, io.EOF
	}

	readSize := uint(len(p))
	if remain := r.totalSize - r.offset; remain < uint64(readSize) {
		readSize = uint(remain)
	}

	n, err := r.client.Read(r.ctx, r.contentStart, r.contentEnd, r.offset, readSize, p)
	if err != nil {
		return n, err
	}
	if n == 0 && readSize > 0 {
		return 0, io.EOF
	}

	r.offset += uint64(n)
	return n, nil
}

type restoreJob struct {
	dest string
	info EntryInfo
}

func RemoteRestore(ctx context.Context, client *RemoteClient, sources []string, destDir string) error {
	if err := os.MkdirAll(destDir, 0o755); err != nil {
		return fmt.Errorf("mkdir root: %w", err)
	}

	numWorkers := runtime.NumCPU() * 4
	jobs := make(chan restoreJob, 1024)
	var wg sync.WaitGroup

	for i := 0; i < numWorkers; i++ {
		go func() {
			for job := range jobs {
				if err := processRemoteJob(ctx, client, job, jobs, &wg); err != nil {
					_ = client.SendError(ctx, err)
				}
				wg.Done()
			}
		}()
	}

	func() {
		for _, source := range sources {
			if ctx.Err() != nil {
				return
			}

			sourceAttr, err := client.LookupByPath(ctx, source)
			if err != nil {
				_ = client.SendError(ctx, err)
				continue
			}
			path := filepath.Join(destDir, sourceAttr.Name())

			wg.Add(1)
			select {
			case jobs <- restoreJob{dest: path, info: sourceAttr}:
			case <-ctx.Done():
				wg.Done()
				return
			}
		}
	}()

	wg.Wait()
	close(jobs)

	return ctx.Err()
}

func processRemoteJob(ctx context.Context, client *RemoteClient, job restoreJob, jobs chan<- restoreJob, wg *sync.WaitGroup) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
	}

	if job.info.IsDir() {
		return remoteRestoreDir(ctx, client, job.dest, job.info, jobs, wg)
	}
	if job.info.IsSymlink() {
		return remoteRestoreSymlink(ctx, client, job.dest, job.info)
	}
	if job.info.IsFile() {
		return remoteRestoreFile(ctx, client, job.dest, job.info)
	}
	return nil
}

func remoteRestoreFile(ctx context.Context, client *RemoteClient, path string, e EntryInfo) error {
	f, err := os.OpenFile(path, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0666)
	if err != nil {
		return fmt.Errorf("create file %q: %w", path, err)
	}
	defer f.Close()

	if e.Size > 0 && e.ContentRange != nil {
		bufPtr := copyBufPool.Get().(*copyBuffer)
		defer copyBufPool.Put(bufPtr)

		rr := &rangeReader{
			ctx:          ctx,
			client:       client,
			contentStart: e.ContentRange[0],
			contentEnd:   e.ContentRange[1],
			totalSize:    e.Size,
		}

		if _, err := io.CopyBuffer(f, rr, bufPtr[:]); err != nil {
			return fmt.Errorf("copy data %q: %w", path, err)
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
