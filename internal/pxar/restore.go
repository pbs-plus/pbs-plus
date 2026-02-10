package pxar

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"runtime"
	"sync"
)

type RestoreMode int

const (
	RestoreModeNormal RestoreMode = iota
	RestoreModeZip
	RestoreModeNoAttr
)

type RestoreOptions struct {
	Mode    RestoreMode
	DestDir string
}

type restoreJob struct {
	dest string
	info EntryInfo
}

func Restore(ctx context.Context, client *Client, sources []string, destDir string) error {
	return RestoreWithOptions(ctx, client, sources, RestoreOptions{
		Mode:    RestoreModeNormal,
		DestDir: destDir,
	})
}

func RestoreWithOptions(ctx context.Context, client *Client, sources []string, opts RestoreOptions) error {
	if err := os.MkdirAll(opts.DestDir, 0o755); err != nil {
		return fmt.Errorf("mkdir root: %w", err)
	}

	if opts.Mode == RestoreModeZip {
		return restoreAsZips(ctx, client, sources, opts)
	}

	noAttr := opts.Mode == RestoreModeNoAttr
	return restoreNormal(ctx, client, sources, opts.DestDir, noAttr)
}

func restoreNormal(ctx context.Context, client *Client, sources []string, destDir string, noAttr bool) error {
	fsCap := getFilesystemCapabilities(destDir)

	numCPU := runtime.NumCPU()
	numWorkers := numCPU * 2
	if fsCap.prefersSequentialOps {
		numWorkers = min(numCPU, 2)
	}

	jobs := make(chan restoreJob, 1024)
	var wg sync.WaitGroup

	workerCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	var workersWg sync.WaitGroup
	for i := 0; i < numWorkers; i++ {
		workersWg.Go(func() {
			for job := range jobs {
				if err := processJob(workerCtx, client, job, jobs, fsCap, &wg, noAttr); err != nil {
					_ = client.SendError(workerCtx, err)
				}
				wg.Done()
			}
		})
	}

	var sourcesWg sync.WaitGroup
	for _, source := range sources {
		if workerCtx.Err() != nil {
			break
		}

		sourcesWg.Add(1)
		go func(src string) {
			defer sourcesWg.Done()
			if workerCtx.Err() != nil {
				return
			}

			sourceAttr, err := client.LookupByPath(workerCtx, src)
			if err != nil {
				_ = client.SendError(workerCtx, err)
				return
			}
			path := filepath.Join(destDir, sourceAttr.Name())

			wg.Add(1)
			select {
			case jobs <- restoreJob{dest: path, info: sourceAttr}:
			case <-workerCtx.Done():
				wg.Done()
			}
		}(source)
	}

	sourcesWg.Wait()
	wg.Wait()
	close(jobs)
	workersWg.Wait()

	return ctx.Err()
}

func processJob(ctx context.Context, client *Client, job restoreJob, jobs chan<- restoreJob, fsCap filesystemCapabilities, wg *sync.WaitGroup, noAttr bool) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
	}

	if job.info.IsDir() {
		return restoreDir(ctx, client, job.dest, job.info, jobs, fsCap, wg, noAttr)
	}
	if job.info.IsSymlink() {
		return restoreSymlink(ctx, client, job.dest, job.info, fsCap, noAttr)
	}
	if job.info.IsFile() {
		return restoreFile(ctx, client, job.dest, job.info, fsCap, noAttr)
	}
	return nil
}

func restoreFile(ctx context.Context, client *Client, path string, e EntryInfo, fsCap filesystemCapabilities, noAttr bool) error {
	needsUpdate, err := shouldUpdateFile(path, e, noAttr)
	if err != nil {
		return fmt.Errorf("check file %q: %w", path, err)
	}

	if needsUpdate {
		f, err := os.OpenFile(path, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0666)
		if err != nil {
			return fmt.Errorf("create file %q: %w", path, err)
		}
		defer f.Close()

		const bufSize = 256 * 1024 // 256KB buffer
		bw := bufio.NewWriterSize(f, bufSize)

		if e.Size > 0 && e.ContentRange != nil {
			rr := &rangeReader{
				ctx:          ctx,
				client:       client,
				contentStart: e.ContentRange[0],
				contentEnd:   e.ContentRange[1],
				totalSize:    e.Size,
			}

			if _, err := io.Copy(bw, rr); err != nil {
				return fmt.Errorf("copy data %q: %w", path, err)
			}
		}

		if err := bw.Flush(); err != nil {
			return fmt.Errorf("flush %q: %w", path, err)
		}

		if noAttr {
			return nil
		}

		return applyMeta(ctx, client, f, e, fsCap)
	}

	if !noAttr {
		f, err := os.OpenFile(path, os.O_RDWR, 0666)
		if err != nil {
			return fmt.Errorf("open file %q: %w", path, err)
		}
		return applyMeta(ctx, client, f, e, fsCap)
	}

	return nil
}

func restoreSymlink(ctx context.Context, client *Client, path string, e EntryInfo, fsCap filesystemCapabilities, noAttr bool) error {
	target, err := client.ReadLink(ctx, e.EntryRangeStart, e.EntryRangeEnd)
	if err != nil {
		return fmt.Errorf("readlink data %q: %w", path, err)
	}

	if existingTarget, err := os.Readlink(path); err == nil {
		if existingTarget == string(target) {
			if !noAttr {
				return applyMetaSymlink(ctx, client, path, e, fsCap)
			}
			return nil
		}
		_ = os.Remove(path)
	}

	if err := os.Symlink(string(target), path); err != nil {
		return fmt.Errorf("symlink %q: %w", path, err)
	}

	if noAttr {
		return nil
	}

	return applyMetaSymlink(ctx, client, path, e, fsCap)
}

func shouldUpdateFile(path string, archiveInfo EntryInfo, noAttr bool) (bool, error) {
	stat, err := os.Lstat(path)
	if os.IsNotExist(err) {
		return true, nil
	}
	if err != nil {
		return false, err
	}

	if stat.IsDir() != archiveInfo.IsDir() {
		return true, nil
	}
	if (stat.Mode()&os.ModeSymlink != 0) != archiveInfo.IsSymlink() {
		return true, nil
	}

	if archiveInfo.IsFile() {
		if stat.Size() != int64(archiveInfo.Size) {
			return true, nil
		}
		if !noAttr {
			if stat.ModTime().Unix() != archiveInfo.MtimeSecs {
				return true, nil
			}
		}
	}

	if archiveInfo.IsSymlink() {
		return true, nil
	}

	return false, nil
}
