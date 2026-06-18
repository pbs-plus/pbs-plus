package pxar

import (
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"runtime"
	"strconv"
	"sync"
	"time"

	pxar "github.com/pbs-plus/pxar"
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
	info pxar.FileInfo
}

// Bounds for a plausible Unix-second timestamp (years ~1970..2100). A value
// inside this range is accepted as seconds. A much larger value that is
// plausible as nanoseconds is normalized to seconds. Anything else is
// rejected so a malformed/stale xattr can never produce an invalid time on
// the restored file.
const (
	unixSecsMin = 0       // 1970-01-01
	unixSecsMax = 1 << 33 // ~ year 2242; comfortably beyond any real file
	unixNanosLo = 1 << 47 // ~ year 4317 in seconds; clearly nanoseconds
	unixNanosHi = 1 << 61 // ~ year 2262 in nanoseconds (time.Time max-ish)
)

// parseXattrUnixSecs decodes a serialized xattr timestamp into a validated
// Unix-second value. The backup writer (arpcfs, both legacy and current
// modes) stores these as a decimal string of an int64 Unix-seconds value on
// both Unix and Windows agents. parseXattrUnixSecs additionally tolerates a
// value encoded in nanoseconds so that a cross-platform restore (e.g. a
// Windows source restored to Linux, or an older/legacy agent variant) can
// never yield an out-of-range time. ok is false for empty, non-numeric, or
// implausible values, in which case the caller keeps its fallback time.
func parseXattrUnixSecs(data []byte) (secs int64, ok bool) {
	s := string(data)
	if s == "" {
		return 0, false
	}
	v, err := strconv.ParseInt(s, 10, 64)
	if err != nil {
		return 0, false
	}
	switch {
	case v >= unixSecsMin && v <= unixSecsMax:
		return v, true
	case v >= unixNanosLo && v <= unixNanosHi:
		// Defensively interpret as nanoseconds; older agents or alternate
		// serializations may have stored nanos. Round to the nearest second.
		return v / int64(time.Second), true
	default:
		return 0, false
	}
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

	switch job.info.FileType {
	case pxar.FileTypeDirectory:
		return restoreDir(ctx, client, job.dest, job.info, jobs, fsCap, wg, noAttr)
	case pxar.FileTypeSymlink:
		return restoreSymlink(ctx, client, job.dest, job.info, fsCap, noAttr)
	case pxar.FileTypeFile, pxar.FileTypeHardlink:
		return restoreFile(ctx, client, job.dest, job.info, fsCap, noAttr)
	}
	return nil
}

func restoreFile(ctx context.Context, client *Client, path string, e pxar.FileInfo, fsCap filesystemCapabilities, noAttr bool) error {
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

		if e.RawSize > 0 && e.ContentRange != nil {
			rc, err := client.ReadFileContentReader(ctx, e.ContentRange[0], e.ContentRange[1], e.RawSize)
			if err != nil {
				return fmt.Errorf("open content reader %q: %w", path, err)
			}
			defer rc.Close()

			const bufSize = 256 * 1024
			buf := make([]byte, bufSize)
			if _, err := io.CopyBuffer(f, rc, buf); err != nil {
				return fmt.Errorf("copy data %q: %w", path, err)
			}
		} else {
			if err := f.Sync(); err != nil {
				return fmt.Errorf("sync empty file %q: %w", path, err)
			}
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

func restoreSymlink(ctx context.Context, client *Client, path string, e pxar.FileInfo, fsCap filesystemCapabilities, noAttr bool) error {
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

func shouldUpdateFile(path string, archiveInfo pxar.FileInfo, noAttr bool) (bool, error) {
	stat, err := os.Lstat(path)
	if os.IsNotExist(err) {
		return true, nil
	}
	if err != nil {
		return false, err
	}

	archiveIsDir := archiveInfo.FileType == pxar.FileTypeDirectory
	archiveIsSymlink := archiveInfo.FileType == pxar.FileTypeSymlink
	archiveIsFile := archiveInfo.FileType == pxar.FileTypeFile || archiveInfo.FileType == pxar.FileTypeHardlink

	if stat.IsDir() != archiveIsDir {
		return true, nil
	}
	if (stat.Mode()&os.ModeSymlink != 0) != archiveIsSymlink {
		return true, nil
	}

	if archiveIsFile {
		if stat.Size() != int64(archiveInfo.RawSize) {
			return true, nil
		}
		if !noAttr {
			if stat.ModTime().Unix() != archiveInfo.MtimeSecs {
				return true, nil
			}
		}
	}

	if archiveIsSymlink {
		return true, nil
	}

	return false, nil
}
