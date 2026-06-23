package pxar

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"runtime"
	"runtime/debug"
	"strconv"
	"strings"
	"sync"

	"github.com/fxamacker/cbor/v2"

	"github.com/pbs-plus/pbs-plus/internal/syslog"
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

// reportErr sends a metadata error to the server immediately (never batched
// or deferred). Metadata errors are non-fatal — content is already in place —
func (st *restoreState) reportErr(ctx context.Context, op, path string, err error) {
	if err == nil {
		return
	}
	e := fmt.Errorf("%s %q: %w", op, path, err)
	if serr := st.client.SendError(ctx, e); serr != nil {
		syslog.L.Error(e).
			WithField("restore", "error-report-failed").
			WithField("op", op).
			WithField("sendErr", serr.Error()).
			Write()
	}
}

// runJobRecovered wraps processJob with panic recovery. An unrecovered panic
// in a goroutine terminates the process, leaking temp files and losing queued
// errors. Recovering here converts the panic into a reported error so Done
// always fires and cleanup runs.
func (st *restoreState) runJobRecovered(ctx context.Context, client *Client, job restoreJob) (err error) {
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("panic restoring %q: %v\n%s", job.dest, r, debug.Stack())
			syslog.L.Error(err).
				WithJob(client.name).
				WithField("restore", "panic").
				WithField("dest", job.dest).
				Write()
		}
	}()
	return processJob(ctx, st, job)
}

// forward-slash notation as the hardlink registry key so LinkTarget matches
type restoreJob struct {
	dest    string
	srcPath string
	info    pxar.FileInfo
}

type restoreState struct {
	client *Client
	fsCap  filesystemCapabilities
	noAttr bool
	hl     *hardlinkIndex
	jobs   chan<- restoreJob
	wg     *sync.WaitGroup
}

const unixSecsMax = 32503680000

func parseXattrUnixSecs(data []byte) (secs int64, ok bool) {
	if len(data) == 0 {
		return 0, false
	}
	v, err := strconv.ParseInt(string(data), 10, 64)
	if err != nil || v < 0 || v > unixSecsMax {
		return 0, false
	}
	return v, true
}

// aclFlavor discriminates POSIX vs Windows encoding of a user.acls payload
// so a cross-platform restore never applies a foreign ACL type.
type aclFlavor int

const (
	aclNone aclFlavor = iota
	aclPosix
	aclWindows
)

// detectACLFlavor probes the field discriminator ("sid" vs "tag") in a
// user.acls blob to report POSIX, Windows, or neither. cbor ignores unknown
// fields, so a foreign payload leaves its discriminator empty.
func detectACLFlavor(data []byte) aclFlavor {
	if len(data) == 0 {
		return aclNone
	}
	var probe []struct {
		SID string `cbor:"sid"`
		Tag string `cbor:"tag"`
	}
	if cbor.Unmarshal(data, &probe) != nil {
		return aclNone
	}
	var hasPosix, hasWindows bool
	for _, p := range probe {
		if p.SID != "" {
			hasWindows = true
		}
		if p.Tag != "" {
			hasPosix = true
		}
	}
	switch {
	case hasPosix && !hasWindows:
		return aclPosix
	case hasWindows && !hasPosix:
		return aclWindows
	default:
		return aclNone
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

// cleanupStaleTemps removes leftover .pxar-restore-* temp files from a prior
// crash, walking the destination root and deleting matches without aborting.
func cleanupStaleTemps(root string) {
	if err := filepath.WalkDir(root, func(path string, d os.DirEntry, err error) error {
		if err != nil {
			return nil
		}
		if d.IsDir() {
			return nil
		}
		name := d.Name()
		if strings.HasPrefix(name, ".pxar-restore-") {
			if rmErr := os.Remove(path); rmErr != nil && !os.IsNotExist(rmErr) {
				syslog.L.Warn().
					WithMessage("restore: could not remove stale temp file").
					WithField("path", path).
					WithField("error", rmErr.Error()).
					Write()
			}
		}
		return nil
	}); err != nil {
		syslog.L.Error(err).Write()
	}
}

func restoreNormal(ctx context.Context, client *Client, sources []string, destDir string, noAttr bool) error {
	cleanupStaleTemps(destDir)

	prepareRestoreProcess()

	fsCap := getFilesystemCapabilities(destDir)

	numCPU := runtime.NumCPU()
	numWorkers := numCPU * 2
	if fsCap.prefersSequentialOps {
		numWorkers = min(numCPU, 2)
	}

	jobs := make(chan restoreJob, 1024)
	var wg sync.WaitGroup

	st := &restoreState{
		client: client,
		fsCap:  fsCap,
		noAttr: noAttr,
		hl:     newHardlinkIndex(),
		jobs:   jobs,
		wg:     &wg,
	}

	workerCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	syslog.L.Info().
		WithJob(client.name).
		WithMessage("restore: starting content restore").
		WithField("sources", len(sources)).
		WithField("dest", destDir).
		WithField("workers", numWorkers).
		Write()

	var workersWg sync.WaitGroup
	for i := 0; i < numWorkers; i++ {
		workersWg.Go(func() {
			for job := range jobs {
				func() {
					defer wg.Done()
					if err := st.runJobRecovered(workerCtx, client, job); err != nil {
						if serr := client.SendError(workerCtx, err); serr != nil {
							syslog.L.Error(err).
								WithField("restore", "error-report-failed").
								WithField("sendErr", serr.Error()).
								Write()
						}
					}
				}()
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
				if serr := client.SendError(workerCtx, err); serr != nil {
					syslog.L.Error(err).
						WithField("restore", "error-report-failed").
						WithField("sendErr", serr.Error()).
						Write()
				}
				return
			}
			dest := filepath.Join(destDir, sourceAttr.Name())
			srcPath := strings.TrimPrefix(filepath.ToSlash(sourceAttr.Name()), "/")

			wg.Add(1)
			select {
			case jobs <- restoreJob{dest: dest, srcPath: srcPath, info: sourceAttr}:
			case <-workerCtx.Done():
				wg.Done()
			}
		}(source)
	}

	sourcesWg.Wait()
	wg.Wait()
	close(jobs)
	workersWg.Wait()

	syslog.L.Info().
		WithJob(client.name).
		WithMessage("restore: all workers drained, resolving deferred hardlinks").
		Write()

	func() {
		defer func() {
			if r := recover(); r != nil {
				err := fmt.Errorf("panic resolving deferred hardlinks: %v\n%s", r, debug.Stack())
				syslog.L.Error(err).WithJob(client.name).WithField("restore", "panic").Write()
				if err := client.SendError(workerCtx, err); err != nil {
					syslog.L.Error(err).Write()
				}
			}
		}()
		if err := resolveDeferredHardlinks(workerCtx, st); err != nil {
			if serr := client.SendError(workerCtx, err); serr != nil {
				syslog.L.Error(err).
					WithField("restore", "error-report-failed").
					WithField("sendErr", serr.Error()).
					Write()
			}
		}
	}()

	syslog.L.Info().
		WithJob(client.name).
		WithMessage("restore: content restore complete, returning to caller").
		Write()

	return ctx.Err()
}

// restored during the concurrent pass. Runs after the worker pool drains.
func resolveDeferredHardlinks(ctx context.Context, st *restoreState) error {
	var errs []error
	for _, job := range st.hl.drainDeferred() {
		if target, ok := st.hl.tryResolveNow(job); ok {
			if err := linkFile(target, job.dest); err != nil {
				if ferr := restoreFileContent(ctx, st, job); ferr != nil {
					errs = append(errs, fmt.Errorf("hardlink %q: %w", job.dest, ferr))
				}
			}
		} else {
			if err := restoreFileContent(ctx, st, job); err != nil {
				errs = append(errs, fmt.Errorf("hardlink %q (target %q not in scope): %w", job.dest, job.info.LinkTarget, err))
			}
		}
		st.hl.register(job.srcPath, job.dest)
	}
	return errors.Join(errs...)
}

func processJob(ctx context.Context, st *restoreState, job restoreJob) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
	}

	var err error
	switch job.info.FileType {
	case pxar.FileTypeDirectory:
		err = restoreDir(ctx, st, job)
	case pxar.FileTypeSymlink:
		err = restoreSymlink(ctx, st, job)
	case pxar.FileTypeFile:
		err = restoreFile(ctx, st, job)
	case pxar.FileTypeHardlink:
		if target, ok := st.hl.tryResolveNow(job); ok {
			if lerr := linkFile(target, job.dest); lerr != nil {
				if cerr := restoreFileContent(ctx, st, job); cerr != nil {
					return fmt.Errorf("hardlink %q: link failed (%v) and fallback failed: %w", job.dest, lerr, cerr)
				}
			}
		} else {
			st.hl.deferLink(job)
			return nil
		}
	default:
		return nil
	}
	if err != nil {
		return err
	}
	st.hl.register(job.srcPath, job.dest)
	return nil
}

// restoreFile skips content when size+whole-second mtime match (rsync-style
// quick check) but still reconciles metadata. Otherwise writes to a temp and
// atomically renames, so a crash never leaves a partial destination.
func restoreFile(ctx context.Context, st *restoreState, job restoreJob) error {
	update, err := shouldUpdateFile(job.dest, job.info, st.noAttr)
	if err != nil {
		return fmt.Errorf("check file %q: %w", job.dest, err)
	}

	if !update {
		if st.noAttr {
			return nil
		}
		f, err := os.OpenFile(job.dest, os.O_RDWR, 0666)
		if err != nil {
			return fmt.Errorf("open file %q: %w", job.dest, err)
		}
		return applyMeta(ctx, st, f, job.info)
	}

	return restoreFileContent(ctx, st, job)
}

// restoreFileContent writes to a hidden temp, atomically swaps it into place,
// destination, never the temp: on Windows, a restrictive DACL or READONLY
// attribute on the temp blocks rename/cleanup, leaking .pxar-restore-* files.
func restoreFileContent(ctx context.Context, st *restoreState, job restoreJob) error {
	e := job.info
	if e.FileType == pxar.FileTypeHardlink && (e.RawSize == 0 || e.ContentRange == nil) {
		return fmt.Errorf("hardlink %q (target %q) has no restorable content", job.dest, e.LinkTarget)
	}

	tmpPath, err := streamToTemp(ctx, st, job)
	if err != nil {
		return err
	}

	if err := atomicSwap(tmpPath, job.dest); err != nil {
		return err
	}

	if st.noAttr {
		return applyTempMode(job.dest, e.RawMode)
	}
	f, err := os.OpenFile(job.dest, os.O_RDWR, 0666)
	if err != nil {
		return fmt.Errorf("open restored file for metadata %q: %w", job.dest, err)
	}
	return applyMeta(ctx, st, f, e)
}

// (so the final rename is same-filesystem atomic). On error the temp is
func streamToTemp(ctx context.Context, st *restoreState, job restoreJob) (tmpPath string, err error) {
	e := job.info
	dir := filepath.Dir(job.dest)

	f, err := os.CreateTemp(dir, ".pxar-restore-*")
	if err != nil {
		return "", fmt.Errorf("create temp for %q: %w", job.dest, err)
	}
	tmpPath = f.Name()

	defer func() {
		if err != nil {
			if cerr := f.Close(); cerr != nil {
				syslog.L.Error(cerr).Write()
			}
			if rmErr := os.Remove(tmpPath); rmErr != nil && !os.IsNotExist(rmErr) {
				err = fmt.Errorf("%w (temp cleanup of %q failed: %v)", err, tmpPath, rmErr)
			}
			tmpPath = ""
		}
	}()

	if e.RawSize > 0 && e.ContentRange != nil {
		rc, rerr := st.client.ReadFileContentReader(ctx, e.ContentRange[0], e.ContentRange[1], e.RawSize)
		if rerr != nil {
			return tmpPath, fmt.Errorf("open content reader %q: %w", job.dest, rerr)
		}
		const bufSize = 256 * 1024
		buf := make([]byte, bufSize)
		_, werr := io.CopyBuffer(f, rc, buf)
		if cerr := rc.Close(); cerr != nil && werr == nil {
			werr = cerr
		}
		if werr != nil {
			return tmpPath, fmt.Errorf("copy data %q: %w", job.dest, werr)
		}
	}

	if err = f.Sync(); err != nil {
		return tmpPath, fmt.Errorf("sync temp %q: %w", job.dest, err)
	}
	if err = f.Close(); err != nil {
		return tmpPath, fmt.Errorf("close temp %q: %w", job.dest, err)
	}
	return tmpPath, nil
}

// atomicSwap moves tmpPath over dest via atomic rename, removing an
// incompatible existing dest and retrying once, then falling back to a
func atomicSwap(tmpPath, dest string) (retErr error) {
	defer func() {
		if retErr != nil {
			if rmErr := os.Remove(tmpPath); rmErr != nil && !os.IsNotExist(rmErr) {
				retErr = fmt.Errorf("%w (temp cleanup of %q failed: %v)", retErr, tmpPath, rmErr)
			}
		}
	}()

	if err := os.Rename(tmpPath, dest); err == nil {
		return nil
	}

	if rerr := os.Remove(dest); rerr != nil && !os.IsNotExist(rerr) {
		return fmt.Errorf("replace %q: could not remove existing entry: %w", dest, rerr)
	}
	if err := os.Rename(tmpPath, dest); err == nil {
		return nil
	}

	if err := copyFile(tmpPath, dest); err != nil {
		return fmt.Errorf("swap %q -> %q: rename and copy fallback both failed: %w", tmpPath, dest, err)
	}
	return nil
}

func copyFile(src, dst string) error {
	in, err := os.Open(src)
	if err != nil {
		return err
	}
	defer func() {
		if err := in.Close(); err != nil {
			syslog.L.Error(err).Write()
		}
	}()

	out, err := os.OpenFile(dst, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0o666)
	if err != nil {
		return err
	}
	if _, err := io.Copy(out, in); err != nil {
		if err := out.Close(); err != nil {
			syslog.L.Error(err).Write()
		}
		return err
	}
	return out.Close()
}

func restoreSymlink(ctx context.Context, st *restoreState, job restoreJob) error {
	e := job.info
	target, err := st.client.ReadLink(ctx, e.EntryRangeStart, e.EntryRangeEnd)
	if err != nil {
		return fmt.Errorf("readlink data %q: %w", job.dest, err)
	}

	if existingTarget, err := os.Readlink(job.dest); err == nil {
		if existingTarget == string(target) {
			if !st.noAttr {
				return applyMetaSymlink(ctx, st, job.dest, e)
			}
			return nil
		}
		if err := os.Remove(job.dest); err != nil && !os.IsNotExist(err) {
			syslog.L.Error(err).Write()
		}
	}

	if err := os.Symlink(string(target), job.dest); err != nil {
		return fmt.Errorf("symlink %q: %w", job.dest, err)
	}

	if st.noAttr {
		return nil
	}

	return applyMetaSymlink(ctx, st, job.dest, e)
}

// matches on size and whole-second mtime (rsync-style quick check).
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
