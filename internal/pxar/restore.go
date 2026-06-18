package pxar

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/fxamacker/cbor/v2"

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

// restoreJob describes a single entry to restore. dest is the OS-native
// destination path; srcPath is the archive-relative path (forward-slash, no
// leading slash) used as the hardlink registry key so a hardlink's LinkTarget
// can be matched regardless of host OS path separators.
type restoreJob struct {
	dest    string
	srcPath string
	info    pxar.FileInfo
}

// restoreState bundles the per-restore shared state threaded through every
// worker, avoiding wide parameter lists and keeping the worker entry points
// uniform across platforms.
type restoreState struct {
	client *Client
	fsCap  filesystemCapabilities
	noAttr bool
	hl     *hardlinkIndex
	jobs   chan<- restoreJob
	wg     *sync.WaitGroup
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

// aclFlavor discriminates the encoding of a serialized user.acls payload.
// The backup writer emits one or the other depending on the source agent —
// server_unix stores []PosixACL, server_windows stores []WinACL — so the
// restore must not assume its destination-native type. Applying a foreign
// type (e.g. writing a []WinACL payload as a POSIX ACL on Linux) yields a
// corrupt ACL, which is what this guards against.
type aclFlavor int

const (
	// aclNone means the payload is absent, undecodable, or carries neither
	// a recognizable POSIX nor Windows ACL. Callers skip applying it.
	aclNone aclFlavor = iota
	// aclPosix means the payload decodes as []PosixACL (entries have a "tag").
	aclPosix
	// aclWindows means the payload decodes as []WinACL (entries have a "sid").
	aclWindows
)

// detectACLFlavor inspects a serialized user.acls blob and reports whether it
// carries POSIX or Windows ACLs. It probes the field discriminator ("sid" for
// Windows vs "tag" for POSIX) rather than committing to either struct, so a
// cross-platform restore can decide whether the ACLs are applicable to the
// destination OS. fxamacker/cbor ignores unknown fields by default, so probing
// a WinACL payload into the POSIX-side field leaves "tag" empty (and vice
// versa). Empty/undecodable/mixed payloads resolve to aclNone.
func detectACLFlavor(data []byte) aclFlavor {
	if len(data) == 0 {
		return aclNone
	}
	var probe []struct {
		SID string `cbor:"sid"` // Windows ACL discriminator
		Tag string `cbor:"tag"` // POSIX ACL discriminator
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

func restoreNormal(ctx context.Context, client *Client, sources []string, destDir string, noAttr bool) error {
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

	var workersWg sync.WaitGroup
	for i := 0; i < numWorkers; i++ {
		workersWg.Go(func() {
			for job := range jobs {
				if err := processJob(workerCtx, st, job); err != nil {
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

	// Final hardlink sweep: every regular target is now registered, so any
	// link deferred during the concurrent pass can be resolved. Links whose
	// targets live outside the restored source set (or whose filesystem
	// cannot hard-link) fall back to a byte copy.
	if err := resolveDeferredHardlinks(workerCtx, st); err != nil {
		_ = client.SendError(workerCtx, err)
	}

	return ctx.Err()
}

// resolveDeferredHardlinks processes every hardlink that could not be linked
// inline because its target had not yet been restored by another worker. It
// runs after the worker pool has drained, so all in-scope targets are
// registered. Per-link errors are accumulated and reported together.
func resolveDeferredHardlinks(ctx context.Context, st *restoreState) error {
	var errs []error
	for _, job := range st.hl.drainDeferred() {
		if target, ok := st.hl.tryResolveNow(job); ok {
			if err := linkFile(target, job.dest); err != nil {
				// os.Link rejected the link (e.g. FAT/exFAT, cross-device).
				// Fall back to copying the bytes if the entry carries content.
				if ferr := restoreFileContent(ctx, st, job); ferr != nil {
					errs = append(errs, fmt.Errorf("hardlink %q: %w", job.dest, ferr))
				}
			}
		} else {
			// Target is outside the restored source set. A pxar hardlink entry
			// does not carry its own content, so this is only recoverable when
			// the entry happens to embed a content range.
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
		// Try to reconstruct the hard link against an already-restored target.
		// If the target isn't restored yet (worker ordering), defer it for the
		// final sweep rather than blocking a worker.
		if target, ok := st.hl.tryResolveNow(job); ok {
			if lerr := linkFile(target, job.dest); lerr != nil {
				// Link unsupported here (FAT, cross-device) — copy the bytes.
				if cerr := restoreFileContent(ctx, st, job); cerr != nil {
					return fmt.Errorf("hardlink %q: link failed (%v) and fallback failed: %w", job.dest, lerr, cerr)
				}
			}
		} else {
			st.hl.deferLink(job)
			return nil // resolved in the sweep; not registered yet
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

// restoreFile applies the rsync-style quick check (size + whole-second mtime)
// before transferring any content. When the existing file already matches, it
// skips the content rewrite but still reconciles metadata — rsync likewise
// repairs permissions/ACLs on skipped files when preserving them. When the
// content differs or is absent, the new bytes are written to a hidden temp
// file in the same directory and atomically renamed into place, so a crash or
// cancellation never leaves a partially-written destination (rsync's default
// temp-file + rename behavior, the opposite of --inplace).
func restoreFile(ctx context.Context, st *restoreState, job restoreJob) error {
	update, err := shouldUpdateFile(job.dest, job.info, st.noAttr)
	if err != nil {
		return fmt.Errorf("check file %q: %w", job.dest, err)
	}

	if !update {
		// Quick-check hit: content already correct. Reconcile metadata only.
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

// restoreFileContent writes the entry's bytes to a same-directory temp file,
// applies its metadata, fsyncs, closes, and atomically renames it over the
// destination. A hardlink entry that carries no content range and cannot be
// linked is reported as an error rather than silently producing an empty file.
func restoreFileContent(ctx context.Context, st *restoreState, job restoreJob) error {
	e := job.info
	if e.FileType == pxar.FileTypeHardlink && (e.RawSize == 0 || e.ContentRange == nil) {
		return fmt.Errorf("hardlink %q (target %q) has no restorable content", job.dest, e.LinkTarget)
	}

	tmpPath, err := writeTempFile(ctx, st, job)
	if err != nil {
		return err
	}
	return atomicSwap(tmpPath, job.dest)
}

// writeTempFile creates a hidden temp file in the destination's directory,
// streams the archived content into it, fsyncs, applies metadata (closing the
// file), and returns the temp path ready for an atomic rename. On any error
// the temp file is removed. The temp lives in the same directory as the
// destination so the final rename is a same-filesystem atomic operation.
func writeTempFile(ctx context.Context, st *restoreState, job restoreJob) (string, error) {
	e := job.info
	dir := filepath.Dir(job.dest)

	f, err := os.CreateTemp(dir, ".pxar-restore-*")
	if err != nil {
		return "", fmt.Errorf("create temp for %q: %w", job.dest, err)
	}
	tmpPath := f.Name()

	// fail removes the temp and returns err while the file handle is still open.
	fail := func(err error) (string, error) {
		_ = f.Close()
		_ = os.Remove(tmpPath)
		return "", err
	}

	if e.RawSize > 0 && e.ContentRange != nil {
		rc, err := st.client.ReadFileContentReader(ctx, e.ContentRange[0], e.ContentRange[1], e.RawSize)
		if err != nil {
			return fail(fmt.Errorf("open content reader %q: %w", job.dest, err))
		}
		const bufSize = 256 * 1024
		buf := make([]byte, bufSize)
		_, werr := io.CopyBuffer(f, rc, buf)
		_ = rc.Close()
		if werr != nil {
			return fail(fmt.Errorf("copy data %q: %w", job.dest, werr))
		}
	}
	if err := f.Sync(); err != nil {
		return fail(fmt.Errorf("sync temp %q: %w", job.dest, err))
	}

	if !st.noAttr {
		// applyMeta closes f (deferred file.Close) and sets mode/owner/times/
		// xattrs/ACLs on the temp, so the renamed file is fully complete.
		if err := applyMeta(ctx, st, f, e); err != nil {
			// f is already closed by applyMeta; only the path remains to clean up.
			_ = os.Remove(tmpPath)
			return "", err
		}
	} else {
		// Give the swapped file sensible perms even in no-attr mode (CreateTemp
		// uses 0600); fall back to 0666 when the archive recorded no mode.
		_ = applyTempMode(tmpPath, e.RawMode)
		if err := f.Close(); err != nil {
			_ = os.Remove(tmpPath)
			return "", fmt.Errorf("close temp %q: %w", job.dest, err)
		}
	}

	return tmpPath, nil
}

// atomicSwap renames tmpPath over dest. A direct rename atomically replaces
// an existing regular file on both Unix (rename(2)) and Windows
// (MoveFileEx + MOVEFILE_REPLACE_EXISTING). If it fails — typically because
// dest exists as an incompatible type (directory/symlink) — the conflicting
// entry is removed and the rename retried once.
func atomicSwap(tmpPath, dest string) error {
	if err := os.Rename(tmpPath, dest); err == nil {
		return nil
	}
	if rerr := os.Remove(dest); rerr != nil && !os.IsNotExist(rerr) {
		_ = os.Remove(tmpPath)
		return fmt.Errorf("replace %q: %w", dest, rerr)
	}
	if err := os.Rename(tmpPath, dest); err != nil {
		_ = os.Remove(tmpPath)
		return fmt.Errorf("rename temp %q -> %q: %w", tmpPath, dest, err)
	}
	return nil
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
		_ = os.Remove(job.dest)
	}

	if err := os.Symlink(string(target), job.dest); err != nil {
		return fmt.Errorf("symlink %q: %w", job.dest, err)
	}

	if st.noAttr {
		return nil
	}

	return applyMetaSymlink(ctx, st, job.dest, e)
}

// shouldUpdateFile implements rsync's default quick check: a file is skipped
// (returns false) only when an existing entry of the same type has matching
// size and whole-second mtime. Type mismatches, missing entries, and content
// differences all force an update. mtime is compared at whole-second
// granularity because that is rsync's default modify-window (it stores and
// compares times as seconds; sub-second precision is not part of the skip
// decision).
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
