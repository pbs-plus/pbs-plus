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
	"time"

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

// reportErr sends a single metadata error to the server IMMEDIATELY as it
// happens — it is never batched or deferred. Metadata errors (chmod, chown,
// setxattr, utimes, ACL/file-attribute application, ...) are non-fatal (the
// file content is already in place), so they are surfaced to the operator
// via the task log but never abort the restore. Each call performs one
// client.SendError, which for a remote client is a synchronous RPC (the
// server receives it before reportErr returns) and for a local client is a
// channel send consumed by the job's error collector. A failure to forward
// the error is itself logged locally so nothing is ever swallowed.
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

// runJobRecovered executes a single restore job with panic recovery. Without
// this, a panic in any worker (e.g. a nil dereference or a Windows syscall
// edge case) crashes the ENTIRE process: Go terminates on an unrecovered
// panic in any goroutine, defers in other goroutines never run, queued errors
// are lost, the pxar.Done signal is never sent, and in-flight .pxar-restore-*
// temp files leak — exactly the "agent disconnected, no done signal, stale
// temps" symptom. Recovering here converts a process-killing panic into a
// reported error (with stack) and lets the restore continue, so Done always
// fires and cleanup runs. The stack is sent to the server task log AND logged
// locally so it appears in the agent's Windows Event Log for diagnosis.
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

// cleanupStaleTemps removes leftover .pxar-restore-* temp files from a prior
// restore run that was killed or crashed before it could swap them into place.
// It walks the destination root and deletes any file whose base name matches
// the temp pattern, reporting failures but never aborting the restore. This
// is the safety net that guarantees no .pxar-restore-* files survive across
// restore runs even if a previous run was forcibly terminated mid-swap.
func cleanupStaleTemps(root string) {
	_ = filepath.WalkDir(root, func(path string, d os.DirEntry, err error) error {
		if err != nil {
			return nil // best-effort; keep walking
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
	})
}

func restoreNormal(ctx context.Context, client *Client, sources []string, destDir string, noAttr bool) error {
	// Remove temp files leaked by any prior restore that was killed/crashed
	// before swapping them into place, so .pxar-restore-* never accumulates.
	cleanupStaleTemps(destDir)

	// On Windows this enables SeRestore/SeBackup/SeTakeOwnership/SeSecurity
	// in the process token so owner/group/DACL writes succeed on files the
	// restore user does not own. No-op on Unix. Runs once per process.
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
				// defer wg.Done per job so a recovered panic (or an early
				// continue) can never underflow/over-hold the WaitGroup and
				// deadlock restoreNormal.
				func() {
					defer wg.Done()
					if err := st.runJobRecovered(workerCtx, client, job); err != nil {
						if serr := client.SendError(workerCtx, err); serr != nil {
							// The error itself couldn't be forwarded to the server
							// (connection lost / context cancelled). Log it locally
							// so it is never silently lost.
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

	// Final hardlink sweep: every regular target is now registered, so any
	// link deferred during the concurrent pass can be resolved. Links whose
	// targets live outside the restored source set (or whose filesystem
	// cannot hard-link) fall back to a byte copy. Recovered so a panic in the
	// sweep cannot crash the process after the bulk restore already finished.
	func() {
		defer func() {
			if r := recover(); r != nil {
				err := fmt.Errorf("panic resolving deferred hardlinks: %v\n%s", r, debug.Stack())
				syslog.L.Error(err).WithJob(client.name).WithField("restore", "panic").Write()
				_ = client.SendError(workerCtx, err)
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

// restoreFileContent writes the entry's bytes to a hidden temp file in the
// destination's directory, atomically swaps it into place, and only THEN
// applies metadata to the final file. Metadata is deliberately applied to
// the destination, never the temp: on Windows, applying a restrictive DACL or
// FILE_ATTRIBUTE_READONLY to the temp would make the subsequent rename and
// temp-cleanup fail with ACCESS_DENIED, leaking .pxar-restore-* files (the
// symptom on Windows restores). This order also matches rsync, which sets
// permissions last. A hardlink entry that carries no content range and cannot
// be linked is reported as an error rather than silently producing an empty
// file.
func restoreFileContent(ctx context.Context, st *restoreState, job restoreJob) error {
	e := job.info
	if e.FileType == pxar.FileTypeHardlink && (e.RawSize == 0 || e.ContentRange == nil) {
		return fmt.Errorf("hardlink %q (target %q) has no restorable content", job.dest, e.LinkTarget)
	}

	tmpPath, err := streamToTemp(ctx, st, job)
	if err != nil {
		return err
	}

	// Swap temp into place BEFORE any metadata: the temp retains its default
	// permissive CreateTemp attributes here, so rename cannot be blocked by a
	// restrictive ACL the restore itself just wrote.
	if err := atomicSwap(tmpPath, job.dest); err != nil {
		return err
	}

	// Apply metadata to the final file. Any metadata failure is returned so the
	// caller can surface it in the task log, but by this point the file is
	// already in place with correct content, so the error is effectively a
	// warning (it does not roll back the restore of the file).
	if st.noAttr {
		// Give the file sane perms even in no-attr mode (CreateTemp uses 0600).
		return applyTempMode(job.dest, e.RawMode)
	}
	f, err := os.OpenFile(job.dest, os.O_RDWR, 0666)
	if err != nil {
		return fmt.Errorf("open restored file for metadata %q: %w", job.dest, err)
	}
	return applyMeta(ctx, st, f, e)
}

// streamToTemp streams the archived content into a hidden temp file in the
// destination's directory, fsyncs, and closes it. No metadata is applied —
// the temp keeps CreateTemp's default mode. On any error the temp is removed
// (and a removal failure is folded into the returned error rather than
// swallowed). The temp lives in the same directory as the destination so the
// final rename is a same-filesystem atomic operation.
func streamToTemp(ctx context.Context, st *restoreState, job restoreJob) (tmpPath string, err error) {
	e := job.info
	dir := filepath.Dir(job.dest)

	f, err := os.CreateTemp(dir, ".pxar-restore-*")
	if err != nil {
		return "", fmt.Errorf("create temp for %q: %w", job.dest, err)
	}
	tmpPath = f.Name()

	// On any error path below, close and remove the temp so it can never leak.
	// A failed removal is folded into the error so it is visible, not swallowed.
	defer func() {
		if err != nil {
			_ = f.Close()
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

// atomicSwap moves tmpPath over dest. It prefers the atomic rename path
// (rename(2) on Unix, MoveFileEx+MOVEFILE_REPLACE_EXISTING on Windows). If the
// rename fails — typically because dest exists as an incompatible type
// (directory/symlink) or is locked — it removes the conflicting dest and
// retries once. As a last resort it byte-copies the temp over dest so the
// restore still completes when rename is unavailable on the target FS/OS.
// The temp is GUARANTEED to be removed on every failure path, and any cleanup
// error is folded into the returned error.
func atomicSwap(tmpPath, dest string) (retErr error) {
	// Whatever happens, the temp must not be left behind.
	defer func() {
		if retErr != nil {
			if rmErr := os.Remove(tmpPath); rmErr != nil && !os.IsNotExist(rmErr) {
				retErr = fmt.Errorf("%w (temp cleanup of %q failed: %v)", retErr, tmpPath, rmErr)
			}
		}
	}()

	// Fast path: direct atomic replace.
	if err := os.Rename(tmpPath, dest); err == nil {
		return nil
	}

	// Retry path: a conflicting existing dest of an incompatible type blocks
	// the replace. Remove it (only a regular file/symlink; an empty dir is also
	// removable) and retry. A non-empty dir removal fails here, correctly
	// aborting rather than destroying data.
	if rerr := os.Remove(dest); rerr != nil && !os.IsNotExist(rerr) {
		return fmt.Errorf("replace %q: could not remove existing entry: %w", dest, rerr)
	}
	if err := os.Rename(tmpPath, dest); err == nil {
		return nil
	}

	// Fallback path: rename is unavailable/broken on this FS or OS (observed on
	// some Windows configurations). Byte-copy the already-written temp into
	// place so the restore completes; the deferred cleanup then removes the
	// temp. This sacrifices crash-atomicity on this rare path in exchange for
	// never failing or leaking a temp.
	if err := copyFile(tmpPath, dest); err != nil {
		return fmt.Errorf("swap %q -> %q: rename and copy fallback both failed: %w", tmpPath, dest, err)
	}
	return nil
}

// copyFile copies the contents of src to dst, truncating dst. Used only as the
// last-resort fallback in atomicSwap when rename is not usable.
func copyFile(src, dst string) error {
	in, err := os.Open(src)
	if err != nil {
		return err
	}
	defer func() { _ = in.Close() }()

	out, err := os.OpenFile(dst, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0o666)
	if err != nil {
		return err
	}
	if _, err := io.Copy(out, in); err != nil {
		_ = out.Close()
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
