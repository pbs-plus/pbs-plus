package pxar

import (
	"os"
	"path"
	"strings"
	"sync"
)

// hardlinkIndex tracks restored entries so that pxar hardlink entries
// (FileTypeHardlink) can be reconstructed as real filesystem hard links
// instead of duplicated byte copies  -  rsync -H behavior.
//
// pxar's encoder guarantees a hardlink's target is emitted before the
// hardlink in archive order (see encoder.AddHardlink: "hardlink offset must
// point to a prior file"). The concurrent restore worker pool does not
// preserve that ordering, so resolution is deferred: a hardlink whose target
// is not restored yet is stashed and retried in a single sweep after every
// job has drained (resolveDeferredHardlinks in restore.go). If the target is
// still absent then (it lives outside the restored source set, or the
// filesystem cannot link), the caller falls back to a byte copy.
type hardlinkIndex struct {
	mu       sync.Mutex
	resolved map[string]string // archive srcPath -> dest path
	deferred []restoreJob      // hardlinks awaiting their target
}

func newHardlinkIndex() *hardlinkIndex {
	return &hardlinkIndex{resolved: make(map[string]string)}
}

// register records a successfully restored entry so later hardlinks (and the
// final sweep) can resolve it. Every restored file/dir/symlink registers,
// since a hardlink may target any of them.
func (h *hardlinkIndex) register(srcPath, dest string) {
	if srcPath == "" {
		return
	}
	h.mu.Lock()
	h.resolved[srcPath] = dest
	h.mu.Unlock()
}

// tryResolveNow returns the dest path of an already-restored target for the
// given hardlink job, if any. It probes both the absolute-from-root and the
// relative-to-self interpretations of LinkTarget, because the pxar target
// path coordinate system is not normalized across producers (Proxmox pxar
// uses absolute-from-root; other emitters may use relative paths).
func (h *hardlinkIndex) tryResolveNow(job restoreJob) (string, bool) {
	for _, c := range linkTargetCandidates(job.srcPath, job.info.LinkTarget) {
		h.mu.Lock()
		dest, ok := h.resolved[c]
		h.mu.Unlock()
		if ok {
			return dest, true
		}
	}
	return "", false
}

func (h *hardlinkIndex) deferLink(job restoreJob) {
	h.mu.Lock()
	h.deferred = append(h.deferred, job)
	h.mu.Unlock()
}

func (h *hardlinkIndex) drainDeferred() []restoreJob {
	h.mu.Lock()
	d := h.deferred
	h.deferred = nil
	h.mu.Unlock()
	return d
}

// linkTargetCandidates returns the canonical archive srcPath keys a hardlink
// LinkTarget could refer to, most-specific first.
func linkTargetCandidates(selfSrcPath, linkTarget string) []string {
	if linkTarget == "" {
		return nil
	}
	// Normalize path separators explicitly (not filepath.ToSlash, which only
	// converts the host OS separator): an archive written on Windows may carry
	// backslash paths restored on a Unix host, and vice versa.
	lt := strings.ReplaceAll(linkTarget, "\\", "/")
	lt = strings.ReplaceAll(lt, "\\", "/")
	var out []string
	if abs := path.Clean(strings.TrimPrefix(lt, "/")); abs != "" && abs != "." {
		out = append(out, abs)
	}
	if selfSrcPath != "" {
		if rel := path.Clean(path.Join(path.Dir(selfSrcPath), lt)); rel != "" && rel != "." && rel != out[finalIndex(out)] {
			out = append(out, rel)
		}
	}
	return out
}

// finalIndex returns the index of the last element without risking an
// out-of-range read on an empty slice.
func finalIndex(s []string) int {
	if len(s) == 0 {
		return -1
	}
	return len(s) - 1
}

// linkFile creates a filesystem hard link old->new. os.Link fails if the new
// path already exists (EEXIST on Unix, ERROR_EXISTS on Windows), and the dest
// is being replaced, so a stale entry is removed first. Callers fall back to
// a byte copy when os.Link itself is unsupported (e.g. FAT/exFAT, cross-device).
func linkFile(old, new string) error {
	if _, err := os.Lstat(new); err == nil {
		_ = os.Remove(new)
	}
	return os.Link(old, new)
}
