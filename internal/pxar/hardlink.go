package pxar

import (
	"os"
	"path"
	"strings"
	"sync"
)

// hardlinkIndex tracks restored entries so pxar hardlink entries
// (FileTypeHardlink) can be reconstructed as real filesystem hard links
// instead of duplicated byte copies. The concurrent worker pool does not
// preserve archive ordering, so resolution is deferred to a sweep after all
// jobs drain (resolveDeferredHardlinks). If the target is still absent then
// (outside the restored set or on a non-linking FS), the caller falls back
// to a byte copy.
type hardlinkIndex struct {
	mu       sync.Mutex
	resolved map[string]string
	deferred []restoreJob
}

func newHardlinkIndex() *hardlinkIndex {
	return &hardlinkIndex{resolved: make(map[string]string)}
}

func (h *hardlinkIndex) register(srcPath, dest string) {
	if srcPath == "" {
		return
	}
	h.mu.Lock()
	h.resolved[srcPath] = dest
	h.mu.Unlock()
}

// tryResolveNow returns the dest path of an already-restored target for the
// given hardlink job. It probes both absolute-from-root and relative-to-self
// interpretations of LinkTarget because the pxar target path coordinate system
// is not normalized across producers.
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

func finalIndex(s []string) int {
	if len(s) == 0 {
		return -1
	}
	return len(s) - 1
}

// linkFile creates a filesystem hard link. If new already exists it is removed
// first. Callers fall back to a byte copy when os.Link is unsupported.
func linkFile(old, new string) error {
	if _, err := os.Lstat(new); err == nil {
		_ = os.Remove(new)
	}
	return os.Link(old, new)
}
