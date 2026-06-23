package pxar

import (
	"os"
	"path"
	"strings"
	"sync"

	"github.com/pbs-plus/pbs-plus/internal/syslog"
)

// hardlinkIndex tracks restored entries so pxar hardlink entries
// instead of duplicated byte copies. The concurrent worker pool does not
// jobs drain (resolveDeferredHardlinks). If the target is still absent then
// (outside the restored set or on a non-linking FS), the caller falls back
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

func linkFile(old, new string) error {
	if _, err := os.Lstat(new); err == nil {
		if err := os.Remove(new); err != nil && !os.IsNotExist(err) {
			syslog.L.Error(err).Write()
		}
	}
	return os.Link(old, new)
}
