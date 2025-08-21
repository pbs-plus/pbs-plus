package jobs

import (
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/pbs-plus/pbs-plus/internal/proxy/controllers"
	"github.com/pbs-plus/pbs-plus/internal/store"
	"github.com/pbs-plus/pbs-plus/internal/store/proxmox"
	"github.com/pbs-plus/pbs-plus/internal/syslog"
	"github.com/pbs-plus/pbs-plus/internal/utils"
)

var (
	mountMu      sync.Mutex
	mountedPaths = make(map[string][]string)
)

func getMountedPathsCopy() map[string][]string {
	mountMu.Lock()
	defer mountMu.Unlock()
	cp := make(map[string][]string, len(mountedPaths))
	for k, v := range mountedPaths {
		c := make([]string, len(v))
		copy(c, v)
		cp[k] = c
	}
	return cp
}

func snapshotKey(datastore, backupType, backupID, backupTime, ns, fileName string) string {
	parts := []string{datastore, backupType, backupID, backupTime}
	if ns != "" {
		parts = append(parts, ns)
	}
	if fileName != "" {
		parts = append(parts, fileName)
	}
	return strings.Join(parts, "|")
}

// ExtJsMountHandler mounts/unmounts a pxar archive to a mount point.
// POST mounts; DELETE unmounts. Mirrors the ExtJS client handler.
func ExtJsMountHandler(storeInstance *store.Store) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		switch r.Method {
		case http.MethodPost:
			handleMount(w, r)
		case http.MethodDelete:
			handleUnmount(w, r)
		default:
			http.Error(w, "Invalid HTTP method", http.StatusBadRequest)
		}
	}
}

func handleMount(w http.ResponseWriter, r *http.Request) {
	if err := r.ParseForm(); err != nil {
		controllers.WriteErrorResponse(w, err)
		return
	}

	datastore := utils.DecodePath(r.PathValue("datastore"))
	jobStore := fmt.Sprintf("%s@localhost:%s", proxmox.AUTH_ID, datastore)
	if jobStore == "@localhost:" {
		controllers.WriteErrorResponse(w, fmt.Errorf("invalid datastore configuration"))
		return
	}

	backupType := strings.TrimSpace(r.FormValue("backup-type"))
	backupID := strings.TrimSpace(r.FormValue("backup-id"))
	backupTime := strings.TrimSpace(r.FormValue("backup-time"))
	fileName := strings.TrimSpace(r.FormValue("file-name"))
	mountPoint := strings.TrimSpace(r.FormValue("mount-point"))
	ns := strings.TrimSpace(r.FormValue("ns"))

	if backupType == "" || backupID == "" || backupTime == "" || fileName == "" || mountPoint == "" {
		http.Error(w, "missing required parameters", http.StatusBadRequest)
		return
	}
	if !strings.HasSuffix(fileName, ".mpxar.didx") && !strings.HasSuffix(fileName, ".pxar.didx") {
		http.Error(w, "file-name must end with .pxar.didx or .mpxar.didx", http.StatusBadRequest)
		return
	}
	if !filepath.IsAbs(mountPoint) {
		http.Error(w, "mount-point must be an absolute path", http.StatusBadRequest)
		return
	}

	// Prepare mount directory: lazy unmount, clean, recreate
	_ = exec.Command("umount", "-f", "-l", mountPoint).Run()
	_ = exec.Command("fusermount", "-uz", mountPoint).Run()
	_ = os.RemoveAll(mountPoint)
	if err := os.MkdirAll(mountPoint, 0o755); err != nil {
		controllers.WriteErrorResponse(w, fmt.Errorf("failed to create mount-point: %w", err))
		return
	}

	// proxmox-backup-client mount [--ns NS] <groupPath> <archive> <target>
	archive := strings.TrimSuffix(fileName, ".didx")
	groupPath := fmt.Sprintf("%s/%s/%s", backupType, backupID, backupTime)

	args := []string{
		"mount",
		"--repository", jobStore,
	}
	if ns != "" {
		args = append(args, "--ns", ns)
	}
	args = append(args, groupPath, archive, mountPoint)

	cmd := exec.Command("/usr/bin/proxmox-backup-client", args...)
	cmd.SysProcAttr = &syscall.SysProcAttr{Setsid: true}
	cmd.Env = os.Environ()

	syslog.L.Info().WithField("cmd", strings.Join(cmd.Args, " ")).WithField("jobStore", jobStore).WithMessage("mounting").Write()

	// Retry loop until mounted or timeout
	deadline := time.Now().Add(5 * time.Second)
	var lastErr error
	for {
		if err := cmd.Run(); err != nil {
			lastErr = err
		}
		if utils.IsMounted(mountPoint) {
			break
		}
		if time.Now().After(deadline) {
			_ = os.RemoveAll(mountPoint)
			if lastErr == nil {
				lastErr = errors.New("mount verification failed (timeout)")
			}
			syslog.L.Error(lastErr).WithField("cmd", strings.Join(cmd.Args, " ")).WithField("jobStore", jobStore).Write()
			controllers.WriteErrorResponse(w, lastErr)
			return
		}
		time.Sleep(200 * time.Millisecond)
		// Recreate command for retry
		cmd = exec.Command("/usr/bin/proxmox-backup-client", args...)
		cmd.SysProcAttr = &syscall.SysProcAttr{Setsid: true}
		cmd.Env = os.Environ()
	}

	// Track mount in memory
	key := snapshotKey(datastore, backupType, backupID, backupTime, ns, fileName)
	mountMu.Lock()
	mountedPaths[key] = appendUnique(mountedPaths[key], mountPoint)
	mountMu.Unlock()

	writeJSON(w, JobRunResponse{
		Success: true,
		Status:  http.StatusOK,
		Message: "mounted",
	})
}

func handleUnmount(w http.ResponseWriter, r *http.Request) {
	if err := r.ParseForm(); err != nil {
		controllers.WriteErrorResponse(w, err)
		return
	}
	mountPoint := strings.TrimSpace(r.FormValue("mount-point"))
	backupType := strings.TrimSpace(r.FormValue("backup-type"))
	backupID := strings.TrimSpace(r.FormValue("backup-id"))
	backupTime := strings.TrimSpace(r.FormValue("backup-time"))
	fileName := strings.TrimSpace(r.FormValue("file-name"))
	ns := strings.TrimSpace(r.FormValue("ns"))

	datastore := utils.DecodePath(r.PathValue("datastore"))

	if mountPoint == "" && (backupType == "" || backupID == "" || backupTime == "" || fileName == "") {
		http.Error(w, "provide mount-point or snapshot identifiers to unmount", http.StatusBadRequest)
		return
	}

	targets := []string{}

	mountMu.Lock()
	if mountPoint != "" {
		targets = append(targets, mountPoint)
		// Remove from all keys
		for k, v := range mountedPaths {
			mountedPaths[k] = removeString(v, mountPoint)
			if len(mountedPaths[k]) == 0 {
				delete(mountedPaths, k)
			}
		}
	} else {
		key := snapshotKey(datastore, backupType, backupID, backupTime, ns, fileName)
		if paths, ok := mountedPaths[key]; ok {
			targets = append(targets, paths...)
			delete(mountedPaths, key)
		}
	}
	mountMu.Unlock()

	if len(targets) == 0 {
		writeJSON(w, JobRunResponse{
			Success: true,
			Status:  http.StatusOK,
			Message: "nothing to unmount",
		})
		return
	}

	for _, mp := range targets {
		_ = exec.Command("umount", "-f", "-l", mp).Run()
		_ = exec.Command("fusermount", "-uz", mp).Run()
		_ = os.RemoveAll(mp)
	}

	writeJSON(w, JobRunResponse{
		Success: true,
		Status:  http.StatusOK,
		Message: "unmounted",
	})
}

func writeJSON(w http.ResponseWriter, v any) {
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(v)
}

func appendUnique(slice []string, item string) []string {
	for _, s := range slice {
		if s == item {
			return slice
		}
	}
	return append(slice, item)
}

func removeString(slice []string, item string) []string {
	out := make([]string, 0, len(slice))
	for _, s := range slice {
		if s != item {
			out = append(out, s)
		}
	}
	return out
}
