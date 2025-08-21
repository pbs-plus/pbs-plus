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
	"github.com/pbs-plus/pbs-plus/internal/store/constants"
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

// ExtJsMountHandler mounts a pxar archive to a generated mount point.
func ExtJsMountHandler(storeInstance *store.Store) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "Invalid HTTP method", http.StatusBadRequest)
			return
		}

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
		ns := strings.TrimSpace(r.FormValue("ns"))

		if backupType == "" || backupID == "" || backupTime == "" || fileName == "" {
			http.Error(w, "missing required parameters", http.StatusBadRequest)
			return
		}
		if !strings.HasSuffix(fileName, ".mpxar.didx") && !strings.HasSuffix(fileName, ".pxar.didx") {
			http.Error(w, "file-name must end with .pxar.didx or .mpxar.didx", http.StatusBadRequest)
			return
		}

		mountPoint := filepath.Join(
			constants.RestoreMountBasePath,
			datastore,
			ns,
			fmt.Sprintf("%s-%s", backupType, backupID),
			backupTime,
		)

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

		env := append(os.Environ(),
			fmt.Sprintf("PBS_PASSWORD=%s", proxmox.GetToken()))

		if pbsStatus, err := proxmox.GetProxmoxCertInfo(); err == nil {
			env = append(env, fmt.Sprintf("PBS_FINGERPRINT=%s", pbsStatus.FingerprintSHA256))
		}

		cmd := exec.Command("/usr/bin/proxmox-backup-client", args...)
		cmd.SysProcAttr = &syscall.SysProcAttr{Setsid: true}
		cmd.Env = env

		syslog.L.Info().
			WithField("cmd", strings.Join(cmd.Args, " ")).
			WithField("jobStore", jobStore).
			WithMessage("mounting").
			Write()

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
				syslog.L.Error(lastErr).
					WithField("cmd", strings.Join(cmd.Args, " ")).
					WithField("jobStore", jobStore).
					Write()
				if lastErr == nil {
					lastErr = errors.New("mount verification failed (timeout)")
				}
				controllers.WriteErrorResponse(w, lastErr)
				return
			}
			time.Sleep(200 * time.Millisecond)
			// Recreate command for retry
			cmd = exec.Command("/usr/bin/proxmox-backup-client", args...)
			cmd.SysProcAttr = &syscall.SysProcAttr{Setsid: true}
			cmd.Env = env
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
}

func ExtJsUnmountHandler(storeInstance *store.Store) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "Invalid HTTP method", http.StatusBadRequest)
			return
		}
		if err := r.ParseForm(); err != nil {
			controllers.WriteErrorResponse(w, err)
			return
		}

		backupType := strings.TrimSpace(r.FormValue("backup-type"))
		backupID := strings.TrimSpace(r.FormValue("backup-id"))
		backupTime := strings.TrimSpace(r.FormValue("backup-time"))
		fileName := strings.TrimSpace(r.FormValue("file-name"))
		ns := strings.TrimSpace(r.FormValue("ns"))

		datastore := utils.DecodePath(r.PathValue("datastore"))

		if backupType == "" || backupID == "" || backupTime == "" || fileName == "" {
			controllers.WriteErrorResponse(w, fmt.Errorf("provide snapshot identifiers to unmount"))
			return
		}

		// Always generate mount path internally
		mountPoint := filepath.Join(
			constants.RestoreMountBasePath,
			datastore,
			ns,
			fmt.Sprintf("%s-%s", backupType, backupID),
			backupTime,
		)

		mountMu.Lock()
		// Remove from all keys
		for k, v := range mountedPaths {
			mountedPaths[k] = removeString(v, mountPoint)
			if len(mountedPaths[k]) == 0 {
				delete(mountedPaths, k)
			}
		}
		mountMu.Unlock()

		// Perform unmount
		_ = exec.Command("umount", "-f", "-l", mountPoint).Run()
		_ = exec.Command("fusermount", "-uz", mountPoint).Run()
		_ = os.RemoveAll(mountPoint)

		writeJSON(w, JobRunResponse{
			Success: true,
			Status:  http.StatusOK,
			Message: "unmounted",
		})
	}
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
