package jobs

import (
	"bufio"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
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

func isPathWithin(base, p string) bool {
	base = filepath.Clean(base)
	p = filepath.Clean(p)
	if base == p {
		return true
	}
	rel, err := filepath.Rel(base, p)
	if err != nil {
		return false
	}
	return rel != ".." && !strings.HasPrefix(rel, ".."+string(filepath.Separator))
}

func parseMountPoints() ([]string, error) {
	f, err := os.Open("/proc/self/mountinfo")
	if err != nil {
		return nil, err
	}
	defer f.Close()

	var mps []string
	sc := bufio.NewScanner(f)
	for sc.Scan() {
		// mountinfo format (fields):
		// 1: mount ID
		// 2: parent ID
		// 3: major:minor
		// 4: root
		// 5: mount point
		// 6: mount options
		// 7: optional fields... " - "
		// 8+: fstype, source, super options
		line := sc.Text()
		parts := strings.Split(line, " - ")
		if len(parts) != 2 {
			continue
		}
		fields := strings.Fields(parts[0])
		if len(fields) < 5 {
			continue
		}
		mp := fields[4] // mount point field
		mps = append(mps, mp)
	}
	if err := sc.Err(); err != nil {
		return nil, err
	}
	return mps, nil
}

func isMounted(mp string) (bool, error) {
	mps, err := parseMountPoints()
	if err != nil {
		return false, err
	}
	mp = filepath.Clean(mp)
	for _, m := range mps {
		if filepath.Clean(m) == mp {
			return true, nil
		}
	}
	return false, nil
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

		parsedTime, err := time.Parse(time.RFC3339, backupTime)
		if err != nil {
			controllers.WriteErrorResponse(w, fmt.Errorf("invalid backup-time format: %w", err))
			return
		}
		safeTime := parsedTime.Format("2006-01-02_15-04-05")

		mountPoint := filepath.Join(
			constants.RestoreMountBasePath,
			datastore,
			ns,
			fmt.Sprintf("%s-%s", backupType, backupID),
			safeTime,
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

		parsedTime, err := time.Parse(time.RFC3339, backupTime)
		if err != nil {
			controllers.WriteErrorResponse(w, fmt.Errorf("invalid backup-time format: %w", err))
			return
		}
		safeTime := parsedTime.Format("2006-01-02_15-04-05")

		mountPoint := filepath.Join(
			constants.RestoreMountBasePath,
			datastore,
			ns,
			fmt.Sprintf("%s-%s", backupType, backupID),
			safeTime,
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

func ExtJsUnmountAllHandler(storeInstance *store.Store) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "Invalid HTTP method", http.StatusBadRequest)
			return
		}
		if err := r.ParseForm(); err != nil {
			controllers.WriteErrorResponse(w, err)
			return
		}

		ns := strings.TrimSpace(r.FormValue("ns"))
		datastore := utils.DecodePath(r.PathValue("datastore"))

		mountPoint := filepath.Join(
			constants.RestoreMountBasePath,
			datastore,
			ns,
		)

		allMPs, err := parseMountPoints()
		if err != nil {
			controllers.WriteErrorResponse(w, fmt.Errorf("read mounts: %w", err))
			return
		}

		var targets []string
		for _, mp := range allMPs {
			if isPathWithin(mountPoint, mp) {
				targets = append(targets, mp)
			}
		}

		sort.Slice(targets, func(i, j int) bool {
			di := strings.Count(filepath.Clean(targets[i]), string(filepath.Separator))
			dj := strings.Count(filepath.Clean(targets[j]), string(filepath.Separator))
			if di == dj {
				return len(targets[i]) > len(targets[j])
			}
			return di > dj
		})

		mountMu.Lock()
		for k, v := range mountedPaths {
			var kept []string
			for _, p := range v {
				if !isPathWithin(mountPoint, p) {
					kept = append(kept, p)
				}
			}
			if len(kept) > 0 {
				mountedPaths[k] = kept
			} else {
				delete(mountedPaths, k)
			}
		}
		mountMu.Unlock()

		for _, mp := range targets {
			_ = exec.Command("umount", "-f", "-l", mp).Run()
			_ = exec.Command("fusermount", "-uz", mp).Run()
			_ = os.RemoveAll(mountPoint)
		}

		writeJSON(w, JobRunResponse{
			Success: true,
			Status:  http.StatusOK,
			Message: "unmounted all within datastore",
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
