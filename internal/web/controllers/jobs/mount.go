//go:build linux

package jobs

import (
	"bufio"
	"context"
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

	"github.com/pbs-plus/pbs-plus/internal/store"
	"github.com/pbs-plus/pbs-plus/internal/store/constants"
	"github.com/pbs-plus/pbs-plus/internal/store/proxmox"
	"github.com/pbs-plus/pbs-plus/internal/utils"
	"github.com/pbs-plus/pbs-plus/internal/web/controllers"
)

var (
	mountMu      sync.Mutex
	mountedPaths = make(map[string][]string)
)

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
		mp := fields[4]
		mps = append(mps, mp)
	}
	if err := sc.Err(); err != nil {
		return nil, err
	}
	return mps, nil
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
		dsInfo, err := proxmox.GetDatastoreInfo(datastore)
		if err != nil {
			controllers.WriteErrorResponse(w, err)
			return
		}
		pbsStoreRoot := dsInfo.Path
		if datastore == "" || pbsStoreRoot == "" {
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
		if !strings.HasSuffix(fileName, ".mpxar.didx") &&
			!strings.HasSuffix(fileName, ".pxar.didx") &&
			!strings.HasSuffix(fileName, ".ppxar.didx") {
			http.Error(w, "file-name must end with .pxar.didx, .mpxar.didx, or .ppxar.didx", http.StatusBadRequest)
			return
		}

		parsedTime, err := time.Parse(time.RFC3339, backupTime)
		if err != nil {
			controllers.WriteErrorResponse(w, fmt.Errorf("invalid backup-time format: %w", err))
			return
		}
		safeTime := parsedTime.Format("2006-01-02_15-04-05")

		mountPoint := filepath.Clean(filepath.Join(
			constants.RestoreMountBasePath,
			datastore,
			ns,
			fmt.Sprintf("%s-%s", backupType, backupID),
			safeTime,
		))

		_ = exec.Command("umount", "-f", "-l", mountPoint).Run()
		_ = exec.Command("fusermount", "-uz", mountPoint).Run()
		_ = os.RemoveAll(mountPoint)
		if err := os.MkdirAll(mountPoint, 0o755); err != nil {
			controllers.WriteErrorResponse(w, fmt.Errorf("failed to create mount-point: %w", err))
			return
		}

		groupPath := filepath.Join(backupType, backupID, backupTime)
		if ns != "" {
			groupPath = filepath.Join("ns", ns, groupPath)
		}
		didxPath := filepath.Join(pbsStoreRoot, groupPath, fileName)

		args := []string{}
		switch {
		case strings.HasSuffix(fileName, ".pxar.didx"):
			args = []string{
				"--pbs-store", pbsStoreRoot,
				"--ppxar-didx", didxPath,
			}
		case strings.HasSuffix(fileName, ".mpxar.didx"):
			ppxarName := strings.TrimSuffix(fileName, ".mpxar.didx") + ".ppxar.didx"
			ppxarPath := filepath.Join(pbsStoreRoot, groupPath, ppxarName)
			if _, err := os.Stat(ppxarPath); err != nil {
				controllers.WriteErrorResponse(w, fmt.Errorf("payload index not found: %s", ppxarPath))
				return
			}
			args = []string{
				"--pbs-store", pbsStoreRoot,
				"--mpxar-didx", didxPath,
				"--ppxar-didx", ppxarPath,
			}
		case strings.HasSuffix(fileName, ".ppxar.didx"):
			mpxarName := strings.TrimSuffix(fileName, ".ppxar.didx") + ".mpxar.didx"
			mpxarPath := filepath.Join(pbsStoreRoot, groupPath, mpxarName)
			if _, err := os.Stat(mpxarPath); err != nil {
				controllers.WriteErrorResponse(w, fmt.Errorf("metadata index not found: %s", mpxarPath))
				return
			}
			args = []string{
				"--pbs-store", pbsStoreRoot,
				"--mpxar-didx", mpxarPath,
				"--ppxar-didx", didxPath,
			}
		}
		args = append(args, "--options", "ro,allow_other")
		args = append(args, mountPoint)

		cmd := exec.Command("/usr/bin/proxmox-backup-pxar-mount", args...)
		cmd.SysProcAttr = &syscall.SysProcAttr{Setsid: true}

		if err := cmd.Start(); err != nil {
			controllers.WriteErrorResponse(w, fmt.Errorf("start mount: %w", err))
			_ = os.RemoveAll(mountPoint)
			return
		}
		proc := cmd.Process

		defer func() {
			_ = proc.Signal(syscall.Signal(0))
		}()

		waitCtx, cancel := context.WithTimeout(r.Context(), 6*time.Second)
		defer cancel()

		checkMounted := func() bool {
			for i := 0; i < 10; i++ {
				if utils.IsMounted(mountPoint) {
					return true
				}
				time.Sleep(150 * time.Millisecond)
			}
			return utils.IsMounted(mountPoint)
		}

		done := make(chan error, 1)
		go func() { done <- cmd.Wait() }()

		var mountOK bool
		select {
		case <-waitCtx.Done():
			mountOK = utils.IsMounted(mountPoint)
		case err := <-done:
			if err != nil {
				mountOK = false
			} else {
				mountOK = checkMounted()
			}
		case <-time.After(500 * time.Millisecond):
			mountOK = checkMounted()
		}

		if !mountOK {
			_ = proc.Signal(syscall.SIGTERM)
			select {
			case <-done:
			case <-time.After(1 * time.Second):
				_ = proc.Kill()
				_ = cmd.Wait()
			}
			_ = exec.Command("umount", "-f", "-l", mountPoint).Run()
			_ = exec.Command("fusermount", "-uz", mountPoint).Run()
			_ = os.RemoveAll(mountPoint)
			controllers.WriteErrorResponse(w, errors.New("mount failed"))
			return
		}

		key := snapshotKey(datastore, backupType, backupID, backupTime, ns, fileName)
		mountMu.Lock()
		mountedPaths[key] = appendUnique(mountedPaths[key], filepath.Clean(mountPoint))
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

		mountPoint := filepath.Clean(filepath.Join(
			constants.RestoreMountBasePath,
			datastore,
			ns,
			fmt.Sprintf("%s-%s", backupType, backupID),
			safeTime,
		))

		mountMu.Lock()
		for k, v := range mountedPaths {
			mountedPaths[k] = removeString(v, mountPoint)
			if len(mountedPaths[k]) == 0 {
				delete(mountedPaths, k)
			}
		}
		mountMu.Unlock()

		// Unmount and remove the mount directory
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

		base := filepath.Clean(filepath.Join(
			constants.RestoreMountBasePath,
			datastore,
			ns,
		))

		allMPs, err := parseMountPoints()
		if err != nil {
			controllers.WriteErrorResponse(w, fmt.Errorf("read mounts: %w", err))
			return
		}

		var targets []string
		for _, mp := range allMPs {
			if isPathWithin(base, mp) {
				targets = append(targets, filepath.Clean(mp))
			}
		}

		// Unmount deeper paths first
		sort.Slice(targets, func(i, j int) bool {
			di := strings.Count(filepath.Clean(targets[i]), string(filepath.Separator))
			dj := strings.Count(filepath.Clean(targets[j]), string(filepath.Separator))
			if di == dj {
				return len(targets[i]) > len(targets[j])
			}
			return di > dj
		})

		// Update in-memory tracking
		mountMu.Lock()
		for k, v := range mountedPaths {
			var kept []string
			for _, p := range v {
				if !isPathWithin(base, p) {
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
			_ = os.RemoveAll(mp)
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
