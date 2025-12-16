//go:build linux

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
	"syscall"
	"time"

	"github.com/pbs-plus/pbs-plus/internal/store"
	"github.com/pbs-plus/pbs-plus/internal/store/constants"
	"github.com/pbs-plus/pbs-plus/internal/store/proxmox"
	"github.com/pbs-plus/pbs-plus/internal/utils"
	"github.com/pbs-plus/pbs-plus/internal/web/controllers"
)

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
		parts := strings.Split(sc.Text(), " - ")
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

// buildGroupPath constructs the proper path for backup groups with nested namespaces
func buildGroupPath(ns, backupType, backupID, backupTime string) string {
	var parts []string

	// Handle nested namespaces: split by '/' and prefix each with 'ns/'
	if ns != "" {
		nsParts := strings.Split(ns, "/")
		for _, nsPart := range nsParts {
			if nsPart != "" {
				parts = append(parts, "ns", nsPart)
			}
		}
	}

	// Add backup type (prefixed with appropriate directory)
	parts = append(parts, backupType, backupID, backupTime)

	return filepath.Join(parts...)
}

// removeEmptyDirsToBase removes empty directories recursively up to basePath
func removeEmptyDirsToBase(path, basePath string) {
	path = filepath.Clean(path)
	basePath = filepath.Clean(basePath)

	for {
		if path == basePath || path == "/" || !isPathWithin(basePath, path) {
			break
		}

		entries, err := os.ReadDir(path)
		if err != nil || len(entries) > 0 {
			break
		}

		if err := os.Remove(path); err != nil {
			break
		}

		path = filepath.Dir(path)
	}
}

// unmountPath attempts to unmount a single path using multiple methods
func unmountPath(mountPoint string) error {
	// Try fusermount3 first (modern FUSE)
	if err := exec.Command("fusermount3", "-uz", mountPoint).Run(); err == nil {
		return nil
	}

	// Try fusermount (legacy FUSE)
	if err := exec.Command("fusermount", "-uz", mountPoint).Run(); err == nil {
		return nil
	}

	// Try umount with force and lazy flags
	if err := exec.Command("umount", "-f", "-l", mountPoint).Run(); err == nil {
		return nil
	}

	return fmt.Errorf("failed to unmount %s", mountPoint)
}

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

		// Clean up any existing mount
		_ = unmountPath(mountPoint)
		_ = os.RemoveAll(mountPoint)

		if err := os.MkdirAll(mountPoint, 0o755); err != nil {
			controllers.WriteErrorResponse(w, fmt.Errorf("failed to create mount-point: %w", err))
			return
		}

		groupPath := buildGroupPath(ns, backupType, backupID, backupTime)
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
		args = append(args, mountPoint)

		cmd := exec.Command("/usr/bin/proxmox-backup-pxar-mount", args...)
		// Detach from current process - run independently
		cmd.SysProcAttr = &syscall.SysProcAttr{
			Setsid:  true,
			Setpgid: true,
		}
		// Detach standard streams
		cmd.Stdin = nil
		cmd.Stdout = nil
		cmd.Stderr = nil

		if err := cmd.Start(); err != nil {
			controllers.WriteErrorResponse(w, fmt.Errorf("start mount: %w", err))
			_ = os.RemoveAll(mountPoint)
			return
		}

		// Detach from the process - let it run independently
		_ = cmd.Process.Release()

		// Wait for mount to become active by checking filesystem
		mountOK := false
		for i := 0; i < 20; i++ {
			if utils.IsMounted(mountPoint) {
				mountOK = true
				break
			}
			time.Sleep(150 * time.Millisecond)
		}

		if !mountOK {
			_ = unmountPath(mountPoint)
			_ = os.RemoveAll(mountPoint)
			controllers.WriteErrorResponse(w, errors.New("mount failed"))
			return
		}

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

		basePath := filepath.Clean(filepath.Join(
			constants.RestoreMountBasePath,
			datastore,
		))

		// Unmount the path
		_ = unmountPath(mountPoint)

		// Remove the mount point directory
		_ = os.RemoveAll(mountPoint)

		// Clean up empty parent directories recursively
		removeEmptyDirsToBase(filepath.Dir(mountPoint), basePath)

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
			clean := filepath.Clean(mp)
			if clean == base || isPathWithin(base, clean) {
				targets = append(targets, clean)
			}
		}

		sort.Slice(targets, func(i, j int) bool {
			di := strings.Count(targets[i], string(filepath.Separator))
			dj := strings.Count(targets[j], string(filepath.Separator))
			if di == dj {
				return len(targets[i]) > len(targets[j])
			}
			return di > dj
		})

		// Unmount all targets
		for _, mp := range targets {
			_ = unmountPath(mp)
		}

		// Remove all directories recursively under base
		_ = os.RemoveAll(base)

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
