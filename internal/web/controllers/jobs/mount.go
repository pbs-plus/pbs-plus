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

func unmountPath(mountPoint string) error {
	if err := exec.Command("fusermount3", "-uz", mountPoint).Run(); err == nil {
		return nil
	}

	if err := exec.Command("fusermount", "-uz", mountPoint).Run(); err == nil {
		return nil
	}

	if err := exec.Command("umount", "-f", "-l", mountPoint).Run(); err == nil {
		return nil
	}

	return fmt.Errorf("failed to unmount %s", mountPoint)
}

func buildGroupPath(ns, backupType, backupID, backupTime string) string {
	var parts []string

	if ns != "" {
		nsParts := strings.Split(ns, "/")
		for _, nsPart := range nsParts {
			if nsPart != "" {
				parts = append(parts, "ns", nsPart)
			}
		}
	}

	parts = append(parts, backupType, backupID, backupTime)

	return filepath.Join(parts...)
}

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

func generateServiceName(datastore, ns, backupType, backupID, safeTime string) string {
	parts := []string{"pbs-plus-restore", datastore}
	if ns != "" {
		parts = append(parts, strings.ReplaceAll(ns, "/", "-"))
	}
	parts = append(parts, backupType, backupID, safeTime)

	name := strings.Join(parts, "-")
	name = strings.ReplaceAll(name, " ", "-")
	name = strings.ReplaceAll(name, ":", "-")

	return name + ".service"
}

func createSystemdService(serviceName, mountPoint string, args []string) error {
	cmdArgs := []string{
		"--unit=" + serviceName,
		"--description=PBS Plus restore mount for " + mountPoint,
		"--remain-after-exit",
		"--collect",
		"--property=Type=forking",
		"--property=KillMode=control-group",
		"--property=Restart=no",
		"/usr/bin/proxmox-backup-pxar-mount",
	}
	cmdArgs = append(cmdArgs, args...)

	cmd := exec.Command("systemd-run", cmdArgs...)
	output, err := cmd.CombinedOutput()
	if err != nil {
		return fmt.Errorf("systemd-run failed: %w, output: %s", err, string(output))
	}

	return nil
}

func stopSystemdService(serviceName string) error {
	stopCmd := exec.Command("systemctl", "stop", serviceName)
	_ = stopCmd.Run()

	resetCmd := exec.Command("systemctl", "reset-failed", serviceName)
	_ = resetCmd.Run()

	return nil
}

func listRestoreServices() ([]string, error) {
	cmd := exec.Command("systemctl", "list-units", "--all", "--no-legend", "--plain", "pbs-plus-restore-*")
	output, err := cmd.Output()
	if err != nil {
		return []string{}, nil
	}

	var services []string
	scanner := bufio.NewScanner(strings.NewReader(string(output)))
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line == "" {
			continue
		}
		fields := strings.Fields(line)
		if len(fields) > 0 && strings.HasPrefix(fields[0], "pbs-plus-restore-") {
			services = append(services, fields[0])
		}
	}

	return services, nil
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

		serviceName := generateServiceName(datastore, ns, backupType, backupID, safeTime)

		_ = stopSystemdService(serviceName)
		if utils.IsMounted(mountPoint) {
			_ = unmountPath(mountPoint)
		}
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

		if err := createSystemdService(serviceName, mountPoint, args); err != nil {
			controllers.WriteErrorResponse(w, fmt.Errorf("start mount service: %w", err))
			_ = os.RemoveAll(mountPoint)
			return
		}

		mountOK := false
		for i := 0; i < 30; i++ {
			if utils.IsMounted(mountPoint) {
				mountOK = true
				break
			}
			time.Sleep(200 * time.Millisecond)
		}

		if !mountOK {
			_ = stopSystemdService(serviceName)
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

		serviceName := generateServiceName(datastore, ns, backupType, backupID, safeTime)

		_ = stopSystemdService(serviceName)

		if utils.IsMounted(mountPoint) {
			_ = unmountPath(mountPoint)
		}

		_ = os.RemoveAll(mountPoint)

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

		services, err := listRestoreServices()
		if err != nil {
			controllers.WriteErrorResponse(w, fmt.Errorf("list services: %w", err))
			return
		}

		prefix := "pbs-plus-restore-" + datastore
		if ns != "" {
			prefix += "-" + strings.ReplaceAll(ns, "/", "-")
		}

		for _, svc := range services {
			if strings.HasPrefix(svc, prefix) {
				_ = stopSystemdService(svc)
			}
		}

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

		for _, mp := range targets {
			if utils.IsMounted(mp) {
				_ = unmountPath(mp)
			}
		}

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
