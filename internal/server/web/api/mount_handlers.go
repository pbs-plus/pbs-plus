//go:build linux

package api

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

	"github.com/pbs-plus/pbs-plus/internal/conf"
	"github.com/pbs-plus/pbs-plus/internal/proxmox"
	"github.com/pbs-plus/pbs-plus/internal/proxmox/cli"
	backend "github.com/pbs-plus/pbs-plus/internal/server"
	"github.com/pbs-plus/pbs-plus/internal/server/store"

	"github.com/pbs-plus/pbs-plus/internal/log"
	"github.com/pbs-plus/pbs-plus/internal/validate"
)

func parseMountPoints() ([]string, error) {
	f, err := os.Open("/proc/self/mountinfo")
	if err != nil {
		return nil, err
	}
	defer func() {
		if err := f.Close(); err != nil {
			log.Error(err, "")
		}
	}()

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

func removeEmptyDirsToBase(path, basePath string) {
	path = filepath.Clean(path)
	basePath = filepath.Clean(basePath)

	for {
		if path == basePath || path == "/" || !validate.IsPathWithin(basePath, path) {
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

func ExtJsMountHandler(storeInstance *store.Store) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "Invalid HTTP method", http.StatusBadRequest)
			return
		}

		if err := r.ParseForm(); err != nil {
			WriteErrorResponse(w, err)
			return
		}

		datastore := validate.DecodePath(r.PathValue("datastore"))
		if err := validate.ValidateDatastore(datastore); err != nil {
			WriteErrorResponse(w, fmt.Errorf("invalid datastore: %w", err))
			return
		}

		dsInfo, err := cli.GetDatastoreInfo(datastore)
		if err != nil {
			WriteErrorResponse(w, err)
			return
		}
		pbsStoreRoot := dsInfo.Path
		if pbsStoreRoot == "" {
			WriteErrorResponse(w, fmt.Errorf("invalid datastore configuration"))
			return
		}

		backupType := strings.TrimSpace(r.FormValue("backup-type"))
		backupID := strings.TrimSpace(r.FormValue("backup-id"))
		backupTime := strings.TrimSpace(r.FormValue("backup-time"))
		fileName := strings.TrimSpace(r.FormValue("file-name"))
		ns := strings.TrimSpace(r.FormValue("ns"))

		if err := validate.ValidateBackupType(backupType); err != nil {
			WriteErrorResponse(w, err)
			return
		}
		if err := validate.ValidateBackupID(backupID); err != nil {
			WriteErrorResponse(w, err)
			return
		}
		if err := validate.ValidateFileName(fileName); err != nil {
			WriteErrorResponse(w, err)
			return
		}
		if err := validate.ValidateNamespace(ns); err != nil {
			WriteErrorResponse(w, err)
			return
		}

		parsedTime, err := time.Parse(time.RFC3339, backupTime)
		if err != nil {
			WriteErrorResponse(w, fmt.Errorf("invalid backup-time format: %w", err))
			return
		}
		safeTime := parsedTime.Format("2006-01-02_15-04-05")

		mountPoint := filepath.Clean(filepath.Join(
			conf.RestoreMountBasePath,
			datastore,
			ns,
			fmt.Sprintf("%s-%s", backupType, backupID),
			safeTime,
		))

		if err := validate.SanitizeMountPoint(mountPoint, conf.RestoreMountBasePath); err != nil {
			WriteErrorResponse(w, err)
			return
		}

		serviceName := backend.GenerateMountServiceName(datastore, ns, backupType, backupID, safeTime)

		if err := backend.StopMountService(r.Context(), serviceName); err != nil {
			log.Error(err, "")
		}
		if IsMounted(mountPoint) {
			if err := unmountPath(mountPoint); err != nil {
				log.Error(err, "")
			}
		}
		if err := os.RemoveAll(mountPoint); err != nil && !os.IsNotExist(err) {
			log.Error(err, "")
		}

		if err := os.MkdirAll(mountPoint, 0o755); err != nil {
			WriteErrorResponse(w, fmt.Errorf("failed to create mount-point: %w", err))
			return
		}

		mpxarPath, ppxarPath, isMetadataSplit, err := proxmox.BuildPxarPaths(pbsStoreRoot, ns, backupType, backupID, backupTime, fileName)
		if err != nil {
			WriteErrorResponse(w, err)
			return
		}

		args := []string{"--pbs-store", pbsStoreRoot}
		if isMetadataSplit {
			args = append(args, "--mpxar-didx", mpxarPath, "--ppxar-didx", ppxarPath)
		} else {
			args = append(args, "--ppxar-didx", ppxarPath)
		}
		args = append(args, mountPoint)

		if err := backend.CreateMountService(r.Context(), serviceName, mountPoint, args); err != nil {
			WriteErrorResponse(w, fmt.Errorf("start mount service: %w", err))
			if err := os.RemoveAll(mountPoint); err != nil && !os.IsNotExist(err) {
				log.Error(err, "")
			}
			return
		}

		mountOK := false
		for range 30 {
			if IsMounted(mountPoint) {
				mountOK = true
				break
			}
			time.Sleep(200 * time.Millisecond)
		}

		if !mountOK {
			if err := backend.StopMountService(r.Context(), serviceName); err != nil {
				log.Error(err, "")
			}
			if err := os.RemoveAll(mountPoint); err != nil && !os.IsNotExist(err) {
				log.Error(err, "")
			}
			WriteErrorResponse(w, errors.New("mount failed"))
			return
		}

		writeJSON(w, BackupRunResponse{
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
			WriteErrorResponse(w, err)
			return
		}

		datastore := validate.DecodePath(r.PathValue("datastore"))
		backupType := strings.TrimSpace(r.FormValue("backup-type"))
		backupID := strings.TrimSpace(r.FormValue("backup-id"))
		backupTime := strings.TrimSpace(r.FormValue("backup-time"))
		fileName := strings.TrimSpace(r.FormValue("file-name"))
		ns := strings.TrimSpace(r.FormValue("ns"))

		if err := validate.ValidateDatastore(datastore); err != nil {
			WriteErrorResponse(w, fmt.Errorf("invalid datastore: %w", err))
			return
		}
		if err := validate.ValidateBackupType(backupType); err != nil {
			WriteErrorResponse(w, err)
			return
		}
		if err := validate.ValidateBackupID(backupID); err != nil {
			WriteErrorResponse(w, err)
			return
		}
		if err := validate.ValidateFileName(fileName); err != nil {
			WriteErrorResponse(w, err)
			return
		}
		if err := validate.ValidateNamespace(ns); err != nil {
			WriteErrorResponse(w, err)
			return
		}

		parsedTime, err := time.Parse(time.RFC3339, backupTime)
		if err != nil {
			WriteErrorResponse(w, fmt.Errorf("invalid backup-time format: %w", err))
			return
		}
		safeTime := parsedTime.Format("2006-01-02_15-04-05")

		mountPoint := filepath.Clean(filepath.Join(
			conf.RestoreMountBasePath,
			datastore,
			ns,
			fmt.Sprintf("%s-%s", backupType, backupID),
			safeTime,
		))

		if err := validate.SanitizeMountPoint(mountPoint, conf.RestoreMountBasePath); err != nil {
			WriteErrorResponse(w, err)
			return
		}

		basePath := filepath.Clean(filepath.Join(
			conf.RestoreMountBasePath,
			datastore,
		))

		serviceName := backend.GenerateMountServiceName(datastore, ns, backupType, backupID, safeTime)

		if err := backend.StopMountService(r.Context(), serviceName); err != nil {
			log.Error(err, "")
		}

		if IsMounted(mountPoint) {
			if err := unmountPath(mountPoint); err != nil {
				log.Error(err, "")
			}
		}

		if err := os.RemoveAll(mountPoint); err != nil && !os.IsNotExist(err) {
			log.Error(err, "")
		}

		removeEmptyDirsToBase(filepath.Dir(mountPoint), basePath)

		writeJSON(w, BackupRunResponse{
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
			WriteErrorResponse(w, err)
			return
		}

		datastore := validate.DecodePath(r.PathValue("datastore"))
		ns := strings.TrimSpace(r.FormValue("ns"))

		if err := validate.ValidateDatastore(datastore); err != nil {
			WriteErrorResponse(w, fmt.Errorf("invalid datastore: %w", err))
			return
		}
		if err := validate.ValidateNamespace(ns); err != nil {
			WriteErrorResponse(w, err)
			return
		}

		base := filepath.Clean(filepath.Join(
			conf.RestoreMountBasePath,
			datastore,
			ns,
		))

		if err := validate.SanitizeMountPoint(base, conf.RestoreMountBasePath); err != nil {
			WriteErrorResponse(w, err)
			return
		}

		services, err := backend.ListMountServices(r.Context())
		if err != nil {
			WriteErrorResponse(w, fmt.Errorf("list services: %w", err))
			return
		}

		prefix := "pbs-plus-restore-" + datastore
		if ns != "" {
			prefix += "-" + strings.ReplaceAll(ns, "/", "-")
		}

		for _, svc := range services {
			if strings.HasPrefix(svc, prefix) {
				if err := backend.StopMountService(r.Context(), svc); err != nil {
					log.Error(err, "")
				}
			}
		}

		allMPs, err := parseMountPoints()
		if err != nil {
			WriteErrorResponse(w, fmt.Errorf("read mounts: %w", err))
			return
		}

		var targets []string
		for _, mp := range allMPs {
			clean := filepath.Clean(mp)
			if clean == base || validate.IsPathWithin(base, clean) {
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
			if IsMounted(mp) {
				if err := unmountPath(mp); err != nil {
					log.Error(err, "")
				}
			}
		}

		if err := os.RemoveAll(base); err != nil && !os.IsNotExist(err) {
			log.Error(err, "")
		}

		writeJSON(w, BackupRunResponse{
			Success: true,
			Status:  http.StatusOK,
			Message: "unmounted all within datastore",
		})
	}
}

func writeJSON(w http.ResponseWriter, v any) {
	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(v); err != nil {
		log.Error(err, "")
	}
}
