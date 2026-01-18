//go:build linux

package jobs

import (
	"encoding/json"
	"net/http"
	"os"
	"os/exec"
	"strconv"
	"strings"

	"github.com/pbs-plus/pbs-plus/internal/backend/vfs"
	"github.com/pbs-plus/pbs-plus/internal/store"
	"github.com/pbs-plus/pbs-plus/internal/store/database"
	vfssessions "github.com/pbs-plus/pbs-plus/internal/store/vfs"
	"github.com/pbs-plus/pbs-plus/internal/utils"
	"github.com/pbs-plus/pbs-plus/internal/web/controllers"
)

func D2DBackupHandler(storeInstance *store.Store) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			http.Error(w, "Invalid HTTP method", http.StatusBadRequest)
			return
		}

		allBackups, err := storeInstance.Database.GetAllBackups()
		if err != nil {
			controllers.WriteErrorResponse(w, err)
			return
		}

		for i, backup := range allBackups {
			var stats vfs.VFSStats
			switch backup.Target.Type {
			case database.TargetTypeAgent:
				session := vfssessions.GetSessionARPCFS(backup.GetStreamID())
				if session == nil {
					continue
				}

				stats = session.GetStats()
			case database.TargetTypeS3:
				session := vfssessions.GetSessionS3FS(backup.GetStreamID())
				if session == nil {
					continue
				}

				stats = session.GetStats()
			case database.TargetTypeLocal:
				continue
			default:
				continue
			}

			allBackups[i].CurrentStats.CurrentFileCount = int(stats.FilesAccessed)
			allBackups[i].CurrentStats.CurrentFolderCount = int(stats.FoldersAccessed)
			allBackups[i].CurrentStats.CurrentBytesTotal = int(stats.TotalBytes)
			allBackups[i].CurrentStats.CurrentBytesSpeed = int(stats.ByteReadSpeed)
			allBackups[i].CurrentStats.CurrentFilesSpeed = int(stats.FileAccessSpeed)
			allBackups[i].CurrentStats.StatCacheHits = int(stats.StatCacheHits)
		}

		digest, err := utils.CalculateDigest(allBackups)
		if err != nil {
			controllers.WriteErrorResponse(w, err)
			return
		}

		toReturn := BackupsResponse{
			Data:   allBackups,
			Digest: digest,
		}

		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(toReturn)
	}
}

func ExtJsBackupRunHandler(storeInstance *store.Store) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost && r.Method != http.MethodDelete {
			http.Error(w, "Invalid HTTP method", http.StatusBadRequest)
			return
		}

		var response BackupRunResponse

		// Get all backup IDs from query parameters: ?job=backup1&job=backup2
		backupIDs := r.URL.Query()["job"]
		if len(backupIDs) == 0 {
			http.Error(w, "Missing job parameter(s)", http.StatusBadRequest)
			return
		}

		decodedBackupIDs := []string{}

		for _, backupID := range backupIDs {
			decoded := utils.DecodePath(backupID)
			decodedBackupIDs = append(decodedBackupIDs, decoded)

			backup, err := storeInstance.Database.GetBackup(decoded)
			if err != nil {
				controllers.WriteErrorResponse(w, err)
				return
			}
			backup.RemoveAllRetrySchedules(r.Context())
		}

		execPath, err := os.Executable()
		if err != nil {
			controllers.WriteErrorResponse(w, err)
			return
		}

		args := []string{}
		for _, backupId := range decodedBackupIDs {
			args = append(args, "-backup-job", backupId)
		}
		args = append(args, "-web")
		if r.Method == http.MethodDelete {
			args = append(args, "-stop")
		} else if r.Method != http.MethodPost {
			http.Error(w, "Invalid HTTP method", http.StatusBadRequest)
			return
		}

		cmd := exec.Command(execPath, args...)
		cmd.Env = os.Environ()
		err = cmd.Start()
		if err != nil {
			controllers.WriteErrorResponse(w, err)
			return
		}

		w.Header().Set("Content-Type", "application/json")
		response.Status = http.StatusOK
		response.Success = true
		json.NewEncoder(w).Encode(response)
	}
}

func ExtJsBackupHandler(storeInstance *store.Store) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		response := BackupConfigResponse{}
		if r.Method != http.MethodPost {
			http.Error(w, "Invalid HTTP method", http.StatusBadRequest)
			return
		}

		w.Header().Set("Content-Type", "application/json")

		err := r.ParseForm()
		if err != nil {
			controllers.WriteErrorResponse(w, err)
			return
		}

		retry, err := strconv.Atoi(r.FormValue("retry"))
		if err != nil {
			if r.FormValue("retry") == "" {
				retry = 0
			} else {
				controllers.WriteErrorResponse(w, err)
				return
			}
		}

		retryInterval, err := strconv.Atoi(r.FormValue("retry-interval"))
		if err != nil {
			if r.FormValue("retry-interval") == "" {
				retryInterval = 1
			} else {
				controllers.WriteErrorResponse(w, err)
				return
			}
		}

		maxDirEntries, err := strconv.Atoi(r.FormValue("max-dir-entries"))
		if err != nil {
			if r.FormValue("max-dir-entries") == "" {
				maxDirEntries = 1048576
			} else {
				controllers.WriteErrorResponse(w, err)
				return
			}
		}

		includeXattr, err := strconv.ParseBool(r.FormValue("include-xattr"))
		if err != nil {
			includeXattr = true
		}

		legacyXattr, err := strconv.ParseBool(r.FormValue("legacy-xattr"))
		if err != nil {
			legacyXattr = false
		}

		newBackup := database.Backup{
			ID:               r.FormValue("id"),
			Store:            r.FormValue("store"),
			SourceMode:       r.FormValue("sourcemode"),
			ReadMode:         r.FormValue("readmode"),
			Mode:             r.FormValue("mode"),
			Target:           database.Target{Name: r.FormValue("target")},
			Subpath:          r.FormValue("subpath"),
			Schedule:         r.FormValue("schedule"),
			Comment:          r.FormValue("comment"),
			Namespace:        r.FormValue("ns"),
			MaxDirEntries:    maxDirEntries,
			NotificationMode: r.FormValue("notification-mode"),
			Retry:            retry,
			RetryInterval:    retryInterval,
			Exclusions:       []database.Exclusion{},
			PreScript:        r.FormValue("pre_script"),
			PostScript:       r.FormValue("post_script"),
			IncludeXattr:     includeXattr,
			LegacyXattr:      legacyXattr,
		}

		rawExclusions := r.FormValue("rawexclusions")
		for exclusion := range strings.SplitSeq(rawExclusions, "\n") {
			exclusion = strings.TrimSpace(exclusion)
			if exclusion == "" {
				continue
			}

			exclusionInst := database.Exclusion{
				Path:  exclusion,
				JobId: newBackup.ID,
			}

			newBackup.Exclusions = append(newBackup.Exclusions, exclusionInst)
		}

		err = storeInstance.Database.CreateBackup(nil, newBackup)
		if err != nil {
			controllers.WriteErrorResponse(w, err)
			return
		}

		response.Status = http.StatusOK
		response.Success = true
		json.NewEncoder(w).Encode(response)
	}
}

func ExtJsBackupSingleHandler(storeInstance *store.Store) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		response := BackupConfigResponse{}
		if r.Method != http.MethodPut && r.Method != http.MethodGet && r.Method != http.MethodDelete {
			http.Error(w, "Invalid HTTP method", http.StatusBadRequest)
			return
		}

		w.Header().Set("Content-Type", "application/json")

		if r.Method == http.MethodPut {
			backup, err := storeInstance.Database.GetBackup(utils.DecodePath(r.PathValue("backup")))
			if err != nil {
				controllers.WriteErrorResponse(w, err)
				return
			}

			err = r.ParseForm()
			if err != nil {
				controllers.WriteErrorResponse(w, err)
				return
			}

			if r.FormValue("store") != "" {
				backup.Store = r.FormValue("store")
			}
			if r.FormValue("mode") != "" {
				backup.Mode = r.FormValue("mode")
			}
			if r.FormValue("sourcemode") != "" {
				backup.SourceMode = r.FormValue("sourcemode")
			}
			if r.FormValue("readmode") != "" {
				backup.ReadMode = r.FormValue("readmode")
			}
			if r.FormValue("target") != "" {
				backup.Target.Name = r.FormValue("target")
			}
			if r.FormValue("schedule") != "" {
				backup.Schedule = r.FormValue("schedule")
			}
			if r.FormValue("comment") != "" {
				backup.Comment = r.FormValue("comment")
			}
			if r.FormValue("notification-mode") != "" {
				backup.NotificationMode = r.FormValue("notification-mode")
			}

			if r.FormValue("include-xattr") != "" {
				includeXattr, err := strconv.ParseBool(r.FormValue("include-xattr"))
				if err != nil {
					includeXattr = true
				}
				backup.IncludeXattr = includeXattr
			}

			if r.FormValue("legacy-xattr") != "" {
				legacyXattr, err := strconv.ParseBool(r.FormValue("legacy-xattr"))
				if err != nil {
					legacyXattr = false
				}
				backup.LegacyXattr = legacyXattr
			}

			backup.PreScript = r.FormValue("pre_script")
			backup.PostScript = r.FormValue("post_script")

			retry, err := strconv.Atoi(r.FormValue("retry"))
			if err != nil {
				retry = 0
			}

			retryInterval, err := strconv.Atoi(r.FormValue("retry-interval"))
			if err != nil {
				retryInterval = 1
			}

			maxDirEntries, err := strconv.Atoi(r.FormValue("max-dir-entries"))
			if err != nil {
				maxDirEntries = 1048576
			}

			backup.Retry = retry
			backup.RetryInterval = retryInterval
			backup.MaxDirEntries = maxDirEntries

			backup.Subpath = r.FormValue("subpath")
			backup.Namespace = r.FormValue("ns")
			backup.Exclusions = []database.Exclusion{}

			if r.FormValue("rawexclusions") != "" {
				rawExclusions := r.FormValue("rawexclusions")
				for _, exclusion := range strings.Split(rawExclusions, "\n") {
					exclusion = strings.TrimSpace(exclusion)
					if exclusion == "" {
						continue
					}

					exclusionInst := database.Exclusion{
						Path:  exclusion,
						JobId: backup.ID,
					}

					backup.Exclusions = append(backup.Exclusions, exclusionInst)
				}
			}

			if delArr, ok := r.Form["delete"]; ok {
				for _, attr := range delArr {
					switch attr {
					case "store":
						backup.Store = ""
					case "mode":
						backup.Mode = ""
					case "sourcemode":
						backup.SourceMode = ""
					case "readmode":
						backup.ReadMode = ""
					case "target":
						backup.Target.Name = ""
					case "subpath":
						backup.Subpath = ""
					case "schedule":
						backup.Schedule = ""
					case "comment":
						backup.Comment = ""
					case "ns":
						backup.Namespace = ""
					case "retry":
						backup.Retry = 0
					case "retry-interval":
						backup.RetryInterval = 1
					case "max-dir-entries":
						backup.MaxDirEntries = 1048576
					case "notification-mode":
						backup.NotificationMode = ""
					case "pre_script":
						backup.PreScript = ""
					case "post_script":
						backup.PostScript = ""
					case "rawexclusions":
						backup.Exclusions = []database.Exclusion{}
					}
				}
			}

			err = storeInstance.Database.UpdateBackup(nil, backup)
			if err != nil {
				controllers.WriteErrorResponse(w, err)
				return
			}

			response.Status = http.StatusOK
			response.Success = true
			json.NewEncoder(w).Encode(response)

			return
		}

		if r.Method == http.MethodGet {
			backup, err := storeInstance.Database.GetBackup(utils.DecodePath(r.PathValue("backup")))
			if err != nil {
				controllers.WriteErrorResponse(w, err)
				return
			}

			response.Status = http.StatusOK
			response.Success = true
			response.Data = backup
			json.NewEncoder(w).Encode(response)

			return
		}

		if r.Method == http.MethodDelete {
			err := storeInstance.Database.DeleteBackup(nil, utils.DecodePath(r.PathValue("backup")))
			if err != nil {
				controllers.WriteErrorResponse(w, err)
				return
			}

			response.Status = http.StatusOK
			response.Success = true
			json.NewEncoder(w).Encode(response)
			return
		}
	}
}
