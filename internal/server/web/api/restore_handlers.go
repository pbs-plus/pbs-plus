//go:build linux

package api

import (
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"net/rpc"
	"strconv"
	"time"

	"github.com/pbs-plus/pbs-plus/internal/conf"
	"github.com/pbs-plus/pbs-plus/internal/pxar"
	"github.com/pbs-plus/pbs-plus/internal/server/database"
	jobrpc "github.com/pbs-plus/pbs-plus/internal/server/rpc"
	"github.com/pbs-plus/pbs-plus/internal/server/store"
	"github.com/pbs-plus/pbs-plus/internal/server/vfs/sessions"
	"github.com/pbs-plus/pbs-plus/internal/syslog"
	"github.com/pbs-plus/pbs-plus/internal/validate"
)

func D2DRestoreHandler(storeInstance *store.Store) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			http.Error(w, "Invalid HTTP method", http.StatusBadRequest)
			return
		}

		allRestores, err := storeInstance.RestoreSvc.GetAllRestores()
		if err != nil {
			WriteErrorResponse(w, err)
			return
		}

		for i, restore := range allRestores {
			var stats pxar.PxarReaderStats
			session := sessions.GetSessionPxarReader(restore.GetStreamID())
			if session == nil {
				continue
			}

			stats = session.GetStats()

			allRestores[i].CurrentStats.CurrentFileCount = int(stats.FilesAccessed)
			allRestores[i].CurrentStats.CurrentFolderCount = int(stats.FoldersAccessed)
			allRestores[i].CurrentStats.CurrentBytesTotal = int(stats.TotalBytes)
			allRestores[i].CurrentStats.CurrentBytesSpeed = int(stats.ByteReadSpeed)
			allRestores[i].CurrentStats.CurrentFilesSpeed = int(stats.FileAccessSpeed)
			allRestores[i].CurrentStats.StatCacheHits = int(stats.StatCacheHits)
		}

		flatRestores := FlattenRestores(allRestores)

		digest, err := calculateDigest(flatRestores)
		if err != nil {
			WriteErrorResponse(w, err)
			return
		}

		toReturn := map[string]any{
			"data":    flatRestores,
			"digest":  digest,
			"success": true,
		}

		w.Header().Set("Content-Type", "application/json")
		if err := json.NewEncoder(w).Encode(toReturn); err != nil {
			syslog.L.Error(err).Write()
		}
	}
}

func ExtJsRestoreRunHandler(storeInstance *store.Store) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost && r.Method != http.MethodDelete {
			http.Error(w, "Invalid HTTP method", http.StatusBadRequest)
			return
		}

		var response RestoreRunResponse

		restoreIDs := r.URL.Query()["job"]
		if len(restoreIDs) == 0 {
			http.Error(w, "Missing restore parameter(s)", http.StatusBadRequest)
			return
		}

		decodedRestoreIDs := []string{}
		for _, restoreID := range restoreIDs {
			decoded := validate.DecodePath(restoreID)
			if err := validate.ValidateJobId(decoded); err != nil {
				WriteErrorResponse(w, err)
				return
			}
			decodedRestoreIDs = append(decodedRestoreIDs, decoded)
		}

		stop := r.Method == http.MethodDelete

		go func() {
			conn, err := net.DialTimeout("unix", conf.JobMutateSocketPath, 5*time.Minute)
			if err != nil {
				syslog.L.Error(err).WithField("restores", decodedRestoreIDs).Write()
				return
			}
			rpcClient := rpc.NewClient(conn)
			defer func() {
				if err := rpcClient.Close(); err != nil {
					syslog.L.Error(err).Write()
				}
			}()

			for _, restoreID := range decodedRestoreIDs {
				restoreTask, err := storeInstance.Database.GetRestore(restoreID)
				if err != nil {
					syslog.L.Error(err).WithField("restoreID", restoreID).Write()
					continue
				}

				args := &jobrpc.RestoreQueueArgs{
					Job:       restoreTask,
					SkipCheck: true,
					Stop:      stop,
					Web:       true,
				}
				var reply jobrpc.QueueReply
				if err := rpcClient.Call("JobRPCService.RestoreQueue", args, &reply); err != nil {
					syslog.L.Error(err).WithField("restoreID", restoreID).Write()
					continue
				}
				if reply.Status != 200 {
					syslog.L.Error(fmt.Errorf("%s", reply.Message)).WithField("restoreID", restoreID).Write()
				}
			}
		}()

		w.Header().Set("Content-Type", "application/json")
		response.Status = http.StatusOK
		response.Success = true
		if err := json.NewEncoder(w).Encode(response); err != nil {
			syslog.L.Error(err).Write()
		}
	}
}

func ExtJsRestoreHandler(storeInstance *store.Store) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		response := RestoreConfigResponse{}
		if r.Method != http.MethodPost {
			http.Error(w, "Invalid HTTP method", http.StatusBadRequest)
			return
		}

		w.Header().Set("Content-Type", "application/json")

		err := r.ParseForm()
		if err != nil {
			WriteErrorResponse(w, err)
			return
		}

		retry, err := strconv.Atoi(r.FormValue("retry"))
		if err != nil {
			if r.FormValue("retry") == "" {
				retry = 0
			} else {
				WriteErrorResponse(w, err)
				return
			}
		}

		retryInterval, err := strconv.Atoi(r.FormValue("retry-interval"))
		if err != nil {
			if r.FormValue("retry-interval") == "" {
				retryInterval = 1
			} else {
				WriteErrorResponse(w, err)
				return
			}
		}

		mode, err := strconv.Atoi(r.FormValue("mode"))
		if err != nil {
			if r.FormValue("mode") == "" {
				mode = 0
			} else {
				WriteErrorResponse(w, err)
				return
			}
		}

		id := r.FormValue("id")
		if err := validate.ValidateJobId(id); err != nil && id != "" {
			WriteErrorResponse(w, err)
			return
		}

		store := r.FormValue("store")
		if err := validate.ValidateDatastore(store); err != nil {
			WriteErrorResponse(w, err)
			return
		}

		namespace := r.FormValue("ns")
		if err := validate.ValidateNamespace(namespace); err != nil {
			WriteErrorResponse(w, err)
			return
		}

		snapshot := r.FormValue("snapshot")
		if err := validate.ValidateSnapshot(snapshot); err != nil {
			WriteErrorResponse(w, err)
			return
		}

		srcPath := r.FormValue("src-path")
		if err := validate.ValidateSubpath("src-path", srcPath); err != nil {
			WriteErrorResponse(w, err)
			return
		}

		destSubpath := r.FormValue("dest-subpath")
		if err := validate.ValidateSubpath("dest-subpath", destSubpath); err != nil {
			WriteErrorResponse(w, err)
			return
		}

		preScript := r.FormValue("pre_script")
		if err := validate.ValidateScriptPath("pre_script", preScript); err != nil {
			WriteErrorResponse(w, err)
			return
		}

		postScript := r.FormValue("post_script")
		if err := validate.ValidateScriptPath("post_script", postScript); err != nil {
			WriteErrorResponse(w, err)
			return
		}

		newRestore := database.Restore{
			ID:               id,
			Store:            store,
			Namespace:        namespace,
			Snapshot:         snapshot,
			SrcPath:          srcPath,
			Mode:             mode,
			DestTarget:       database.Target{Name: r.FormValue("dest-target")},
			DestSubpath:      destSubpath,
			PreScript:        preScript,
			PostScript:       postScript,
			Comment:          r.FormValue("comment"),
			NotificationMode: r.FormValue("notification-mode"),
			Retry:            retry,
			RetryInterval:    retryInterval,
		}

		err = storeInstance.RestoreSvc.CreateRestore(newRestore)
		if err != nil {
			WriteErrorResponse(w, err)
			return
		}

		ApplyJobBatchAssignment(storeInstance, "restore", newRestore.ID, r.FormValue("notification-batch"))

		response.Status = http.StatusOK
		response.Success = true
		if err := json.NewEncoder(w).Encode(response); err != nil {
			syslog.L.Error(err).Write()
		}
	}
}

func ExtJsRestoreSingleHandler(storeInstance *store.Store) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		response := RestoreConfigResponse{}
		if r.Method != http.MethodPut && r.Method != http.MethodGet && r.Method != http.MethodDelete {
			http.Error(w, "Invalid HTTP method", http.StatusBadRequest)
			return
		}

		w.Header().Set("Content-Type", "application/json")

		if r.Method == http.MethodPut {
			restoreID := validate.DecodePath(r.PathValue("restore"))
			if err := validate.ValidateJobId(restoreID); err != nil {
				WriteErrorResponse(w, err)
				return
			}

			restore, err := storeInstance.RestoreSvc.GetRestore(restoreID)
			if err != nil {
				WriteErrorResponse(w, err)
				return
			}

			err = r.ParseForm()
			if err != nil {
				WriteErrorResponse(w, err)
				return
			}

			if r.FormValue("store") != "" {
				if err := validate.ValidateDatastore(r.FormValue("store")); err != nil {
					WriteErrorResponse(w, err)
					return
				}
				restore.Store = r.FormValue("store")
			}
			if r.FormValue("ns") != "" {
				if err := validate.ValidateNamespace(r.FormValue("ns")); err != nil {
					WriteErrorResponse(w, err)
					return
				}
				restore.Namespace = r.FormValue("ns")
			}
			if r.FormValue("snapshot") != "" {
				if err := validate.ValidateSnapshot(r.FormValue("snapshot")); err != nil {
					WriteErrorResponse(w, err)
					return
				}
				restore.Snapshot = r.FormValue("snapshot")
			}
			if r.FormValue("src-path") != "" {
				if err := validate.ValidateSubpath("src-path", r.FormValue("src-path")); err != nil {
					WriteErrorResponse(w, err)
					return
				}
				restore.SrcPath = r.FormValue("src-path")
			}
			if r.FormValue("dest-target") != "" {
				restore.DestTarget = database.Target{Name: r.FormValue("dest-target")}
			}
			if r.FormValue("dest-subpath") != "" {
				if err := validate.ValidateSubpath("dest-subpath", r.FormValue("dest-subpath")); err != nil {
					WriteErrorResponse(w, err)
					return
				}
				restore.DestSubpath = r.FormValue("dest-subpath")
			}
			if r.FormValue("comment") != "" {
				restore.Comment = r.FormValue("comment")
			}
			if r.FormValue("notification-mode") != "" {
				restore.NotificationMode = r.FormValue("notification-mode")
			}

			preScript := r.FormValue("pre_script")
			if err := validate.ValidateScriptPath("pre_script", preScript); err != nil {
				WriteErrorResponse(w, err)
				return
			}
			restore.PreScript = preScript

			postScript := r.FormValue("post_script")
			if err := validate.ValidateScriptPath("post_script", postScript); err != nil {
				WriteErrorResponse(w, err)
				return
			}
			restore.PostScript = postScript

			mode, err := strconv.Atoi(r.FormValue("mode"))
			if err != nil {
				mode = 0
			}

			restore.Mode = mode

			retry, err := strconv.Atoi(r.FormValue("retry"))
			if err != nil {
				retry = 0
			}

			retryInterval, err := strconv.Atoi(r.FormValue("retry-interval"))
			if err != nil {
				retryInterval = 1
			}

			restore.Retry = retry
			restore.RetryInterval = retryInterval

			if delArr, ok := r.Form["delete"]; ok {
				for _, attr := range delArr {
					switch attr {
					case "store":
						restore.Store = ""
					case "ns":
						restore.Namespace = ""
					case "snapshot":
						restore.Snapshot = ""
					case "src-path":
						restore.SrcPath = ""
					case "dest-target":
						restore.DestTarget.Name = ""
					case "dest-subpath":
						restore.DestSubpath = ""
					case "pre_script":
						restore.PreScript = ""
					case "post_script":
						restore.PostScript = ""
					case "comment":
						restore.Comment = ""
					case "notification-mode":
						restore.NotificationMode = ""
					case "mode":
						restore.Mode = 0
					case "retry":
						restore.Retry = 0
					case "retry-interval":
						restore.RetryInterval = 1
					}
				}
			}

			err = storeInstance.RestoreSvc.UpdateRestore(restore)
			if err != nil {
				WriteErrorResponse(w, err)
				return
			}

			ApplyJobBatchAssignment(storeInstance, "restore", restore.ID, r.FormValue("notification-batch"))

			response.Status = http.StatusOK
			response.Success = true
			if err := json.NewEncoder(w).Encode(response); err != nil {
				syslog.L.Error(err).Write()
			}

			return
		}

		if r.Method == http.MethodGet {
			restoreID := validate.DecodePath(r.PathValue("restore"))
			if err := validate.ValidateJobId(restoreID); err != nil {
				WriteErrorResponse(w, err)
				return
			}

			restore, err := storeInstance.RestoreSvc.GetRestore(restoreID)
			if err != nil {
				WriteErrorResponse(w, err)
				return
			}

			response.Status = http.StatusOK
			response.Success = true
			flat := FlattenRestoreForEdit(restore)
			flat["notification-batch"] = GetJobBatchName(storeInstance, "restore", restore.ID)
			response.Data = flat
			if err := json.NewEncoder(w).Encode(response); err != nil {
				syslog.L.Error(err).Write()
			}

			return
		}

		if r.Method == http.MethodDelete {
			restoreID := validate.DecodePath(r.PathValue("restore"))
			if err := validate.ValidateJobId(restoreID); err != nil {
				WriteErrorResponse(w, err)
				return
			}

			err := storeInstance.RestoreSvc.DeleteRestore(restoreID)
			if err != nil {
				WriteErrorResponse(w, err)
				return
			}

			response.Status = http.StatusOK
			response.Success = true
			if err := json.NewEncoder(w).Encode(response); err != nil {
				syslog.L.Error(err).Write()
			}
			return
		}
	}
}
