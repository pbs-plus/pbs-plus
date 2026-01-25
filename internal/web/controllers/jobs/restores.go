//go:build linux

package jobs

import (
	"encoding/json"
	"errors"
	"net/http"
	"os"
	"os/exec"
	"strconv"

	"github.com/fxamacker/cbor/v2"
	arpcTypes "github.com/pbs-plus/pbs-plus/internal/agent/agentfs/types"
	"github.com/pbs-plus/pbs-plus/internal/pxar"
	"github.com/pbs-plus/pbs-plus/internal/store"
	"github.com/pbs-plus/pbs-plus/internal/store/database"
	vfssessions "github.com/pbs-plus/pbs-plus/internal/store/vfs"
	"github.com/pbs-plus/pbs-plus/internal/utils"
	"github.com/pbs-plus/pbs-plus/internal/web/controllers"
)

func D2DRestoreFileTree(storeInstance *store.Store) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			http.Error(w, "Invalid HTTP method", http.StatusMethodNotAllowed)
			return
		}

		targetName := utils.DecodePath(r.PathValue("target"))
		target, err := storeInstance.Database.GetTarget(targetName)
		if err != nil {
			controllers.WriteErrorResponse(w, err)
			return
		}

		subPath := ""
		if r.FormValue("filepath") != "" {
			subPath = utils.DecodePath(r.FormValue("filepath"))
			if err := utils.ValidateSubpath("filepath", subPath); err != nil {
				controllers.WriteErrorResponse(w, err)
				return
			}
		}

		if !target.IsAgent() {
			controllers.WriteErrorResponse(w, errors.ErrUnsupported)
			return
		}

		arpcSess, ok := storeInstance.ARPCAgentsManager.GetStreamPipe(target.GetHostname())
		if !ok {
			controllers.WriteErrorResponse(w, errors.New("target unreachable"))
			return
		}

		reqData := arpcTypes.FileTreeReq{HostPath: target.GetAgentHostPath(), SubPath: subPath}
		resp, err := arpcSess.CallData(r.Context(), "filetree", &reqData)
		if err != nil {
			controllers.WriteErrorResponse(w, err)
			return
		}

		var respData arpcTypes.FileTreeResp
		err = cbor.Unmarshal(resp, &respData)
		if err != nil {
			controllers.WriteErrorResponse(w, err)
			return
		}

		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(respData)
	}
}

func D2DRestoreHandler(storeInstance *store.Store) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			http.Error(w, "Invalid HTTP method", http.StatusBadRequest)
			return
		}

		allRestores, err := storeInstance.Database.GetAllRestores()
		if err != nil {
			controllers.WriteErrorResponse(w, err)
			return
		}

		for i, restore := range allRestores {
			var stats pxar.PxarReaderStats
			if restore.DestTarget.IsAgent() {
				session := vfssessions.GetSessionPxarReader(restore.GetStreamID())
				if session == nil {
					continue
				}

				stats = session.GetStats()
			} else {
				continue
			}

			allRestores[i].CurrentStats.CurrentFileCount = int(stats.FilesAccessed)
			allRestores[i].CurrentStats.CurrentFolderCount = int(stats.FoldersAccessed)
			allRestores[i].CurrentStats.CurrentBytesTotal = int(stats.TotalBytes)
			allRestores[i].CurrentStats.CurrentBytesSpeed = int(stats.ByteReadSpeed)
			allRestores[i].CurrentStats.CurrentFilesSpeed = int(stats.FileAccessSpeed)
			allRestores[i].CurrentStats.StatCacheHits = int(stats.StatCacheHits)
		}

		digest, err := utils.CalculateDigest(allRestores)
		if err != nil {
			controllers.WriteErrorResponse(w, err)
			return
		}

		toReturn := RestoresResponse{
			Data:   allRestores,
			Digest: digest,
		}

		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(toReturn)
	}
}

func ExtJsRestoreRunHandler(storeInstance *store.Store) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost && r.Method != http.MethodDelete {
			http.Error(w, "Invalid HTTP method", http.StatusBadRequest)
			return
		}

		var response RestoreRunResponse

		// Get all restore IDs from query parameters: ?job=restore1&job=restore2
		restoreIDs := r.URL.Query()["job"]
		if len(restoreIDs) == 0 {
			http.Error(w, "Missing restore parameter(s)", http.StatusBadRequest)
			return
		}

		decodedRestoreIDs := []string{}

		for _, restoreID := range restoreIDs {
			decoded := utils.DecodePath(restoreID)

			if err := utils.ValidateJobId(decoded); err != nil {
				controllers.WriteErrorResponse(w, err)
				return
			}

			decodedRestoreIDs = append(decodedRestoreIDs, decoded)
		}

		execPath, err := os.Executable()
		if err != nil {
			controllers.WriteErrorResponse(w, err)
			return
		}

		args := []string{}
		for _, restoreId := range decodedRestoreIDs {
			args = append(args, "-restore-job", restoreId)
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

		id := r.FormValue("id")
		if err := utils.ValidateJobId(id); err != nil {
			controllers.WriteErrorResponse(w, err)
			return
		}

		store := r.FormValue("store")
		if err := utils.ValidateDatastore(store); err != nil {
			controllers.WriteErrorResponse(w, err)
			return
		}

		namespace := r.FormValue("ns")
		if err := utils.ValidateNamespace(namespace); err != nil {
			controllers.WriteErrorResponse(w, err)
			return
		}

		snapshot := r.FormValue("snapshot")
		if err := utils.ValidateSnapshot(snapshot); err != nil {
			controllers.WriteErrorResponse(w, err)
			return
		}

		srcPath := r.FormValue("src-path")
		if err := utils.ValidateSubpath("src-path", srcPath); err != nil {
			controllers.WriteErrorResponse(w, err)
			return
		}

		destSubpath := r.FormValue("dest-subpath")
		if err := utils.ValidateSubpath("dest-subpath", destSubpath); err != nil {
			controllers.WriteErrorResponse(w, err)
			return
		}

		preScript := r.FormValue("pre_script")
		if err := utils.ValidateScriptPath("pre_script", preScript); err != nil {
			controllers.WriteErrorResponse(w, err)
			return
		}

		postScript := r.FormValue("post_script")
		if err := utils.ValidateScriptPath("post_script", postScript); err != nil {
			controllers.WriteErrorResponse(w, err)
			return
		}

		newRestore := database.Restore{
			ID:            id,
			Store:         store,
			Namespace:     namespace,
			Snapshot:      snapshot,
			SrcPath:       srcPath,
			DestTarget:    database.Target{Name: r.FormValue("dest-target")},
			DestSubpath:   destSubpath,
			PreScript:     preScript,
			PostScript:    postScript,
			Comment:       r.FormValue("comment"),
			Retry:         retry,
			RetryInterval: retryInterval,
		}

		err = storeInstance.Database.CreateRestore(nil, newRestore)
		if err != nil {
			controllers.WriteErrorResponse(w, err)
			return
		}

		response.Status = http.StatusOK
		response.Success = true
		json.NewEncoder(w).Encode(response)
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
			restoreID := utils.DecodePath(r.PathValue("restore"))
			if err := utils.ValidateJobId(restoreID); err != nil {
				controllers.WriteErrorResponse(w, err)
				return
			}

			restore, err := storeInstance.Database.GetRestore(restoreID)
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
				if err := utils.ValidateDatastore(r.FormValue("store")); err != nil {
					controllers.WriteErrorResponse(w, err)
					return
				}
				restore.Store = r.FormValue("store")
			}
			if r.FormValue("ns") != "" {
				if err := utils.ValidateNamespace(r.FormValue("ns")); err != nil {
					controllers.WriteErrorResponse(w, err)
					return
				}
				restore.Namespace = r.FormValue("ns")
			}
			if r.FormValue("snapshot") != "" {
				if err := utils.ValidateSnapshot(r.FormValue("snapshot")); err != nil {
					controllers.WriteErrorResponse(w, err)
					return
				}
				restore.Snapshot = r.FormValue("snapshot")
			}
			if r.FormValue("src-path") != "" {
				if err := utils.ValidateSubpath("src-path", r.FormValue("src-path")); err != nil {
					controllers.WriteErrorResponse(w, err)
					return
				}
				restore.SrcPath = r.FormValue("src-path")
			}
			if r.FormValue("dest-target") != "" {
				restore.DestTarget = database.Target{Name: r.FormValue("dest-target")}
			}
			if r.FormValue("dest-subpath") != "" {
				if err := utils.ValidateSubpath("dest-subpath", r.FormValue("dest-subpath")); err != nil {
					controllers.WriteErrorResponse(w, err)
					return
				}
				restore.DestSubpath = r.FormValue("dest-subpath")
			}
			if r.FormValue("comment") != "" {
				restore.Comment = r.FormValue("comment")
			}

			preScript := r.FormValue("pre_script")
			if err := utils.ValidateScriptPath("pre_script", preScript); err != nil {
				controllers.WriteErrorResponse(w, err)
				return
			}
			restore.PreScript = preScript

			postScript := r.FormValue("post_script")
			if err := utils.ValidateScriptPath("post_script", postScript); err != nil {
				controllers.WriteErrorResponse(w, err)
				return
			}
			restore.PostScript = postScript

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
					case "retry":
						restore.Retry = 0
					case "retry-interval":
						restore.RetryInterval = 1
					}
				}
			}

			err = storeInstance.Database.UpdateRestore(nil, restore)
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
			restoreID := utils.DecodePath(r.PathValue("restore"))
			if err := utils.ValidateJobId(restoreID); err != nil {
				controllers.WriteErrorResponse(w, err)
				return
			}

			restore, err := storeInstance.Database.GetRestore(restoreID)
			if err != nil {
				controllers.WriteErrorResponse(w, err)
				return
			}

			response.Status = http.StatusOK
			response.Success = true
			response.Data = restore
			json.NewEncoder(w).Encode(response)

			return
		}

		if r.Method == http.MethodDelete {
			restoreID := utils.DecodePath(r.PathValue("restore"))
			if err := utils.ValidateJobId(restoreID); err != nil {
				controllers.WriteErrorResponse(w, err)
				return
			}

			err := storeInstance.Database.DeleteRestore(nil, restoreID)
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
