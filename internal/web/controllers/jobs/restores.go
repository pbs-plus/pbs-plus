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
	"github.com/pbs-plus/pbs-plus/internal/store/types"
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

		target, err := storeInstance.Database.GetTarget(types.WrapTargetName(utils.DecodePath(r.PathValue("target"))))
		if err != nil {
			controllers.WriteErrorResponse(w, err)
			return
		}

		subPath := ""
		if r.FormValue("filepath") != "" {
			subPath = utils.DecodePath(r.FormValue("filepath"))
		}

		if !target.Path.IsAgent() {
			controllers.WriteErrorResponse(w, errors.ErrUnsupported)
			return
		}

		targetInfo := target.Path.GetPathInfo()

		arpcSess, ok := storeInstance.ARPCAgentsManager.GetStreamPipe(target.Name.GetHostname())
		if !ok {
			controllers.WriteErrorResponse(w, errors.New("target unreachable"))
			return
		}

		reqData := arpcTypes.FileTreeReq{HostPath: targetInfo.HostPath, SubPath: subPath}
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
			if restore.DestTargetPath.IsAgent() {
				session := vfssessions.GetSessionPxarReader(restore.GetStreamID())
				if session == nil {
					continue
				}

				stats = session.GetStats()
			} else {
				continue
			}

			allRestores[i].CurrentFileCount = int(stats.FilesAccessed)
			allRestores[i].CurrentFolderCount = int(stats.FoldersAccessed)
			allRestores[i].CurrentBytesTotal = int(stats.TotalBytes)
			allRestores[i].CurrentBytesSpeed = int(stats.ByteReadSpeed)
			allRestores[i].CurrentFilesSpeed = int(stats.FileAccessSpeed)
			allRestores[i].StatCacheHits = int(stats.StatCacheHits)
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

		newRestore := types.Restore{
			ID:            r.FormValue("id"),
			Store:         r.FormValue("store"),
			Namespace:     r.FormValue("ns"),
			Snapshot:      r.FormValue("snapshot"),
			SrcPath:       r.FormValue("src-path"),
			DestTarget:    types.WrapTargetName(r.FormValue("dest-target")),
			DestPath:      r.FormValue("dest-path"),
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
			restore, err := storeInstance.Database.GetRestore(utils.DecodePath(r.PathValue("restore")))
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
				restore.Store = r.FormValue("store")
			}
			if r.FormValue("ns") != "" {
				restore.Namespace = r.FormValue("ns")
			}
			if r.FormValue("snapshot") != "" {
				restore.Snapshot = r.FormValue("snapshot")
			}
			if r.FormValue("src-path") != "" {
				restore.SrcPath = r.FormValue("src-path")
			}
			if r.FormValue("dest-target") != "" {
				restore.DestTarget = types.WrapTargetName(r.FormValue("dest-target"))
			}
			if r.FormValue("dest-path") != "" {
				restore.DestPath = r.FormValue("dest-path")
			}
			if r.FormValue("comment") != "" {
				restore.Comment = r.FormValue("comment")
			}

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
						restore.DestTarget.Raw = ""
					case "dest-path":
						restore.DestPath = ""
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
			restore, err := storeInstance.Database.GetRestore(utils.DecodePath(r.PathValue("restore")))
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
			err := storeInstance.Database.DeleteRestore(nil, utils.DecodePath(r.PathValue("restore")))
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
