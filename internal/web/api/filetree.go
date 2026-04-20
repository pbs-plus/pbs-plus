package api

import (
	"encoding/json"
	"errors"
	"net/http"

	"github.com/fxamacker/cbor/v2"
	arpcTypes "github.com/pbs-plus/pbs-plus/internal/agent/agentfs/types"
	"github.com/pbs-plus/pbs-plus/internal/store"
	"github.com/pbs-plus/pbs-plus/internal/store/system"

	"github.com/pbs-plus/pbs-plus/internal/validate"
)

func D2DFileTree(storeInstance *store.Store) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			http.Error(w, "Invalid HTTP method", http.StatusMethodNotAllowed)
			return
		}

		targetName := validate.DecodePath(r.PathValue("target"))
		target, err := storeInstance.Database.GetTarget(targetName)
		if err != nil {
			WriteErrorResponse(w, err)
			return
		}

		subPath := ""
		if r.FormValue("filepath") != "" {
			subPath = validate.DecodePath(r.FormValue("filepath"))
			if err := validate.ValidateSubpath("filepath", subPath); err != nil {
				WriteErrorResponse(w, err)
				return
			}
		}

		if !target.IsAgent() && !target.IsLocal() {
			WriteErrorResponse(w, errors.ErrUnsupported)
			return
		}

		if target.IsLocal() {
			respData, err := system.FileTree(target.Path, subPath)
			if err != nil {
				WriteErrorResponse(w, err)
				return
			}

			w.Header().Set("Content-Type", "application/json")
			json.NewEncoder(w).Encode(respData)
			return
		}

		arpcSess, ok := storeInstance.ARPCAgentsManager.GetStreamPipe(target.GetHostname())
		if !ok {
			WriteErrorResponse(w, errors.New("target unreachable"))
			return
		}

		reqData := arpcTypes.FileTreeReq{HostPath: target.GetAgentHostPath(), SubPath: subPath}
		resp, err := arpcSess.CallData(r.Context(), "filetree", &reqData)
		if err != nil {
			WriteErrorResponse(w, err)
			return
		}

		var respData arpcTypes.FileTreeResp
		err = cbor.Unmarshal(resp, &respData)
		if err != nil {
			WriteErrorResponse(w, err)
			return
		}

		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(respData)
	}
}
