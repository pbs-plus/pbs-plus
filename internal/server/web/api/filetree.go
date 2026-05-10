package api

import (
	"encoding/json"
	"errors"
	"net/http"

	"github.com/fxamacker/cbor/v2"
	arpcTypes "github.com/pbs-plus/pbs-plus/internal/agent/agentfs/types"
	backend "github.com/pbs-plus/pbs-plus/internal/server"
	"github.com/pbs-plus/pbs-plus/internal/server/store"

	"github.com/pbs-plus/pbs-plus/internal/validate"
)

func D2DFileTree(storeInstance *store.Store) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			http.Error(w, "Invalid HTTP method", http.StatusMethodNotAllowed)
			return
		}

		targetName := validate.DecodePath(r.PathValue("target"))
		target, err := storeInstance.TargetSvc.GetTarget(targetName)
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
			respData, err := backend.FileTree(target.Path, subPath)
			if err != nil {
				WriteErrorResponse(w, err)
				return
			}

			w.Header().Set("Content-Type", "application/json")
			json.NewEncoder(w).Encode(respData)
			return
		}

		var resp []byte
		var ftErr error

		if qSess, qOk := storeInstance.ARPCAgentsManager.GetQuicPipe(target.GetHostname()); qOk {
			reqData := arpcTypes.FileTreeReq{HostPath: target.GetAgentHostPath(), SubPath: subPath}
			resp, ftErr = qSess.CallData(r.Context(), "filetree", &reqData)
		} else if tSess, tOk := storeInstance.ARPCAgentsManager.GetStreamPipe(target.GetHostname()); tOk {
			reqData := arpcTypes.FileTreeReq{HostPath: target.GetAgentHostPath(), SubPath: subPath}
			resp, ftErr = tSess.CallData(r.Context(), "filetree", &reqData)
		} else {
			WriteErrorResponse(w, errors.New("target unreachable"))
			return
		}

		if ftErr != nil {
			WriteErrorResponse(w, ftErr)
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
