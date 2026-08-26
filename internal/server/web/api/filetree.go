package api

import (
	"encoding/json"
	"errors"
	"net/http"

	"github.com/fxamacker/cbor/v2"
	"github.com/pbs-plus/pbs-plus/internal/agent/agentfs/fswire"
	"github.com/pbs-plus/pbs-plus/internal/filetree"
	"github.com/pbs-plus/pbs-plus/internal/server/application"
	"github.com/pbs-plus/pbs-plus/internal/server/jobs"

	"github.com/pbs-plus/pbs-plus/internal/log"
	"github.com/pbs-plus/pbs-plus/internal/validate"
)

func D2DFileTree(app *application.Runtime) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			http.Error(w, "Invalid HTTP method", http.StatusMethodNotAllowed)
			return
		}

		targetName := validate.DecodePath(r.PathValue("target"))
		target, err := app.Target.GetTarget(targetName)
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
			respData, err := filetree.Read(target.Path, subPath)
			if err != nil {
				WriteErrorResponse(w, err)
				return
			}

			w.Header().Set("Content-Type", "application/json")
			if err := json.NewEncoder(w).Encode(respData); err != nil {
				log.Error(err, "")
			}
			return
		}

		var resp []byte
		var ftErr error

		if qSess, qOk := app.Agents.GetQuicPipe(target.GetHostname()); qOk {
			reqData := fswire.FileTreeReq{HostPath: target.GetAgentHostPath(), SubPath: subPath}
			resp, ftErr = qSess.CallData(r.Context(), "filetree", &reqData)
		} else if tSess, tOk := app.Agents.GetStreamPipe(target.GetHostname()); tOk {
			reqData := fswire.FileTreeReq{HostPath: target.GetAgentHostPath(), SubPath: subPath}
			resp, ftErr = tSess.CallData(r.Context(), "filetree", &reqData)
		} else {
			WriteErrorResponse(w, jobs.ErrTargetUnreachable)
			return
		}

		if ftErr != nil {
			WriteErrorResponse(w, ftErr)
			return
		}

		var respData fswire.FileTreeResp
		err = cbor.Unmarshal(resp, &respData)
		if err != nil {
			WriteErrorResponse(w, err)
			return
		}

		w.Header().Set("Content-Type", "application/json")
		if err := json.NewEncoder(w).Encode(respData); err != nil {
			log.Error(err, "")
		}
	}
}
