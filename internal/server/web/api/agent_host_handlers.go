//go:build linux

package api

import (
	"encoding/json"
	"net/http"

	"github.com/pbs-plus/pbs-plus/internal/server/application"

	"github.com/pbs-plus/pbs-plus/internal/log"
	"github.com/pbs-plus/pbs-plus/internal/validate"
)

func ExtJsAgentSingleHandler(app *application.Runtime) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		response := AgentConfigResponse{}
		if r.Method != http.MethodGet && r.Method != http.MethodDelete {
			http.Error(w, "Invalid HTTP method", http.StatusMethodNotAllowed)
			return
		}

		w.Header().Set("Content-Type", "application/json")

		if r.Method == http.MethodGet {
			agent, err := app.AgentHost.GetAgentHost(validate.DecodePath(r.PathValue("agent")))
			if err != nil {
				WriteErrorResponse(w, err)
				return
			}

			response.Status = http.StatusOK
			response.Success = true
			response.Data = agent
			if err := json.NewEncoder(w).Encode(response); err != nil {
				log.Error(err, "")
			}

			return
		}

		if r.Method == http.MethodDelete {
			err := app.AgentHost.DeleteAgentHost(validate.DecodePath(r.PathValue("agent")))
			if err != nil {
				WriteErrorResponse(w, err)
				return
			}

			response.Status = http.StatusOK
			response.Success = true
			if err := json.NewEncoder(w).Encode(response); err != nil {
				log.Error(err, "")
			}
			return
		}
	}
}
