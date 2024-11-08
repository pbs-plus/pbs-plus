//go:build linux

package agents

import (
	"encoding/json"
	"fmt"
	"net/http"

	"github.com/sonroyaalmerol/pbs-d2d-backup/internal/logger"
	"github.com/sonroyaalmerol/pbs-d2d-backup/internal/proxy/controllers"
	"github.com/sonroyaalmerol/pbs-d2d-backup/internal/store"
)

type LogRequest struct {
	Hostname string `json:"hostname"`
	Message  string `json:"message"`
}

func AgentLogHandler(storeInstance *store.Store) func(http.ResponseWriter, *http.Request, map[string]string) {
	return func(w http.ResponseWriter, r *http.Request, pathVar map[string]string) {
		if r.Method != http.MethodPost {
			http.Error(w, "Invalid HTTP method", http.StatusBadRequest)
		}

		syslogger, err := logger.InitializeSyslogger()
		if err != nil {
			w.WriteHeader(http.StatusInternalServerError)
			controllers.WriteErrorResponse(w, err)
			return
		}

		var reqParsed LogRequest
		err = json.NewDecoder(r.Body).Decode(&reqParsed)
		if err != nil {
			w.WriteHeader(http.StatusInternalServerError)
			controllers.WriteErrorResponse(w, err)
			return
		}

		syslogger.Info(fmt.Sprintf("PBS Agent [%s]: %s", reqParsed.Hostname, reqParsed.Message))

		w.Header().Set("Content-Type", "application/json")
		err = json.NewEncoder(w).Encode(map[string]string{"success": "true"})
		if err != nil {
			w.WriteHeader(http.StatusInternalServerError)
			controllers.WriteErrorResponse(w, err)
			return
		}
	}
}
