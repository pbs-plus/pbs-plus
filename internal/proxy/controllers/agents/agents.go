//go:build linux

package agents

import (
	"encoding/json"
	"net/http"

	"github.com/sonroyaalmerol/pbs-plus/internal/proxy/controllers"
	"github.com/sonroyaalmerol/pbs-plus/internal/store"
	"github.com/sonroyaalmerol/pbs-plus/internal/syslog"
)

type LogRequest struct {
	Hostname string `json:"hostname"`
	Message  string `json:"message"`
	Level    string `json:"level"`
}

func AgentLogHandler(storeInstance *store.Store) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "Invalid HTTP method", http.StatusBadRequest)
		}

		if err := storeInstance.CheckProxyAuth(r); err != nil {
			http.Error(w, "authentication failed - no authentication credentials provided", http.StatusUnauthorized)
			return
		}

		syslogger, err := syslog.InitializeLogger()
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

		switch reqParsed.Level {
		case "info":
			syslogger.Infof("PBS Agent [%s]: %s", reqParsed.Hostname, reqParsed.Message)
		case "error":
			syslogger.Errorf("PBS Agent [%s]: %s", reqParsed.Hostname, reqParsed.Message)
		case "warn":
			syslogger.Warnf("PBS Agent [%s]: %s", reqParsed.Hostname, reqParsed.Message)
		default:
			syslogger.Infof("PBS Agent [%s]: %s", reqParsed.Hostname, reqParsed.Message)
		}

		w.Header().Set("Content-Type", "application/json")
		err = json.NewEncoder(w).Encode(map[string]string{"success": "true"})
		if err != nil {
			w.WriteHeader(http.StatusInternalServerError)
			controllers.WriteErrorResponse(w, err)
			return
		}
	}
}
