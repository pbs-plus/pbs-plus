//go:build linux

package arpc

import (
	"encoding/json"
	"net/http"

	"github.com/sonroyaalmerol/pbs-plus/internal/arpc"
	"github.com/sonroyaalmerol/pbs-plus/internal/store"
	"github.com/sonroyaalmerol/pbs-plus/internal/syslog"
)

func ARPCHandler(store *store.Store) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		session, err := arpc.HijackUpgradeHTTP(w, r, nil)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}

		agentHostname := r.Header.Get("X-PBS-Agent")

		store.AddARPC(agentHostname, session)
		defer store.RemoveARPC(agentHostname)

		router := arpc.NewRouter()
		router.Handle("echo", func(req arpc.Request) (arpc.Response, error) {
			var msg string
			if err := json.Unmarshal(req.Payload, &msg); err != nil {
				return arpc.Response{Status: 400, Message: "invalid payload"}, err
			}
			return arpc.Response{Status: 200, Data: msg}, nil
		})

		if err := session.Serve(router); err != nil {
			syslog.L.Errorf("session closed:", err)
		}
	}
}
