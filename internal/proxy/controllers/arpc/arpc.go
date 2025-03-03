//go:build linux

package arpc

import (
	"net/http"

	"github.com/sonroyaalmerol/pbs-plus/internal/arpc"
	"github.com/sonroyaalmerol/pbs-plus/internal/store"
	"github.com/sonroyaalmerol/pbs-plus/internal/syslog"
)

func ARPCHandler(store *store.Store) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		agentHostname := r.Header.Get("X-PBS-Agent")

		session, err := arpc.HijackUpgradeHTTP(w, r, agentHostname, store.ARPCSessionManager, nil)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		defer store.ARPCSessionManager.CloseSession(agentHostname)

		syslog.L.Infof("Agent successfully connected: %s", agentHostname)
		defer syslog.L.Infof("Agent disconnected: %s", agentHostname)

		if err := session.Serve(); err != nil {
			syslog.L.Errorf("session closed: %v", err)
		}
	}
}
