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
		clientCert := r.TLS.PeerCertificates[0]

		agentHostname := clientCert.Subject.CommonName
		agentVersion := r.Header.Get("X-PBS-Plus-Version")

		session, err := arpc.HijackUpgradeHTTP(w, r, agentHostname, agentVersion, store.ARPCSessionManager, nil)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		defer store.ARPCSessionManager.CloseSession(agentHostname)

		syslog.L.Info().WithMessage("agent successfully connected").WithField("hostname", agentHostname).Write()
		defer syslog.L.Info().WithMessage("agent disconnected").WithField("hostname", agentHostname).Write()

		if err := session.Serve(); err != nil {
			syslog.L.Error(err).WithMessage("error occurred while serving session").WithField("hostname", agentHostname).Write()
		}
	}
}
