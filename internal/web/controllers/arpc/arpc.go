//go:build linux

package arpc

import (
	"errors"
	"io"
	"net/http"

	"github.com/pbs-plus/pbs-plus/internal/arpc"
	s "github.com/pbs-plus/pbs-plus/internal/store"
	"github.com/pbs-plus/pbs-plus/internal/syslog"
)

func ARPCHandler(store *s.Store) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		clientCert := r.TLS.PeerCertificates[0]

		agentHostname := clientCert.Subject.CommonName
		jobId := r.Header.Get("X-PBS-Plus-JobId")
		agentVersion := r.Header.Get("X-PBS-Plus-Version")

		if jobId != "" {
			agentHostname = agentHostname + "|" + jobId
		}

		session, err := arpc.HijackUpgradeHTTP(w, r, agentHostname, agentVersion, store.ARPCAgentsManager, nil)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		defer func() {
			store.ARPCAgentsManager.CloseSession(agentHostname)
			s.DisconnectSession(agentHostname)
		}()

		syslog.L.Info().WithMessage("agent successfully connected").WithField("hostname", agentHostname).Write()
		defer syslog.L.Info().WithMessage("agent disconnected").WithField("hostname", agentHostname).Write()

		if err := session.Serve(); err != nil {
			if !errors.Is(err, io.ErrClosedPipe) && !errors.Is(err, io.EOF) {
				syslog.L.Error(err).WithMessage("error occurred while serving session").WithField("hostname", agentHostname).Write()
			}
		}
	}
}
