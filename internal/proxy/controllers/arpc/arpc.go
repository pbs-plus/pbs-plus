//go:build linux

package arpc

import (
	"fmt"
	"net/http"

	"github.com/sonroyaalmerol/pbs-plus/internal/arpc"
	"github.com/sonroyaalmerol/pbs-plus/internal/store"
	"github.com/sonroyaalmerol/pbs-plus/internal/syslog"
	"github.com/valyala/fastjson"
)

func ARPCHandler(store *store.Store) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		session, err := arpc.HijackUpgradeHTTP(w, r, nil)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}

		agentHostname := r.Header.Get("X-PBS-Agent")

		syslog.L.Infof("Agent successfully connected: %s", agentHostname)
		defer syslog.L.Infof("Agent disconnected: %s", agentHostname)

		store.AddARPC(agentHostname, session)
		defer store.RemoveARPC(agentHostname)

		router := arpc.NewRouter()
		router.Handle("echo", func(req arpc.Request) (arpc.Response, error) {
			// Ensure the payload is a JSON string.
			if req.Payload == nil || req.Payload.Type() != fastjson.TypeString {
				return arpc.Response{
					Status:  400,
					Message: "invalid payload",
				}, fmt.Errorf("payload is not a string")
			}
			msg := string(req.Payload.GetStringBytes())

			dataVal := fastjson.MustParse(fmt.Sprintf("%q", msg))

			return arpc.Response{
				Status: 200,
				Data:   dataVal,
			}, nil
		})

		if err := session.Serve(router); err != nil {
			syslog.L.Errorf("session closed: %v", err)
		}
	}
}
