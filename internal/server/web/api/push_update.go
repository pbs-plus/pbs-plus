//go:build linux

package api

import (
	"context"
	"encoding/json"
	"net/http"
	"strings"
	"time"

	"github.com/pbs-plus/pbs-plus/internal/log"
	"github.com/pbs-plus/pbs-plus/internal/server/store"
)

type pushUpdateRequest struct {
	Hostnames []string `json:"hostnames"`
	Timeout   int      `json:"timeout"`
}

type pushUpdateResponse struct {
	Data    []applicationPushUpdateResult `json:"data"`
	Success bool                          `json:"success"`
}

type applicationPushUpdateResult struct {
	Hostname string `json:"hostname"`
	Updated  bool   `json:"updated"`
	Message  string `json:"message"`
}

func ExtJsPushUpdateHandler(storeInstance *store.Store) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "Invalid HTTP method", http.StatusMethodNotAllowed)
			return
		}

		var req pushUpdateRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			if !strings.Contains(err.Error(), "EOF") {
				WriteErrorResponse(w, err)
				return
			}
		}

		timeout := time.Duration(req.Timeout) * time.Second
		if timeout <= 0 {
			timeout = 30 * time.Second
		}

		ctx, cancel := withTimeout(r.Context(), timeout+10*time.Second)
		defer cancel()

		results := storeInstance.TargetSvc.PushUpdate(ctx, req.Hostnames, timeout)

		out := make([]applicationPushUpdateResult, len(results))
		for i, res := range results {
			out[i] = applicationPushUpdateResult{
				Hostname: res.Hostname,
				Updated:  res.Updated,
				Message:  res.Message,
			}
		}

		resp := pushUpdateResponse{Data: out, Success: true}

		w.Header().Set("Content-Type", "application/json")
		if err := json.NewEncoder(w).Encode(resp); err != nil {
			log.Error(err, "")
		}
	}
}

func withTimeout(parent context.Context, d time.Duration) (context.Context, context.CancelFunc) {
	if parent == nil {
		return context.WithTimeout(context.Background(), d)
	}
	return context.WithTimeout(parent, d)
}
