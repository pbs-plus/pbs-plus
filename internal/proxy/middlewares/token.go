//go:build linux

package middlewares

import (
	"fmt"
	"net/http"

	"github.com/sonroyaalmerol/pbs-plus/internal/proxy"
	"github.com/sonroyaalmerol/pbs-plus/internal/store"
	"github.com/sonroyaalmerol/pbs-plus/internal/utils"
)

func AcquireToken(store *store.Store, next http.Handler) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if utils.IsRequestFromSelf(r) {
			selfIP := utils.GetIPFromRequest(r)
			allowedOrigin := fmt.Sprintf("https://%s:8007", selfIP)
			allowedHeaders := r.Header.Get("Access-Control-Request-Headers")
			if allowedHeaders == "" {
				allowedHeaders = "Content-Type, *"
			}

			w.Header().Set("Access-Control-Allow-Origin", allowedOrigin)
			w.Header().Set("Access-Control-Allow-Methods", "POST, GET, OPTIONS, PUT, DELETE")
			w.Header().Set("Access-Control-Allow-Headers", allowedHeaders)
			w.Header().Set("Access-Control-Allow-Credentials", "true")
		}

		_ = proxy.ExtractTokenFromRequest(r, store)
		next.ServeHTTP(w, r)
	}
}
