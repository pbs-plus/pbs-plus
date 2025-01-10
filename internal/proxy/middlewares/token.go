//go:build linux

package middlewares

import (
	"net/http"

	"github.com/sonroyaalmerol/pbs-plus/internal/proxy"
	"github.com/sonroyaalmerol/pbs-plus/internal/store"
)

func AcquireToken(store *store.Store, next http.Handler) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Access-Control-Allow-Origin", "*")

		_ = proxy.ExtractTokenFromRequest(r, store)
		next.ServeHTTP(w, r)
	}
}
