//go:build linux

package plus

import (
	"net/http"

	"github.com/sonroyaalmerol/pbs-plus/internal/store"
)

func WSHandler(storeInstance *store.Store) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			http.Error(w, "Invalid HTTP method", http.StatusBadRequest)
			return
		}

		if err := storeInstance.CheckProxyAuth(r); err != nil {
			http.Error(w, "authentication failed - no authentication credentials provided", http.StatusUnauthorized)
			return
		}

		storeInstance.WSHub.HandleClientConnection(w, r)
	}
}
