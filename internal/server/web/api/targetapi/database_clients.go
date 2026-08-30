//go:build linux

package targetapi

import (
	"encoding/json"
	"net/http"

	"github.com/pbs-plus/pbs-plus/internal/log"
	"github.com/pbs-plus/pbs-plus/internal/server/database"
	"github.com/pbs-plus/pbs-plus/internal/server/web/api/respond"
)

func D2DDatabaseClientsHandler() http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			http.Error(w, "Invalid HTTP method", http.StatusMethodNotAllowed)
			return
		}
		bundles, err := database.DiscoverClientBundles(r.Context())
		if err != nil {
			respond.WriteErrorResponse(w, err)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		if err := json.NewEncoder(w).Encode(map[string]any{"data": bundles, "success": true}); err != nil {
			log.Error(err, "")
		}
	}
}
