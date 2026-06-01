package api

import (
	"encoding/json"
	"fmt"
	"net/http"

	"github.com/pbs-plus/pbs-plus/internal/server/database"
	"github.com/pbs-plus/pbs-plus/internal/server/store"
)

// AlertSettingsHandler handles GET (list) and POST (upsert) for alert settings.
func AlertSettingsHandler(storeInstance *store.Store) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")

		if r.Method == http.MethodGet {
			settings, err := storeInstance.Database.ListAlertSettings()
			if err != nil {
				WriteErrorResponse(w, err)
				return
			}
			json.NewEncoder(w).Encode(map[string]any{
				"data":    settings,
				"success": true,
				"status":  http.StatusOK,
			})
			return
		}

		if r.Method == http.MethodPost || r.Method == http.MethodPut {
			if err := r.ParseForm(); err != nil {
				WriteErrorResponse(w, err)
				return
			}

			setting := database.AlertSetting{
				Name:     r.FormValue("name"),
				Severity: r.FormValue("severity"),
				Comment:  r.FormValue("comment"),
			}

			if setting.Name == "" {
				WriteErrorResponse(w, fmt.Errorf("name is required"))
				return
			}

			setting.Enabled = r.FormValue("enabled") != "0"
			setting.Threshold = formValueInt(r, "threshold", 0)
			if setting.Severity == "" {
				setting.Severity = "warning"
			}

			if err := storeInstance.Database.UpsertAlertSetting(setting); err != nil {
				WriteErrorResponse(w, err)
				return
			}

			json.NewEncoder(w).Encode(map[string]any{
				"success": true,
				"status":  http.StatusOK,
			})
			return
		}

		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
	}
}

// AlertSettingSingleHandler handles GET/PUT for a single alert setting.
func AlertSettingSingleHandler(storeInstance *store.Store) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")

		name := r.PathValue("name")
		if name == "" {
			WriteErrorResponse(w, fmt.Errorf("name is required"))
			return
		}

		if r.Method == http.MethodGet {
			setting, err := storeInstance.Database.GetAlertSetting(name)
			if err != nil {
				WriteErrorResponse(w, err)
				return
			}
			json.NewEncoder(w).Encode(map[string]any{
				"data":    setting,
				"success": true,
				"status":  http.StatusOK,
			})
			return
		}

		if r.Method == http.MethodPut {
			if err := r.ParseForm(); err != nil {
				WriteErrorResponse(w, err)
				return
			}

			setting := database.AlertSetting{
				Name:     name,
				Severity: r.FormValue("severity"),
				Comment:  r.FormValue("comment"),
			}

			setting.Enabled = r.FormValue("enabled") != "0"
			setting.Threshold = formValueInt(r, "threshold", 0)
			if setting.Severity == "" {
				setting.Severity = "warning"
			}

			if err := storeInstance.Database.UpsertAlertSetting(setting); err != nil {
				WriteErrorResponse(w, err)
				return
			}

			json.NewEncoder(w).Encode(map[string]any{
				"success": true,
				"status":  http.StatusOK,
			})
			return
		}

		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
	}
}
