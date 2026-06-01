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
				Name:            r.FormValue("name"),
				Severity:        r.FormValue("severity"),
				Comment:         r.FormValue("comment"),
				CooldownMinutes: formValueInt(r, "cooldown-minutes", 1440),
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
			if setting.CooldownMinutes <= 0 {
				setting.CooldownMinutes = 1440
			}

			// Parse quiet-days as JSON array
			if qd := r.FormValue("quiet-days"); qd != "" {
				if err := json.Unmarshal([]byte(qd), &setting.QuietDays); err != nil {
					WriteErrorResponse(w, fmt.Errorf("invalid quiet-days: %w", err))
					return
				}
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
				Name:            name,
				Severity:        r.FormValue("severity"),
				Comment:         r.FormValue("comment"),
				CooldownMinutes: formValueInt(r, "cooldown-minutes", 1440),
			}

			setting.Enabled = r.FormValue("enabled") != "0"
			setting.Threshold = formValueInt(r, "threshold", 0)
			if setting.Severity == "" {
				setting.Severity = "warning"
			}
			if setting.CooldownMinutes <= 0 {
				setting.CooldownMinutes = 1440
			}

			// Parse quiet-days as JSON array
			if qd := r.FormValue("quiet-days"); qd != "" {
				if err := json.Unmarshal([]byte(qd), &setting.QuietDays); err != nil {
					WriteErrorResponse(w, fmt.Errorf("invalid quiet-days: %w", err))
					return
				}
			}

			// Preserve last-sent: reload existing, keep its value
			existing, err := storeInstance.Database.GetAlertSetting(name)
			if err == nil {
				setting.LastSent = existing.LastSent
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
