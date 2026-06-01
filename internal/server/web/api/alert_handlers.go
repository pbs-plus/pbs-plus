package api

import (
	"encoding/json"
	"fmt"
	"net/http"
	"strconv"

	"github.com/pbs-plus/pbs-plus/internal/server/database"
	"github.com/pbs-plus/pbs-plus/internal/server/store"
)

// AlertSettingsHandler handles GET (list all) and POST (update) for alert settings.
func AlertSettingsHandler(storeInstance *store.Store) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
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

			name := r.FormValue("name")
			if name == "" {
				WriteErrorResponse(w, fmt.Errorf("name is required"))
				return
			}

			setting, err := storeInstance.Database.GetAlertSetting(name)
			if err != nil {
				WriteErrorResponse(w, fmt.Errorf("alert setting not found: %w", err))
				return
			}

			if v := r.FormValue("enabled"); v != "" {
				setting.Enabled = v == "1"
			}
			if v := r.FormValue("threshold"); v != "" {
				if i, err := strconv.Atoi(v); err == nil {
					setting.Threshold = i
				}
			}
			if v := r.FormValue("severity"); v != "" {
				setting.Severity = v
			}
			if v := r.FormValue("comment"); v != "" {
				setting.Comment = v
			}
			if v := r.FormValue("cooldown-minutes"); v != "" {
				if i, err := strconv.Atoi(v); err == nil {
					setting.CooldownMinutes = i
				}
			}
			if v := r.FormValue("quiet-days"); v != "" {
				var days []string
				if err := json.Unmarshal([]byte(v), &days); err == nil {
					setting.QuietDays = days
				}
			}
			if v := r.FormValue("skip-unscheduled"); v != "" {
				setting.SkipUnscheduled = v == "1"
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

			setting, err := storeInstance.Database.GetAlertSetting(name)
			if err != nil {
				WriteErrorResponse(w, fmt.Errorf("alert setting not found: %w", err))
				return
			}

			if v := r.FormValue("enabled"); v != "" {
				setting.Enabled = v == "1"
			}
			if v := r.FormValue("threshold"); v != "" {
				if i, err := strconv.Atoi(v); err == nil {
					setting.Threshold = i
				}
			}
			if v := r.FormValue("severity"); v != "" {
				setting.Severity = v
			}
			if v := r.FormValue("comment"); v != "" {
				setting.Comment = v
			}
			if v := r.FormValue("cooldown-minutes"); v != "" {
				if i, err := strconv.Atoi(v); err == nil {
					setting.CooldownMinutes = i
				}
			}
			if v := r.FormValue("quiet-days"); v != "" {
				var days []string
				if err := json.Unmarshal([]byte(v), &days); err == nil {
					setting.QuietDays = days
				}
			}
			if v := r.FormValue("skip-unscheduled"); v != "" {
				setting.SkipUnscheduled = v == "1"
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

// AlertExclusionsHandler handles GET (list) and POST (create) for alert exclusions.
func AlertExclusionsHandler(storeInstance *store.Store) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodGet {
			alertType := r.URL.Query().Get("type")
			var exclusions []database.AlertExclusion
			var err error

			if alertType != "" {
				exclusions, err = storeInstance.Database.ListAlertExclusions(alertType)
			} else {
				exclusions, err = storeInstance.Database.ListAllAlertExclusions()
			}

			if err != nil {
				WriteErrorResponse(w, err)
				return
			}

			json.NewEncoder(w).Encode(map[string]any{
				"data":    exclusions,
				"success": true,
			})
			return
		}

		if r.Method == http.MethodPost {
			if err := r.ParseForm(); err != nil {
				WriteErrorResponse(w, err)
				return
			}

			alertType := r.FormValue("alert-type")
			excludeType := r.FormValue("exclude-type")
			excludeValue := r.FormValue("exclude-value")
			comment := r.FormValue("comment")

			if alertType == "" || excludeType == "" || excludeValue == "" {
				WriteErrorResponse(w, fmt.Errorf("alert-type, exclude-type, and exclude-value are required"))
				return
			}

			if err := storeInstance.Database.CreateAlertExclusion(alertType, excludeType, excludeValue, comment); err != nil {
				WriteErrorResponse(w, err)
				return
			}

			json.NewEncoder(w).Encode(map[string]any{
				"success": true,
			})
			return
		}

		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
	}
}

// AlertExclusionSingleHandler handles DELETE for a single exclusion.
func AlertExclusionSingleHandler(storeInstance *store.Store) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		idStr := r.PathValue("id")
		if idStr == "" {
			WriteErrorResponse(w, fmt.Errorf("id is required"))
			return
		}

		id, err := strconv.ParseInt(idStr, 10, 64)
		if err != nil {
			WriteErrorResponse(w, fmt.Errorf("invalid id"))
			return
		}

		if r.Method == http.MethodDelete {
			if err := storeInstance.Database.DeleteAlertExclusion(id); err != nil {
				WriteErrorResponse(w, err)
				return
			}

			json.NewEncoder(w).Encode(map[string]any{
				"success": true,
			})
			return
		}

		if r.Method == http.MethodGet {
			exclusion, err := storeInstance.Database.GetAlertExclusion(id)
			if err != nil {
				WriteErrorResponse(w, err)
				return
			}

			json.NewEncoder(w).Encode(map[string]any{
				"data":    exclusion,
				"success": true,
			})
			return
		}

		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
	}
}
