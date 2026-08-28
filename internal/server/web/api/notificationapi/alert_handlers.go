package notificationapi

import (
	"encoding/json"
	"fmt"
	"net/http"
	"strconv"

	"github.com/pbs-plus/pbs-plus/internal/server/web/api/respond"

	"github.com/pbs-plus/pbs-plus/internal/log"
	"github.com/pbs-plus/pbs-plus/internal/server/application"
	"github.com/pbs-plus/pbs-plus/internal/server/coredb"
)

func AlertSettingsHandler(app *application.Runtime) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodGet {
			settings, err := app.CoreDB.ListAlertSettings()
			if err != nil {
				respond.WriteErrorResponse(w, err)
				return
			}
			if err := json.NewEncoder(w).Encode(map[string]any{
				"data":    settings,
				"success": true,
				"status":  http.StatusOK,
			}); err != nil {
				log.Error(err, "")
			}
			return
		}

		if r.Method == http.MethodPost || r.Method == http.MethodPut {
			if err := r.ParseForm(); err != nil {
				respond.WriteErrorResponse(w, err)
				return
			}

			name := r.FormValue("name")
			if name == "" {
				respond.WriteErrorResponse(w, fmt.Errorf("name is required"))
				return
			}

			setting, err := app.CoreDB.GetAlertSetting(name)
			if err != nil {
				respond.WriteErrorResponse(w, fmt.Errorf("alert setting not found: %w", err))
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
			if v := r.FormValue("schedule-time"); v != "" {
				setting.ScheduleTime = v
			} else {
				setting.ScheduleTime = ""
			}
			if v := r.FormValue("schedule-window-minutes"); v != "" {
				if i, err := strconv.Atoi(v); err == nil {
					setting.ScheduleWindowMinutes = i
				}
			}

			if err := app.CoreDB.UpsertAlertSetting(setting); err != nil {
				respond.WriteErrorResponse(w, err)
				return
			}

			if err := json.NewEncoder(w).Encode(map[string]any{
				"success": true,
				"status":  http.StatusOK,
			}); err != nil {
				log.Error(err, "")
			}
			return
		}

		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
	}
}

func AlertSettingSingleHandler(app *application.Runtime) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		name := r.PathValue("name")
		if name == "" {
			respond.WriteErrorResponse(w, fmt.Errorf("name is required"))
			return
		}

		if r.Method == http.MethodGet {
			setting, err := app.CoreDB.GetAlertSetting(name)
			if err != nil {
				respond.WriteErrorResponse(w, err)
				return
			}
			if err := json.NewEncoder(w).Encode(map[string]any{
				"data":    setting,
				"success": true,
				"status":  http.StatusOK,
			}); err != nil {
				log.Error(err, "")
			}
			return
		}

		if r.Method == http.MethodPut {
			if err := r.ParseForm(); err != nil {
				respond.WriteErrorResponse(w, err)
				return
			}

			setting, err := app.CoreDB.GetAlertSetting(name)
			if err != nil {
				respond.WriteErrorResponse(w, fmt.Errorf("alert setting not found: %w", err))
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
			if v := r.FormValue("schedule-time"); v != "" {
				setting.ScheduleTime = v
			} else {
				setting.ScheduleTime = ""
			}
			if v := r.FormValue("schedule-window-minutes"); v != "" {
				if i, err := strconv.Atoi(v); err == nil {
					setting.ScheduleWindowMinutes = i
				}
			}

			if err := app.CoreDB.UpsertAlertSetting(setting); err != nil {
				respond.WriteErrorResponse(w, err)
				return
			}

			if err := json.NewEncoder(w).Encode(map[string]any{
				"success": true,
				"status":  http.StatusOK,
			}); err != nil {
				log.Error(err, "")
			}
			return
		}

		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
	}
}

func AlertExclusionsHandler(app *application.Runtime) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodGet {
			alertType := r.URL.Query().Get("type")
			var exclusions []coredb.AlertExclusion
			var err error

			if alertType != "" {
				exclusions, err = app.CoreDB.ListAlertExclusions(alertType)
			} else {
				exclusions, err = app.CoreDB.ListAllAlertExclusions()
			}

			if err != nil {
				respond.WriteErrorResponse(w, err)
				return
			}

			if err := json.NewEncoder(w).Encode(map[string]any{
				"data":    exclusions,
				"success": true,
			}); err != nil {
				log.Error(err, "")
			}
			return
		}

		if r.Method == http.MethodPost {
			if err := r.ParseForm(); err != nil {
				respond.WriteErrorResponse(w, err)
				return
			}

			alertType := r.FormValue("alert-type")
			excludeType := r.FormValue("exclude-type")
			excludeValue := r.FormValue("exclude-value")
			comment := r.FormValue("comment")

			if alertType == "" || excludeType == "" || excludeValue == "" {
				respond.WriteErrorResponse(w, fmt.Errorf("alert-type, exclude-type, and exclude-value are required"))
				return
			}

			if err := app.CoreDB.CreateAlertExclusion(alertType, excludeType, excludeValue, comment); err != nil {
				respond.WriteErrorResponse(w, err)
				return
			}

			if err := json.NewEncoder(w).Encode(map[string]any{
				"success": true,
			}); err != nil {
				log.Error(err, "")
			}
			return
		}

		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
	}
}

func AlertExclusionSingleHandler(app *application.Runtime) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		idStr := r.PathValue("id")
		if idStr == "" {
			respond.WriteErrorResponse(w, fmt.Errorf("id is required"))
			return
		}

		id, err := strconv.ParseInt(idStr, 10, 64)
		if err != nil {
			respond.WriteErrorResponse(w, fmt.Errorf("invalid id"))
			return
		}

		if r.Method == http.MethodDelete {
			if err := app.CoreDB.DeleteAlertExclusion(id); err != nil {
				respond.WriteErrorResponse(w, err)
				return
			}

			if err := json.NewEncoder(w).Encode(map[string]any{
				"success": true,
			}); err != nil {
				log.Error(err, "")
			}
			return
		}

		if r.Method == http.MethodGet {
			exclusion, err := app.CoreDB.GetAlertExclusion(id)
			if err != nil {
				respond.WriteErrorResponse(w, err)
				return
			}

			if err := json.NewEncoder(w).Encode(map[string]any{
				"data":    exclusion,
				"success": true,
			}); err != nil {
				log.Error(err, "")
			}
			return
		}

		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
	}
}
