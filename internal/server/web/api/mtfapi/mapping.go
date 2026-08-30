//go:build linux

package mtfapi

import (
	"encoding/json"
	"fmt"
	"net/http"
	"strconv"

	"github.com/pbs-plus/pbs-plus/internal/server/web/api/respond"

	"github.com/pbs-plus/pbs-plus/internal/log"
	"github.com/pbs-plus/pbs-plus/internal/server/application"
	"github.com/pbs-plus/pbs-plus/internal/server/mtf/mtfdb"
)

func ExtJsMtfMappingHandler(app *application.Runtime) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		ms := mtfStore(app)
		if ms == nil {
			respond.WriteErrorResponse(w, fmt.Errorf("MTF store unavailable"))
			return
		}

		switch r.Method {
		case http.MethodGet:
			list, err := ms.ListMappings(r.Context())
			if err != nil {
				respond.WriteErrorResponse(w, err)
				return
			}
			response := MtfMappingConfigResponse{}
			w.Header().Set("Content-Type", "application/json")
			response.Status = http.StatusOK
			response.Success = true
			response.Data = list
			if err := json.NewEncoder(w).Encode(response); err != nil {
				log.Error(err, "")
			}

		case http.MethodPost:
			response := MtfMappingConfigResponse{}
			w.Header().Set("Content-Type", "application/json")

			if err := r.ParseForm(); err != nil {
				respond.WriteErrorResponse(w, err)
				return
			}
			m := mtfdb.NamespaceMapping{
				Name:       r.FormValue("name"),
				Priority:   atoiDefault(r.FormValue("priority"), 0),
				MatchRegex: r.FormValue("match_regex"),
				Template:   r.FormValue("template"),
				IsDefault:  r.FormValue("is_default") == "1" || r.FormValue("is_default") == "true",
				Enabled:    r.FormValue("enabled") == "1" || r.FormValue("enabled") == "true",
				Comment:    r.FormValue("comment"),
			}
			if m.Template == "" {
				respond.WriteErrorResponse(w, fmt.Errorf("template is required"))
				return
			}
			id, err := ms.CreateMapping(r.Context(), m)
			if err != nil {
				respond.WriteErrorResponse(w, err)
				return
			}
			if mapper := app.MtfMapper; mapper != nil {
				mapper.Invalidate()
			}

			response.Status = http.StatusOK
			response.Success = true
			response.Data = mtfdb.NamespaceMapping{ID: id}
			if err := json.NewEncoder(w).Encode(response); err != nil {
				log.Error(err, "")
			}

		default:
			http.Error(w, "Invalid HTTP method", http.StatusBadRequest)
		}
	}
}

func ExtJsMtfMappingSingleHandler(app *application.Runtime) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet && r.Method != http.MethodPut && r.Method != http.MethodDelete {
			http.Error(w, "Invalid HTTP method", http.StatusBadRequest)
			return
		}
		ms := mtfStore(app)
		if ms == nil {
			respond.WriteErrorResponse(w, fmt.Errorf("MTF store unavailable"))
			return
		}
		id, err := strconv.ParseInt(r.PathValue("id"), 10, 64)
		if err != nil || id <= 0 {
			respond.WriteErrorResponse(w, fmt.Errorf("invalid mapping id"))
			return
		}

		w.Header().Set("Content-Type", "application/json")

		switch r.Method {
		case http.MethodGet:
			m, err := ms.GetMapping(r.Context(), id)
			if err != nil {
				respond.WriteErrorResponse(w, err)
				return
			}
			response := MtfMappingConfigResponse{
				Status:  http.StatusOK,
				Success: true,
				Data:    m}
			if err := json.NewEncoder(w).Encode(response); err != nil {
				log.Error(err, "")
			}

		case http.MethodPut:
			m, err := ms.GetMapping(r.Context(), id)
			if err != nil {
				respond.WriteErrorResponse(w, err)
				return
			}
			if err := r.ParseForm(); err != nil {
				respond.WriteErrorResponse(w, err)
				return
			}
			if v := r.FormValue("name"); v != "" {
				m.Name = v
			}
			if v := r.FormValue("priority"); v != "" {
				m.Priority = atoiDefault(v, 0)
			}
			if v := r.FormValue("match_regex"); v != "" {
				m.MatchRegex = v
			}
			if v := r.FormValue("template"); v != "" {
				m.Template = v
			}
			if r.FormValue("enabled") != "" {
				m.Enabled = r.FormValue("enabled") == "1" || r.FormValue("enabled") == "true"
			}
			if r.FormValue("is_default") != "" {
				m.IsDefault = r.FormValue("is_default") == "1" || r.FormValue("is_default") == "true"
			}
			if v := r.FormValue("comment"); v != "" {
				m.Comment = v
			}

			if delArr, ok := r.Form["delete"]; ok {
				for _, attr := range delArr {
					switch attr {
					case "name":
						m.Name = ""
					case "match_regex":
						m.MatchRegex = ""
					case "template":
						m.Template = ""
					case "comment":
						m.Comment = ""
					}
				}
			}

			if err := ms.UpdateMapping(r.Context(), m); err != nil {
				respond.WriteErrorResponse(w, err)
				return
			}
			if mapper := app.MtfMapper; mapper != nil {
				mapper.Invalidate()
			}

			response := MtfMappingConfigResponse{
				Status:  http.StatusOK,
				Success: true}
			if err := json.NewEncoder(w).Encode(response); err != nil {
				log.Error(err, "")
			}

		case http.MethodDelete:
			if err := ms.DeleteMapping(r.Context(), id); err != nil {
				respond.WriteErrorResponse(w, err)
				return
			}
			if mapper := app.MtfMapper; mapper != nil {
				mapper.Invalidate()
			}

			response := MtfMappingConfigResponse{
				Status:  http.StatusOK,
				Success: true}
			if err := json.NewEncoder(w).Encode(response); err != nil {
				log.Error(err, "")
			}
		}
	}
}

// flatMtfJob is the flattened API response for an MTF job. The history block

type MtfMappingConfigResponse struct {
	Errors  map[string]string `json:"errors"`
	Message string            `json:"message"`
	Data    any               `json:"data"`
	Status  int               `json:"status"`
	Success bool              `json:"success"`
}
