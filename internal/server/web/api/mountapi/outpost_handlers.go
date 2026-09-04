//go:build linux

package mountapi

import (
	"encoding/json"
	"fmt"
	"net/http"
	"strings"

	"github.com/pbs-plus/pbs-plus/internal/log"
	"github.com/pbs-plus/pbs-plus/internal/server/application"
	"github.com/pbs-plus/pbs-plus/internal/server/outpost"
	"github.com/pbs-plus/pbs-plus/internal/server/web/api/respond"
)

type outpostView struct {
	Name       string   `json:"name"`
	Type       string   `json:"type"`
	ListenAddr string   `json:"listen-addr"`
	Running    bool     `json:"running"`
	Error      string   `json:"error,omitempty"`
	Attached   []string `json:"attached"`
	Endpoints  []string `json:"endpoints"`
}

func toOutpostView(s outpost.Status) outpostView {
	return outpostView{
		Name:       s.Name,
		Type:       s.Type,
		ListenAddr: s.ListenAddr,
		Running:    s.Running,
		Error:      s.Error,
		Attached:   s.Attached,
		Endpoints:  s.Endpoints,
	}
}

func outpostFormValues(r *http.Request) outpost.Outpost {
	return outpost.Outpost{
		Name:       strings.TrimSpace(r.FormValue("name")),
		Type:       strings.TrimSpace(r.FormValue("type")),
		ListenAddr: strings.TrimSpace(r.FormValue("listen-addr")),
	}
}

func writeOutpostInvalid(w http.ResponseWriter, err error) {
	log.Error(err, "")
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusBadRequest)
	if encErr := json.NewEncoder(w).Encode(&respond.ErrorResponse{
		Message: err.Error(),
		Status:  http.StatusBadRequest,
		Success: false,
	}); encErr != nil {
		log.Error(encErr, "")
	}
}

func ExtJsOutpostsHandler(app *application.Runtime) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet && r.Method != http.MethodPost {
			http.Error(w, "Invalid HTTP method", http.StatusBadRequest)
			return
		}
		switch r.Method {
		case http.MethodGet:
			statuses := outpost.StatusAll()
			views := make([]outpostView, 0, len(statuses))
			for _, s := range statuses {
				views = append(views, toOutpostView(s))
			}
			writeExtJS(w, views)
		case http.MethodPost:
			if err := r.ParseForm(); err != nil {
				respond.WriteErrorResponse(w, err)
				return
			}
			o := outpostFormValues(r)
			if err := outpost.ValidateOutpost(o); err != nil {
				writeOutpostInvalid(w, err)
				return
			}
			if _, exists, err := outpost.LoadOutpost(o.Name); err != nil {
				respond.WriteErrorResponse(w, err)
				return
			} else if exists {
				writeOutpostInvalid(w, fmt.Errorf("outpost %s already exists", o.Name))
				return
			}
			if err := outpost.ApplyConfig(r.Context(), o); err != nil {
				respond.WriteErrorResponse(w, err)
				return
			}
			s := outpost.Status{Outpost: o, Running: true}
			writeExtJS(w, toOutpostView(s))
		}
	}
}

func ExtJsOutpostSingleHandler(app *application.Runtime) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet && r.Method != http.MethodPut && r.Method != http.MethodDelete {
			http.Error(w, "Invalid HTTP method", http.StatusBadRequest)
			return
		}
		name := r.PathValue("name")
		existing, ok, err := outpost.LoadOutpost(name)
		if err != nil {
			respond.WriteErrorResponse(w, err)
			return
		}
		if !ok {
			respond.WriteErrorResponse(w, fmt.Errorf("no such outpost"))
			return
		}
		switch r.Method {
		case http.MethodGet:
			for _, s := range outpost.StatusAll() {
				if s.Name == name {
					writeExtJS(w, toOutpostView(s))
					return
				}
			}
			writeExtJS(w, toOutpostView(outpost.Status{Outpost: existing}))
		case http.MethodPut:
			if err := r.ParseForm(); err != nil {
				respond.WriteErrorResponse(w, err)
				return
			}
			o := outpostFormValues(r)
			if o.Name == "" {
				o.Name = existing.Name
			}
			o.CreatedAt = existing.CreatedAt
			if err := outpost.ValidateOutpost(o); err != nil {
				writeOutpostInvalid(w, err)
				return
			}
			if err := outpost.ApplyConfig(r.Context(), o); err != nil {
				respond.WriteErrorResponse(w, err)
				return
			}
			if o.Name != existing.Name {
				if err := outpost.DeleteOutpost(existing.Name); err != nil {
					respond.WriteErrorResponse(w, err)
					return
				}
				outpost.StopOutpost(existing.Name)
			}
			for _, s := range outpost.StatusAll() {
				if s.Name == o.Name {
					writeExtJS(w, toOutpostView(s))
					return
				}
			}
			writeExtJS(w, toOutpostView(outpost.Status{Outpost: o, Running: true}))
		case http.MethodDelete:
			if hasAttachedShares(name) {
				respond.WriteErrorResponse(w, fmt.Errorf("outpost %s still has attached mounts", name))
				return
			}
			outpost.StopOutpost(name)
			if err := outpost.DeleteOutpost(name); err != nil {
				respond.WriteErrorResponse(w, err)
				return
			}
			writeExtJS(w, nil)
		}
	}
}

func hasAttachedShares(name string) bool {
	for _, s := range outpost.StatusAll() {
		if s.Name == name {
			return len(s.Attached) > 0
		}
	}
	return false
}
