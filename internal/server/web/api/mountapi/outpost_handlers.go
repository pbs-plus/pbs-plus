//go:build linux

package mountapi

import (
	"encoding/json"
	"fmt"
	"net/http"
	"strings"

	"github.com/pbs-plus/pbs-plus/internal/server/application"
	"github.com/pbs-plus/pbs-plus/internal/server/objectstore"
	"github.com/pbs-plus/pbs-plus/internal/server/outpost"
	"github.com/pbs-plus/pbs-plus/internal/server/snapshotmount"
	"github.com/pbs-plus/pbs-plus/internal/server/web/api/respond"
)

type outpostView struct {
	Name       string          `json:"name"`
	Type       string          `json:"type"`
	ListenAddr string          `json:"listen-addr"`
	Guest      bool            `json:"guest"`
	ValidUsers string          `json:"valid-users"`
	ForceUser  string          `json:"force-user"`
	HostsAllow string          `json:"hosts-allow"`
	Browseable bool            `json:"browseable"`
	S3         json.RawMessage `json:"s3,omitempty"`
	Running    bool            `json:"running"`
	Error      string          `json:"error,omitempty"`
	Attached   []string        `json:"attached"`
	Endpoints  []string        `json:"endpoints"`
}

func toOutpostView(s outpost.Status) (outpostView, error) {
	view := outpostView{
		Name:       s.Name,
		Type:       s.Type,
		ListenAddr: s.ListenAddr,
		Guest:      s.Guest,
		ValidUsers: s.ValidUsers,
		ForceUser:  s.ForceUser,
		HostsAllow: s.HostsAllow,
		Browseable: s.Browseable,
		Running:    s.Running,
		Error:      s.Error,
		Attached:   s.Attached,
		Endpoints:  s.Endpoints,
	}
	if s.S3 != nil {
		data, err := json.Marshal(s.S3)
		if err != nil {
			return outpostView{}, err
		}
		view.S3 = data
	}
	return view, nil
}

func writeOutpostView(w http.ResponseWriter, s outpost.Status) {
	view, err := toOutpostView(s)
	if err != nil {
		respond.WriteErrorResponse(w, err)
		return
	}
	writeExtJS(w, view)
}

func outpostFormValues(r *http.Request) (outpost.Outpost, error) {
	o := outpost.Outpost{
		Name:       strings.TrimSpace(r.FormValue("name")),
		Type:       strings.TrimSpace(r.FormValue("type")),
		ListenAddr: strings.TrimSpace(r.FormValue("listen-addr")),
		Guest:      r.FormValue("guest") == "1" || r.FormValue("guest") == "true",
		ValidUsers: strings.TrimSpace(r.FormValue("valid-users")),
		ForceUser:  strings.TrimSpace(r.FormValue("force-user")),
		HostsAllow: strings.TrimSpace(r.FormValue("hosts-allow")),
		Browseable: r.FormValue("browseable") == "1" || r.FormValue("browseable") == "true",
	}
	if s3 := strings.TrimSpace(r.FormValue("s3")); s3 != "" {
		config := &objectstore.Config{}
		if err := json.Unmarshal([]byte(s3), config); err != nil {
			return outpost.Outpost{}, fmt.Errorf("invalid s3 config: %w", err)
		}
		o.S3 = config
	}
	return o, nil
}

func writeOutpostInvalid(w http.ResponseWriter, err error) {
	respond.Error(w, http.StatusBadRequest, err)
}

func ExtJsOutpostsHandler(app *application.Runtime) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet && r.Method != http.MethodPost {
			respond.MethodNotAllowed(w, r)
			return
		}
		switch r.Method {
		case http.MethodGet:
			statuses := outpost.StatusAll()
			views := make([]outpostView, 0, len(statuses))
			for _, s := range statuses {
				view, err := toOutpostView(s)
				if err != nil {
					respond.WriteErrorResponse(w, err)
					return
				}
				views = append(views, view)
			}
			writeExtJS(w, views)
		case http.MethodPost:
			if err := r.ParseForm(); err != nil {
				respond.WriteErrorResponse(w, err)
				return
			}
			o, err := outpostFormValues(r)
			if err != nil {
				writeOutpostInvalid(w, err)
				return
			}
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
			writeOutpostView(w, outpost.Status{Outpost: o, Running: true})
		}
	}
}

func ExtJsOutpostSingleHandler(app *application.Runtime) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet && r.Method != http.MethodPut && r.Method != http.MethodDelete {
			respond.MethodNotAllowed(w, r)
			return
		}
		name := r.PathValue("name")
		existing, ok, err := outpost.LoadOutpost(name)
		if err != nil {
			respond.WriteErrorResponse(w, err)
			return
		}
		if !ok {
			respond.NotFound(w, "no such outpost: %s", name)
			return
		}
		switch r.Method {
		case http.MethodGet:
			for _, s := range outpost.StatusAll() {
				if s.Name == name {
					writeOutpostView(w, s)
					return
				}
			}
			writeOutpostView(w, outpost.Status{Outpost: existing})
		case http.MethodPut:
			if err := r.ParseForm(); err != nil {
				respond.WriteErrorResponse(w, err)
				return
			}
			o, err := outpostFormValues(r)
			if err != nil {
				writeOutpostInvalid(w, err)
				return
			}
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
			snapshotmount.ReattachOutpost(r.Context(), o.Name)
			for _, s := range outpost.StatusAll() {
				if s.Name == o.Name {
					writeOutpostView(w, s)
					return
				}
			}
			writeOutpostView(w, outpost.Status{Outpost: o, Running: true})
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
