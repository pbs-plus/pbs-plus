//go:build linux

package mountapi

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"time"

	"github.com/pbs-plus/pbs-plus/internal/log"
	"github.com/pbs-plus/pbs-plus/internal/server/application"
	"github.com/pbs-plus/pbs-plus/internal/server/snapshotmount"
	"github.com/pbs-plus/pbs-plus/internal/server/web/api/respond"
)

type profileView struct {
	ID        string `json:"id"`
	Datastore string `json:"datastore"`
	Namespace string `json:"namespace"`
	Mode      string `json:"mode"`
	Outpost   string `json:"outpost"`
	ShareName string `json:"share-name"`
	MountPath string `json:"mount-path"`
	Schedule  string `json:"schedule"`
	AutoMount bool   `json:"auto-mount"`
	Replace   bool   `json:"replace"`
}

func toProfileView(p snapshotmount.Profile) profileView {
	return profileView{
		ID:        p.ID(),
		Datastore: p.Datastore,
		Namespace: p.Namespace,
		Mode:      p.Mode,
		Outpost:   p.Outpost,
		ShareName: p.ShareName,
		MountPath: p.MountPath,
		Schedule:  p.Schedule,
		AutoMount: p.AutoMount,
		Replace:   p.Replace,
	}
}

func profileFormValues(r *http.Request) snapshotmount.Profile {
	mode := strings.TrimSpace(r.FormValue("mode"))
	if mode == "" {
		mode = snapshotmount.ModeRO
	}
	return snapshotmount.Profile{
		Datastore: strings.TrimSpace(r.FormValue("datastore")),
		Namespace: strings.TrimSpace(r.FormValue("ns")),
		Mode:      mode,
		Outpost:   strings.TrimSpace(r.FormValue("outpost")),
		ShareName: strings.TrimSpace(r.FormValue("share-name")),
		MountPath: strings.TrimSpace(r.FormValue("mount-path")),
		Schedule:  strings.TrimSpace(r.FormValue("schedule")),
		AutoMount: r.FormValue("auto-mount") == "1" || r.FormValue("auto-mount") == "true",
		Replace:   r.FormValue("replace") == "1" || r.FormValue("replace") == "true",
	}
}

func writeProfileInvalid(w http.ResponseWriter, err error) {
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

func ExtJsMountProfilesHandler(app *application.Runtime) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet && r.Method != http.MethodPost {
			http.Error(w, "Invalid HTTP method", http.StatusBadRequest)
			return
		}
		if r.Method == http.MethodGet {
			profiles, err := snapshotmount.ListProfiles()
			if err != nil {
				respond.WriteErrorResponse(w, err)
				return
			}
			views := make([]profileView, 0, len(profiles))
			for _, p := range profiles {
				views = append(views, toProfileView(p))
			}
			writeExtJS(w, views)
			return
		}
		if err := r.ParseForm(); err != nil {
			respond.WriteErrorResponse(w, err)
			return
		}
		p := profileFormValues(r)
		if err := snapshotmount.ValidateProfile(p); err != nil {
			writeProfileInvalid(w, err)
			return
		}
		if _, exists, err := snapshotmount.LoadProfile(p.ID()); err != nil {
			respond.WriteErrorResponse(w, err)
			return
		} else if exists {
			respond.WriteErrorResponse(w, fmt.Errorf("batch profile for %s already exists", p.ID()))
			return
		}
		p.CreatedAt = time.Now().Unix()
		p.UpdatedAt = p.CreatedAt
		if err := snapshotmount.SaveProfile(p); err != nil {
			respond.WriteErrorResponse(w, err)
			return
		}
		writeExtJS(w, toProfileView(p))
	}
}

func ExtJsMountProfileSingleHandler(app *application.Runtime) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPut && r.Method != http.MethodDelete && r.Method != http.MethodGet {
			http.Error(w, "Invalid HTTP method", http.StatusBadRequest)
			return
		}
		id := r.PathValue("id")
		existing, ok, err := snapshotmount.LoadProfile(id)
		if err != nil {
			respond.WriteErrorResponse(w, err)
			return
		}
		if !ok {
			respond.WriteErrorResponse(w, fmt.Errorf("no such profile"))
			return
		}
		switch r.Method {
		case http.MethodGet:
			writeExtJS(w, toProfileView(existing))
		case http.MethodPut:
			if err := r.ParseForm(); err != nil {
				respond.WriteErrorResponse(w, err)
				return
			}
			p := profileFormValues(r)
			if _, ok := r.Form["datastore"]; !ok {
				p.Datastore = existing.Datastore
				p.Namespace = existing.Namespace
			}
			if _, ok := r.Form["outpost"]; !ok {
				p.Outpost = existing.Outpost
			}
			if _, ok := r.Form["share-name"]; !ok {
				p.ShareName = existing.ShareName
			}
			if _, ok := r.Form["mount-path"]; !ok {
				p.MountPath = existing.MountPath
			}
			if err := snapshotmount.ValidateProfile(p); err != nil {
				writeProfileInvalid(w, err)
				return
			}
			p.CreatedAt = existing.CreatedAt
			p.UpdatedAt = time.Now().Unix()
			if err := snapshotmount.SaveProfile(p); err != nil {
				respond.WriteErrorResponse(w, err)
				return
			}
			writeExtJS(w, toProfileView(p))
		case http.MethodDelete:
			if err := snapshotmount.DeleteProfile(id); err != nil {
				respond.WriteErrorResponse(w, err)
				return
			}
			writeExtJS(w, toProfileView(existing))
		}
	}
}

func ExtJsMountProfileMountHandler(app *application.Runtime) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "Invalid HTTP method", http.StatusBadRequest)
			return
		}
		id := r.PathValue("id")
		p, ok, err := snapshotmount.LoadProfile(id)
		if err != nil {
			respond.WriteErrorResponse(w, err)
			return
		}
		if !ok {
			respond.WriteErrorResponse(w, fmt.Errorf("no such profile"))
			return
		}
		task, err := newTask("mount", p.Datastore, id)
		if err != nil {
			respond.WriteErrorResponse(w, err)
			return
		}
		go func() {
			snapshotmount.ReconcileProfileNow(context.WithoutCancel(r.Context()), app.Engine, p)
			task.LogString("reconciled batch profile " + id)
			task.CloseOK()
		}()
		writeExtJS(w, upidTask(task))
	}
}
