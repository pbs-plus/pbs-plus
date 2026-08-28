//go:build linux

package mountapi

import (
	"fmt"
	"net/http"
	"strings"
	"time"

	"github.com/pbs-plus/pbs-plus/internal/server/application"
	"github.com/pbs-plus/pbs-plus/internal/server/jobs"
	"github.com/pbs-plus/pbs-plus/internal/server/snapshotmount"
	"github.com/pbs-plus/pbs-plus/internal/server/web/api/respond"
)

type profileView struct {
	ID         string `json:"id"`
	Datastore  string `json:"datastore"`
	Namespace  string `json:"namespace"`
	BackupType string `json:"backup-type"`
	BackupID   string `json:"backup-id"`
	Mode       string `json:"mode"`
	MountPath  string `json:"mount-path"`
	Schedule   string `json:"schedule"`
	AutoMount  bool   `json:"auto-mount"`
}

func toProfileView(p snapshotmount.Profile) profileView {
	return profileView{
		ID:         p.ID(),
		Datastore:  p.Datastore,
		Namespace:  p.Namespace,
		BackupType: p.BackupType,
		BackupID:   p.BackupID,
		Mode:       p.Mode,
		MountPath:  p.MountPath,
		Schedule:   p.Schedule,
		AutoMount:  p.AutoMount,
	}
}

func profileFormValues(r *http.Request) snapshotmount.Profile {
	mode := strings.TrimSpace(r.FormValue("mode"))
	if mode == "" {
		mode = snapshotmount.ModeRO
	}
	return snapshotmount.Profile{
		Datastore:  strings.TrimSpace(r.FormValue("datastore")),
		Namespace:  strings.TrimSpace(r.FormValue("ns")),
		BackupType: strings.TrimSpace(r.FormValue("backup-type")),
		BackupID:   strings.TrimSpace(r.FormValue("backup-id")),
		Mode:       mode,
		MountPath:  strings.TrimSpace(r.FormValue("mount-path")),
		Schedule:   strings.TrimSpace(r.FormValue("schedule")),
		AutoMount:  r.FormValue("auto-mount") == "1" || r.FormValue("auto-mount") == "true",
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
			respond.WriteErrorResponse(w, err)
			return
		}
		if _, exists, err := snapshotmount.LoadProfile(p.ID()); err != nil {
			respond.WriteErrorResponse(w, err)
			return
		} else if exists {
			respond.WriteErrorResponse(w, fmt.Errorf("profile for %s/%s already exists", p.BackupType, p.BackupID))
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
			if p.Datastore == "" || p.BackupType == "" || p.BackupID == "" {
				p.Datastore = existing.Datastore
				p.Namespace = existing.Namespace
				p.BackupType = existing.BackupType
				p.BackupID = existing.BackupID
			}
			if err := snapshotmount.ValidateProfile(p); err != nil {
				respond.WriteErrorResponse(w, err)
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
		backupTime, fileName, err := snapshotmount.LatestSnapshot(p)
		if err != nil {
			respond.WriteErrorResponse(w, err)
			return
		}
		parsed, err := time.Parse(time.RFC3339, backupTime)
		if err != nil {
			respond.WriteErrorResponse(w, err)
			return
		}
		key := snapshotmount.Key(p.Datastore, p.Namespace, p.BackupType, p.BackupID, snapshotmount.DirTime(parsed))
		task, err := newTask("mount", p.Datastore, key)
		if err != nil {
			respond.WriteErrorResponse(w, err)
			return
		}
		upid, ok2 := submitSnapshotWorkflow(w, r, app, jobs.WorkflowSnapshotMount, key, "snapshot-mount:"+key, jobs.SnapshotMountInput{
			Datastore:  p.Datastore,
			Namespace:  p.Namespace,
			BackupType: p.BackupType,
			BackupID:   p.BackupID,
			BackupTime: backupTime,
			FileName:   fileName,
			Mode:       p.Mode,
			MountPath:  p.MountPath,
			UPID:       upidTask(task),
			Web:        true,
		})
		if !ok2 {
			task.CloseErr(fmt.Errorf("workflow submit failed"))
			return
		}
		writeExtJS(w, upid)
	}
}
