//go:build linux

package mountapi

import (
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"github.com/pbs-plus/pbs-plus/internal/conf"
	"github.com/pbs-plus/pbs-plus/internal/log"
	"github.com/pbs-plus/pbs-plus/internal/proxmox/tasklog"
	"github.com/pbs-plus/pbs-plus/internal/server/application"
	"github.com/pbs-plus/pbs-plus/internal/server/jobs"
	"github.com/pbs-plus/pbs-plus/internal/server/snapshotmount"
	"github.com/pbs-plus/pbs-plus/internal/server/systemd"
	"github.com/pbs-plus/pbs-plus/internal/server/web/api/backupapi"
	"github.com/pbs-plus/pbs-plus/internal/server/web/api/respond"
	"github.com/pbs-plus/pbs-plus/internal/validate"
)

type mountForm struct {
	Datastore  string
	Namespace  string
	BackupType string
	BackupID   string
	BackupTime string
	FileName   string
	Mode       string
	MountPath  string
	Force      bool
}

func parseMountForm(r *http.Request) (mountForm, error) {
	f := mountForm{
		Datastore:  validate.DecodePath(r.PathValue("datastore")),
		Namespace:  strings.TrimSpace(r.FormValue("ns")),
		BackupType: strings.TrimSpace(r.FormValue("backup-type")),
		BackupID:   strings.TrimSpace(r.FormValue("backup-id")),
		BackupTime: strings.TrimSpace(r.FormValue("backup-time")),
		FileName:   strings.TrimSpace(r.FormValue("file-name")),
		Mode:       strings.TrimSpace(r.FormValue("mode")),
		MountPath:  strings.TrimSpace(r.FormValue("mount-path")),
		Force:      r.FormValue("force") == "1" || r.FormValue("force") == "true",
	}
	if err := validate.ValidateDatastore(f.Datastore); err != nil {
		return f, fmt.Errorf("invalid datastore: %w", err)
	}
	if err := validate.ValidateNamespace(f.Namespace); err != nil {
		return f, err
	}
	if f.BackupTime != "" {
		if _, err := time.Parse(time.RFC3339, f.BackupTime); err != nil {
			return f, fmt.Errorf("invalid backup-time format: %w", err)
		}
	}
	if f.BackupType != "" || f.BackupID != "" || f.FileName != "" {
		if err := validate.ValidateBackupType(f.BackupType); err != nil {
			return f, err
		}
		if err := validate.ValidateBackupID(f.BackupID); err != nil {
			return f, err
		}
		if err := validate.ValidateFileName(f.FileName); err != nil {
			return f, err
		}
	}
	if f.Mode != "" && f.Mode != snapshotmount.ModeRO && f.Mode != snapshotmount.ModeRW {
		return f, fmt.Errorf("invalid mode %q", f.Mode)
	}
	if err := snapshotmount.ValidateMountPath(f.MountPath); err != nil {
		return f, err
	}
	return f, nil
}

func (f mountForm) hasBackupParams() bool {
	return f.BackupType != "" && f.BackupID != "" && f.BackupTime != "" && f.FileName != ""
}

func (f mountForm) safeTime() (string, error) {
	parsedTime, err := time.Parse(time.RFC3339, f.BackupTime)
	if err != nil {
		return "", err
	}
	return snapshotmount.DirTime(parsedTime), nil
}

func newTask(workerType, datastore, key string) (*tasklog.WorkerTask, error) {
	wid := tasklog.FormatWorkerID(datastore, workerType+"-", key)
	task, err := tasklog.NewWorkerTask("pbsplus", workerType, wid)
	if err == nil {
		task.LogString("queued " + workerType + " workflow")
	}
	return task, err
}

func submitSnapshotWorkflow(w http.ResponseWriter, r *http.Request, app *application.Runtime, kind, key, lockKey string, payload any, timeout time.Duration) (string, bool) {
	request, err := jobs.NewWorkflowSubmit(kind, key, "manual", "", payload, []string{lockKey}, 1, timeout)
	if err != nil {
		respond.WriteErrorResponse(w, err)
		return "", false
	}
	execution, _, err := app.Engine.Submit(r.Context(), request)
	if err != nil {
		respond.WriteErrorResponse(w, err)
		return "", false
	}
	upid := ""
	var input struct {
		UPID string `json:"upid"`
	}
	if err := json.Unmarshal(execution.Payload, &input); err == nil {
		upid = input.UPID
	}
	return upid, true
}

func ExtJsMountHandler(app *application.Runtime) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "Invalid HTTP method", http.StatusBadRequest)
			return
		}
		if err := r.ParseForm(); err != nil {
			respond.WriteErrorResponse(w, err)
			return
		}
		f, err := parseMountForm(r)
		if err != nil {
			respond.WriteErrorResponse(w, err)
			return
		}
		safeTime, err := f.safeTime()
		if err != nil {
			respond.WriteErrorResponse(w, err)
			return
		}
		if !f.hasBackupParams() {
			http.Error(w, "Missing backup parameters", http.StatusBadRequest)
			return
		}
		key := snapshotmount.Key(f.Datastore, f.Namespace, f.BackupType, f.BackupID, safeTime)

		task, err := newTask("mount", f.Datastore, key)
		if err != nil {
			respond.WriteErrorResponse(w, err)
			return
		}
		upid, ok := submitSnapshotWorkflow(w, r, app, jobs.WorkflowSnapshotMount, key, "snapshot-mount:"+key, jobs.SnapshotMountInput{
			Datastore:  f.Datastore,
			Namespace:  f.Namespace,
			BackupType: f.BackupType,
			BackupID:   f.BackupID,
			BackupTime: f.BackupTime,
			FileName:   f.FileName,
			Mode:       f.Mode,
			MountPath:  f.MountPath,
			UPID:       upidTask(task),
			Web:        true,
		}, time.Minute)
		if !ok {
			task.CloseErr(fmt.Errorf("workflow submit failed"))
			return
		}
		writeRunResponse(w, upid)
	}
}

func upidTask(task *tasklog.WorkerTask) string {
	if task == nil {
		return ""
	}
	return task.UPID()
}

func ExtJsInitHandler(app *application.Runtime) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "Invalid HTTP method", http.StatusBadRequest)
			return
		}
		if err := r.ParseForm(); err != nil {
			respond.WriteErrorResponse(w, err)
			return
		}
		datastore := validate.DecodePath(r.PathValue("datastore"))
		if datastore == "" {
			http.Error(w, "Missing datastore", http.StatusBadRequest)
			return
		}
		in := jobs.SnapshotInitInput{
			Datastore:  datastore,
			Namespace:  strings.TrimSpace(r.FormValue("ns")),
			BackupType: strings.TrimSpace(r.FormValue("backup-type")),
			BackupID:   strings.TrimSpace(r.FormValue("backup-id")),
			MountPath:  strings.TrimSpace(r.FormValue("mount-path")),
			Web:        true,
		}
		if in.BackupType == "" || in.BackupID == "" {
			http.Error(w, "Missing backup parameters", http.StatusBadRequest)
			return
		}

		key := snapshotmount.Key(in.Datastore, in.Namespace, in.BackupType, in.BackupID, "init")
		task, err := newTask("init", in.Datastore, key)
		if err != nil {
			respond.WriteErrorResponse(w, err)
			return
		}
		in.UPID = upidTask(task)

		upid, ok := submitSnapshotWorkflow(w, r, app, jobs.WorkflowSnapshotInit, key, "snapshot-mount:"+key, in, time.Minute)
		if !ok {
			task.CloseErr(fmt.Errorf("workflow submit failed"))
			return
		}
		writeRunResponse(w, upid)
	}
}

func ExtJsUnmountHandler(app *application.Runtime) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "Invalid HTTP method", http.StatusBadRequest)
			return
		}
		if err := r.ParseForm(); err != nil {
			respond.WriteErrorResponse(w, err)
			return
		}
		f, err := parseMountForm(r)
		if err != nil {
			respond.WriteErrorResponse(w, err)
			return
		}

		key := ""
		if f.MountPath != "" {
			session, found, err := snapshotmount.FindSessionByMountPoint(f.MountPath)
			if err != nil {
				respond.WriteErrorResponse(w, err)
				return
			}
			if found {
				key = session.ServiceKey
			}
		} else {
			if !f.hasBackupParams() {
				http.Error(w, "Missing backup parameters or mount-path", http.StatusBadRequest)
				return
			}
			safeTime, err := f.safeTime()
			if err != nil {
				respond.WriteErrorResponse(w, err)
				return
			}
			key = snapshotmount.Key(f.Datastore, f.Namespace, f.BackupType, f.BackupID, safeTime)
		}

		task, err := newTask("unmount", f.Datastore, key)
		if err != nil {
			respond.WriteErrorResponse(w, err)
			return
		}
		lockKey := "snapshot-mount:" + key
		upid, ok := submitSnapshotWorkflow(w, r, app, jobs.WorkflowSnapshotUnmount, key, lockKey, jobs.SnapshotUnmountInput{
			Datastore:  f.Datastore,
			Namespace:  f.Namespace,
			BackupType: f.BackupType,
			BackupID:   f.BackupID,
			BackupTime: f.BackupTime,
			FileName:   f.FileName,
			MountPath:  f.MountPath,
			Force:      f.Force,
			UPID:       upidTask(task),
			Web:        true,
		}, time.Minute)
		if !ok {
			task.CloseErr(fmt.Errorf("workflow submit failed"))
			return
		}
		writeRunResponse(w, upid)
	}
}

func ExtJsCommitHandler(app *application.Runtime) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "Invalid HTTP method", http.StatusBadRequest)
			return
		}
		if err := r.ParseForm(); err != nil {
			respond.WriteErrorResponse(w, err)
			return
		}
		f, err := parseMountForm(r)
		if err != nil {
			respond.WriteErrorResponse(w, err)
			return
		}
		if f.MountPath == "" {
			http.Error(w, "Missing mount-path parameter", http.StatusBadRequest)
			return
		}
		session, found, err := snapshotmount.FindSessionByMountPoint(f.MountPath)
		if err != nil {
			respond.WriteErrorResponse(w, err)
			return
		}
		if !found {
			respond.WriteErrorResponse(w, fmt.Errorf("no mount session at %s", f.MountPath))
			return
		}
		if !session.CommitCapable() {
			respond.WriteErrorResponse(w, fmt.Errorf("mount at %s is not commit-capable (read-only or offline)", f.MountPath))
			return
		}
		key := session.ServiceKey

		task, err := newTask("commit", session.Datastore, key)
		if err != nil {
			respond.WriteErrorResponse(w, err)
			return
		}
		upid, ok := submitSnapshotWorkflow(w, r, app, jobs.WorkflowSnapshotCommit, key, "snapshot-mount:"+key, jobs.SnapshotCommitInput{
			Datastore: session.Datastore,
			MountPath: session.MountPoint,
			UPID:      upidTask(task),
			Web:       true,
		}, 10*time.Minute)
		if !ok {
			task.CloseErr(fmt.Errorf("workflow submit failed"))
			return
		}
		writeRunResponse(w, upid)
	}
}

func ExtJsMountsHandler(app *application.Runtime) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			http.Error(w, "Invalid HTTP method", http.StatusBadRequest)
			return
		}
		sessions, err := snapshotmount.ListSessions()
		if err != nil {
			respond.WriteErrorResponse(w, err)
			return
		}
		datastore := validate.DecodePath(r.PathValue("datastore"))
		type sessionView struct {
			Datastore     string `json:"datastore"`
			Namespace     string `json:"namespace"`
			BackupType    string `json:"backup-type"`
			BackupID      string `json:"backup-id"`
			BackupTime    string `json:"backup-time"`
			FileName      string `json:"file-name"`
			Mode          string `json:"mode"`
			MountPoint    string `json:"mount-point"`
			Mounted       bool   `json:"mounted"`
			CommitCapable bool   `json:"commit-capable"`
		}
		views := make([]sessionView, 0, len(sessions))
		for _, s := range sessions {
			if datastore != "" && s.Datastore != datastore {
				continue
			}
			views = append(views, sessionView{
				Datastore:     s.Datastore,
				Namespace:     s.Namespace,
				BackupType:    s.BackupType,
				BackupID:      s.BackupID,
				BackupTime:    s.BackupTime,
				FileName:      s.FileName,
				Mode:          s.Mode,
				MountPoint:    s.MountPoint,
				Mounted:       snapshotmount.IsMounted(s.MountPoint),
				CommitCapable: s.CommitCapable(),
			})
		}
		sort.Slice(views, func(i, j int) bool {
			if views[i].Datastore != views[j].Datastore {
				return views[i].Datastore < views[j].Datastore
			}
			return views[i].MountPoint < views[j].MountPoint
		})
		writeExtJS(w, views)
	}
}

func ExtJsUnmountAllHandler(app *application.Runtime) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "Invalid HTTP method", http.StatusBadRequest)
			return
		}
		if err := r.ParseForm(); err != nil {
			respond.WriteErrorResponse(w, err)
			return
		}
		datastore := validate.DecodePath(r.PathValue("datastore"))
		ns := strings.TrimSpace(r.FormValue("ns"))
		if err := validate.ValidateDatastore(datastore); err != nil {
			respond.WriteErrorResponse(w, fmt.Errorf("invalid datastore: %w", err))
			return
		}
		if err := validate.ValidateNamespace(ns); err != nil {
			respond.WriteErrorResponse(w, err)
			return
		}

		sessions, err := snapshotmount.ListSessions()
		if err != nil {
			log.Error(err, "")
		}
		for _, s := range sessions {
			if s.Datastore != datastore || (ns != "" && !strings.HasPrefix(s.Namespace, ns)) {
				continue
			}
			if err := systemd.StopMountService(r.Context(), s.ServiceName()); err != nil {
				log.Error(err, "")
			}
			if snapshotmount.IsMounted(s.MountPoint) {
				if err := snapshotmount.UnmountPath(s.MountPoint); err != nil {
					log.Error(err, "")
				}
			}
			if s.OverlayDir != "" {
				if err := os.RemoveAll(s.OverlayDir); err != nil {
					log.Error(err, "")
				}
			}
			if s.SocketPath != "" {
				for _, suffix := range []string{"", ".monitor", ".log"} {
					if err := os.Remove(s.SocketPath + suffix); err != nil && !os.IsNotExist(err) {
						log.Error(err, "")
					}
				}
			}
			if err := snapshotmount.DeleteSession(s.ServiceKey); err != nil {
				log.Error(err, "")
			}
		}

		base := filepath.Clean(filepath.Join(conf.RestoreMountBasePath, datastore))
		if ns != "" {
			base = filepath.Join(base, ns)
		}
		if err := validate.SanitizeMountPoint(base, conf.RestoreMountBasePath); err != nil {
			respond.WriteErrorResponse(w, err)
			return
		}

		services, err := systemd.ListMountServices(r.Context())
		if err != nil {
			respond.WriteErrorResponse(w, fmt.Errorf("list services: %w", err))
			return
		}
		prefix := "pbs-plus-snapshot-mount-"
		for _, svc := range services {
			if strings.HasPrefix(svc, prefix) {
				if err := systemd.StopMountService(r.Context(), svc); err != nil {
					log.Error(err, "")
				}
			}
		}

		allMPs, err := snapshotmount.ParseMountPoints()
		if err != nil {
			respond.WriteErrorResponse(w, fmt.Errorf("read mounts: %w", err))
			return
		}
		var targets []string
		for _, mp := range allMPs {
			clean := filepath.Clean(mp)
			if clean == base || validate.IsPathWithin(base, clean) {
				targets = append(targets, clean)
			}
		}
		sort.Slice(targets, func(i, j int) bool {
			di := strings.Count(targets[i], string(filepath.Separator))
			dj := strings.Count(targets[j], string(filepath.Separator))
			if di == dj {
				return len(targets[i]) > len(targets[j])
			}
			return di > dj
		})
		for _, mp := range targets {
			if snapshotmount.IsMounted(mp) {
				if err := snapshotmount.UnmountPath(mp); err != nil {
					log.Error(err, "")
				}
			}
		}

		if err := os.RemoveAll(base); err != nil && !os.IsNotExist(err) {
			log.Error(err, "")
		}

		writeJSON(w, backupapi.BackupRunResponse{
			Success: true,
			Status:  http.StatusOK,
			Message: "unmounted all within datastore",
		})
	}
}

func writeRunResponse(w http.ResponseWriter, upid string) {
	w.Header().Set("Content-Type", "application/json")
	response := struct {
		Success bool   `json:"success"`
		Status  int    `json:"status"`
		Data    string `json:"data"`
	}{Success: true, Status: http.StatusOK, Data: upid}
	if err := json.NewEncoder(w).Encode(response); err != nil {
		log.Error(err, "")
	}
}

func writeJSON(w http.ResponseWriter, v any) {
	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(v); err != nil {
		log.Error(err, "")
	}
}

func writeExtJS(w http.ResponseWriter, data any) {
	writeJSON(w, struct {
		Status  int  `json:"status"`
		Success bool `json:"success"`
		Data    any  `json:"data"`
	}{Status: http.StatusOK, Success: true, Data: data})
}
