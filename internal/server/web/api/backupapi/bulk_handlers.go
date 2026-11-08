//go:build linux

package backupapi

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"strconv"

	"github.com/pbs-plus/pbs-plus/internal/log"
	"github.com/pbs-plus/pbs-plus/internal/server/application"
	"github.com/pbs-plus/pbs-plus/internal/server/coredb"
	"github.com/pbs-plus/pbs-plus/internal/server/web/api/respond"
	"github.com/pbs-plus/pbs-plus/internal/validate"
)

type backupChange func(*coredb.Backup)

type bulkBackupField func(value string) (backupChange, error)

func optionalInt(value string, fallback int) (int, error) {
	if value == "" {
		return fallback, nil
	}
	return strconv.Atoi(value)
}

var bulkBackupFields = map[string]bulkBackupField{
	"schedule": func(v string) (backupChange, error) {
		if v != "" {
			if err := validate.ValidateOnCalendar(v); err != nil {
				return nil, err
			}
		}
		return func(b *coredb.Backup) { b.Schedule = v }, nil
	},
	"store": func(v string) (backupChange, error) {
		if err := validate.ValidateDatastore(v); err != nil {
			return nil, err
		}
		return func(b *coredb.Backup) { b.Store = v }, nil
	},
	"ns": func(v string) (backupChange, error) {
		if err := validate.ValidateNamespace(v); err != nil {
			return nil, err
		}
		return func(b *coredb.Backup) { b.Namespace = v }, nil
	},
	"retry": func(v string) (backupChange, error) {
		n, err := optionalInt(v, 0)
		if err != nil || n < 0 {
			return nil, fmt.Errorf("invalid retry count %q", v)
		}
		return func(b *coredb.Backup) { b.Retry = n }, nil
	},
	"retry-interval": func(v string) (backupChange, error) {
		n, err := optionalInt(v, 1)
		if err != nil || n < 1 {
			return nil, fmt.Errorf("invalid retry interval %q", v)
		}
		return func(b *coredb.Backup) { b.RetryInterval = n }, nil
	},
	"pre_script": func(v string) (backupChange, error) {
		if err := validate.ValidateScriptPath("pre_script", v); err != nil {
			return nil, err
		}
		return func(b *coredb.Backup) { b.PreScript = v }, nil
	},
	"post_script": func(v string) (backupChange, error) {
		if err := validate.ValidateScriptPath("post_script", v); err != nil {
			return nil, err
		}
		return func(b *coredb.Backup) { b.PostScript = v }, nil
	},
	"comment": func(v string) (backupChange, error) {
		return func(b *coredb.Backup) { b.Comment = v }, nil
	},
	"notification-mode": func(v string) (backupChange, error) {
		return func(b *coredb.Backup) { b.NotificationMode = v }, nil
	},
}

func parseBulkBackupChanges(form url.Values) ([]backupChange, error) {
	values := map[string]string{}
	for name := range bulkBackupFields {
		if form.Has(name) {
			values[name] = form.Get(name)
		}
	}
	for _, name := range form["delete"] {
		if _, ok := bulkBackupFields[name]; !ok {
			return nil, fmt.Errorf("field %q cannot be bulk edited", name)
		}
		values[name] = ""
	}
	if len(values) == 0 {
		return nil, fmt.Errorf("no changes given")
	}
	changes := make([]backupChange, 0, len(values))
	for name, value := range values {
		change, err := bulkBackupFields[name](value)
		if err != nil {
			return nil, fmt.Errorf("%s: %w", name, err)
		}
		changes = append(changes, change)
	}
	return changes, nil
}

func ExtJsBackupBulkHandler(app *application.Runtime) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPut {
			respond.MethodNotAllowed(w, r)
			return
		}
		if err := r.ParseForm(); err != nil {
			respond.Error(w, http.StatusBadRequest, err)
			return
		}
		ids, err := validate.DecodeJobIDs(r.Form["job"])
		if err != nil {
			respond.Error(w, http.StatusBadRequest, err)
			return
		}
		if len(ids) == 0 {
			respond.BadRequest(w, "missing job parameter(s)")
			return
		}
		changes, err := parseBulkBackupChanges(r.Form)
		if err != nil {
			respond.Error(w, http.StatusBadRequest, err)
			return
		}

		err = app.Backup.UpdateBackups(ids, func(b *coredb.Backup) {
			for _, change := range changes {
				change(b)
			}
		})
		if err != nil {
			respond.WriteErrorResponse(w, err)
			return
		}

		w.Header().Set("Content-Type", "application/json")
		response := BackupConfigResponse{Status: http.StatusOK, Success: true, Data: map[string]int{"updated": len(ids)}}
		if err := json.NewEncoder(w).Encode(response); err != nil {
			log.Error(err, "")
		}
	}
}
