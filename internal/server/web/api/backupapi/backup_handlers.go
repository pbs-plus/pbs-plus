//go:build linux

package backupapi

import (
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"net/rpc"
	"strconv"
	"strings"
	"time"

	"github.com/pbs-plus/pbs-plus/internal/server/web/api/digest"
	"github.com/pbs-plus/pbs-plus/internal/server/web/api/extjs"
	"github.com/pbs-plus/pbs-plus/internal/server/web/api/notificationapi"
	"github.com/pbs-plus/pbs-plus/internal/server/web/api/respond"

	"github.com/pbs-plus/pbs-plus/internal/conf"
	"github.com/pbs-plus/pbs-plus/internal/log"
	"github.com/pbs-plus/pbs-plus/internal/server/application"
	"github.com/pbs-plus/pbs-plus/internal/server/coredb"
	"github.com/pbs-plus/pbs-plus/internal/server/rpc/jobrpc"
	"github.com/pbs-plus/pbs-plus/internal/validate"
)

func D2DBackupHandler(app *application.Runtime) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			http.Error(w, "Invalid HTTP method", http.StatusBadRequest)
			return
		}

		allBackups, err := app.Backup.ListBackups()
		if err != nil {
			respond.WriteErrorResponse(w, err)
			return
		}

		// Resolve stale-backup alert settings for highlighting
		var staleDays int
		var skipUnscheduled bool
		var excludedJobs map[string]bool
		if setting, err := app.CoreDB.GetAlertSetting("stale-backup"); err == nil {
			staleDays = setting.Threshold
			skipUnscheduled = setting.SkipUnscheduled
			if staleDays <= 0 {
				staleDays = 7
			}
			excluded, err := app.CoreDB.GetExcludedValues("stale-backup", "job")
			if err != nil {
				log.Error(err, "")
			}
			excludedJobs = excluded
		}

		flatBackups := extjs.FlattenBackups(allBackups, staleDays, skipUnscheduled, excludedJobs)

		digest, err := digest.Calculate(flatBackups)
		if err != nil {
			respond.WriteErrorResponse(w, err)
			return
		}

		toReturn := map[string]any{
			"data":    flatBackups,
			"digest":  digest,
			"success": true,
		}

		w.Header().Set("Content-Type", "application/json")
		if err := json.NewEncoder(w).Encode(toReturn); err != nil {
			log.Error(err, "")
		}
	}
}

func ExtJsBackupRunHandler(app *application.Runtime) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost && r.Method != http.MethodDelete {
			http.Error(w, "Invalid HTTP method", http.StatusBadRequest)
			return
		}

		var response BackupRunResponse

		backupIDs := r.URL.Query()["job"]
		if len(backupIDs) == 0 {
			http.Error(w, "Missing job parameter(s)", http.StatusBadRequest)
			return
		}

		decodedBackupIDs := []string{}
		for _, backupID := range backupIDs {
			decoded := validate.DecodePath(backupID)
			if err := validate.ValidateJobId(decoded); err != nil {
				respond.WriteErrorResponse(w, err)
				return
			}
			decodedBackupIDs = append(decodedBackupIDs, decoded)
		}

		stop := r.Method == http.MethodDelete
		response.Errors = map[string]string{}
		var messages []string

		conn, err := net.DialTimeout("unix", conf.JobMutateSocketPath, 10*time.Second)
		if err != nil {
			log.Error(err, "", "backups", decodedBackupIDs)
			respond.WriteErrorResponse(w, err)
			return
		}
		rpcClient := rpc.NewClient(conn)
		defer func() {
			if err := rpcClient.Close(); err != nil {
				log.Error(err, "")
			}
		}()

		for _, backupID := range decodedBackupIDs {
			backupTask, err := app.CoreDB.GetBackup(backupID)
			if err != nil {
				log.Error(err, "", "backupID", backupID)
				response.Errors[backupID] = err.Error()
				messages = append(messages, backupID+": "+err.Error())
				continue
			}

			args := &jobrpc.BackupQueueArgs{
				Job:             backupTask,
				SkipCheck:       true,
				Stop:            stop,
				Web:             true,
				ExtraExclusions: nil,
			}
			var reply jobrpc.QueueReply
			if err := rpcClient.Call(jobrpc.ServiceName+".BackupQueue", args, &reply); err != nil {
				log.Error(err, "", "backupID", backupID)
				response.Errors[backupID] = err.Error()
				messages = append(messages, backupID+": "+err.Error())
				continue
			}
			if reply.Status != 200 {
				log.Error(fmt.Errorf("%s", reply.Message), "", "backupID", backupID)
				response.Errors[backupID] = reply.Message
				messages = append(messages, backupID+": "+reply.Message)
			}
		}

		w.Header().Set("Content-Type", "application/json")
		response.Status = http.StatusOK
		response.Success = len(response.Errors) == 0
		if !response.Success {
			response.Status = http.StatusConflict
			response.Message = strings.Join(messages, "; ")
		}
		if err := json.NewEncoder(w).Encode(response); err != nil {
			log.Error(err, "")
		}
	}
}

func parseExpandLimit(raw, name string, fallback int) (int, error) {
	if raw == "" {
		return fallback, nil
	}
	value, err := strconv.Atoi(raw)
	if err != nil || value < -1 {
		return 0, fmt.Errorf("%s must be -1 or greater", name)
	}
	return value, nil
}

func ExtJsBackupHandler(app *application.Runtime) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		response := BackupConfigResponse{}
		if r.Method != http.MethodPost {
			http.Error(w, "Invalid HTTP method", http.StatusBadRequest)
			return
		}

		w.Header().Set("Content-Type", "application/json")

		err := r.ParseForm()
		if err != nil {
			respond.WriteErrorResponse(w, err)
			return
		}

		retry, err := strconv.Atoi(r.FormValue("retry"))
		if err != nil {
			if r.FormValue("retry") == "" {
				retry = 0
			} else {
				respond.WriteErrorResponse(w, err)
				return
			}
		}

		retryInterval, err := strconv.Atoi(r.FormValue("retry-interval"))
		if err != nil {
			if r.FormValue("retry-interval") == "" {
				retryInterval = 1
			} else {
				respond.WriteErrorResponse(w, err)
				return
			}
		}

		maxDirEntries, err := strconv.Atoi(r.FormValue("max-dir-entries"))
		if err != nil {
			if r.FormValue("max-dir-entries") == "" {
				maxDirEntries = 1048576
			} else {
				respond.WriteErrorResponse(w, err)
				return
			}
		}

		includeXattr, err := strconv.ParseBool(r.FormValue("include-xattr"))
		if err != nil {
			includeXattr = true
		}

		legacyXattr, err := strconv.ParseBool(r.FormValue("legacy-xattr"))
		if err != nil {
			legacyXattr = false
		}

		expandArchives, err := strconv.ParseBool(r.FormValue("expand-archives"))
		if err != nil {
			expandArchives = false
		}
		expandZip, err := strconv.ParseBool(r.FormValue("expand-zip"))
		if err != nil {
			expandZip = true
		}
		expandSevenZip, err := strconv.ParseBool(r.FormValue("expand-7z"))
		if err != nil {
			expandSevenZip = true
		}

		expandMaxDepth, err := parseExpandLimit(r.FormValue("expand-max-depth"), "expand-max-depth", 8)
		if err != nil {
			respond.WriteErrorResponse(w, err)
			return
		}
		expandMaxEntries, err := parseExpandLimit(r.FormValue("expand-max-entries"), "expand-max-entries", 0)
		if err != nil {
			respond.WriteErrorResponse(w, err)
			return
		}

		id := r.FormValue("id")
		err = validate.ValidateJobId(id)
		if err != nil && id != "" {
			respond.WriteErrorResponse(w, err)
			return
		}

		namespace := r.FormValue("ns")
		err = validate.ValidateNamespace(namespace)
		if err != nil {
			respond.WriteErrorResponse(w, err)
			return
		}

		datastore := r.FormValue("store")
		err = validate.ValidateDatastore(datastore)
		if err != nil {
			respond.WriteErrorResponse(w, err)
			return
		}

		subpath := r.FormValue("subpath")
		err = validate.ValidateSubpath("subpath", subpath)
		if err != nil {
			respond.WriteErrorResponse(w, err)
			return
		}

		preScript := r.FormValue("pre_script")
		err = validate.ValidateScriptPath("pre_script", preScript)
		if err != nil {
			respond.WriteErrorResponse(w, err)
			return
		}

		postScript := r.FormValue("post_script")
		err = validate.ValidateScriptPath("post_script", postScript)
		if err != nil {
			respond.WriteErrorResponse(w, err)
			return
		}

		newBackup := coredb.Backup{
			ID:               id,
			Store:            datastore,
			SourceMode:       r.FormValue("sourcemode"),
			ReadMode:         r.FormValue("readmode"),
			Mode:             r.FormValue("mode"),
			Target:           coredb.Target{Name: r.FormValue("target")},
			Subpath:          subpath,
			Schedule:         r.FormValue("schedule"),
			Comment:          r.FormValue("comment"),
			Namespace:        namespace,
			MaxDirEntries:    maxDirEntries,
			NotificationMode: r.FormValue("notification-mode"),
			Retry:            retry,
			RetryInterval:    retryInterval,
			Exclusions:       []coredb.Exclusion{},
			PreScript:        preScript,
			PostScript:       postScript,
			IncludeXattr:     includeXattr,
			LegacyXattr:      legacyXattr,
			ExpandArchives:   expandArchives,
			ExpandZip:        expandZip,
			ExpandSevenZip:   expandSevenZip,
			ExpandMaxDepth:   expandMaxDepth,
			ExpandMaxEntries: expandMaxEntries,
			DatabaseScope:    r.FormValue("database_scope"),
			DatabaseName:     r.FormValue("database_name"),
			DovecotUsername:  r.FormValue("dovecot_username"),
			DovecotMailbox:   r.FormValue("dovecot_mailbox"),
		}

		rawExclusions := r.FormValue("rawexclusions")
		for exclusion := range strings.SplitSeq(rawExclusions, "\n") {
			exclusion = strings.TrimSpace(exclusion)
			if exclusion == "" {
				continue
			}

			if err := validate.ValidateExclusionPath(exclusion); err != nil {
				respond.WriteErrorResponse(w, err)
				return
			}

			exclusionInst := coredb.Exclusion{
				Path:  exclusion,
				JobID: newBackup.ID,
			}

			newBackup.Exclusions = append(newBackup.Exclusions, exclusionInst)
		}

		err = app.Backup.CreateBackup(newBackup)
		if err != nil {
			respond.WriteErrorResponse(w, err)
			return
		}

		notificationapi.ApplyJobBatchAssignment(app, "backup", newBackup.ID, r.FormValue("notification-batch"))

		response.Status = http.StatusOK
		response.Success = true
		if err := json.NewEncoder(w).Encode(response); err != nil {
			log.Error(err, "")
		}
	}
}

func ExtJsBackupSingleHandler(app *application.Runtime) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		response := BackupConfigResponse{}
		if r.Method != http.MethodPut && r.Method != http.MethodGet && r.Method != http.MethodDelete {
			http.Error(w, "Invalid HTTP method", http.StatusBadRequest)
			return
		}

		w.Header().Set("Content-Type", "application/json")

		if r.Method == http.MethodPut {
			backupID := validate.DecodePath(r.PathValue("backup"))
			if err := validate.ValidateJobId(backupID); err != nil {
				respond.WriteErrorResponse(w, err)
				return
			}

			backup, err := app.Backup.GetBackup(backupID)
			if err != nil {
				respond.WriteErrorResponse(w, err)
				return
			}

			err = r.ParseForm()
			if err != nil {
				respond.WriteErrorResponse(w, err)
				return
			}

			if r.FormValue("store") != "" {
				if err := validate.ValidateDatastore(r.FormValue("store")); err != nil {
					respond.WriteErrorResponse(w, err)
					return
				}
				backup.Store = r.FormValue("store")
			}
			if r.FormValue("mode") != "" {
				backup.Mode = r.FormValue("mode")
			}
			if r.FormValue("sourcemode") != "" {
				backup.SourceMode = r.FormValue("sourcemode")
			}
			if r.FormValue("readmode") != "" {
				backup.ReadMode = r.FormValue("readmode")
			}
			if r.FormValue("target") != "" {
				backup.Target.Name = r.FormValue("target")
			}
			if r.FormValue("schedule") != "" {
				backup.Schedule = r.FormValue("schedule")
			}
			if r.FormValue("comment") != "" {
				backup.Comment = r.FormValue("comment")
			}
			if r.FormValue("notification-mode") != "" {
				backup.NotificationMode = r.FormValue("notification-mode")
			}
			if r.Form.Has("database_scope") {
				backup.DatabaseScope = r.FormValue("database_scope")
			}
			if r.Form.Has("database_name") {
				backup.DatabaseName = r.FormValue("database_name")
			}
			if r.Form.Has("dovecot_username") {
				backup.DovecotUsername = r.FormValue("dovecot_username")
			}
			if r.Form.Has("dovecot_mailbox") {
				backup.DovecotMailbox = r.FormValue("dovecot_mailbox")
			}

			if r.FormValue("include-xattr") != "" {
				includeXattr, err := strconv.ParseBool(r.FormValue("include-xattr"))
				if err != nil {
					includeXattr = true
				}
				backup.IncludeXattr = includeXattr
			}

			if r.FormValue("legacy-xattr") != "" {
				legacyXattr, err := strconv.ParseBool(r.FormValue("legacy-xattr"))
				if err != nil {
					legacyXattr = false
				}
				backup.LegacyXattr = legacyXattr
			}

			if r.FormValue("expand-archives") != "" {
				expandArchives, err := strconv.ParseBool(r.FormValue("expand-archives"))
				if err != nil {
					expandArchives = false
				}
				backup.ExpandArchives = expandArchives
			}

			if r.FormValue("expand-zip") != "" {
				expandZip, err := strconv.ParseBool(r.FormValue("expand-zip"))
				if err != nil {
					respond.WriteErrorResponse(w, err)
					return
				}
				backup.ExpandZip = expandZip
			}
			if r.FormValue("expand-7z") != "" {
				expandSevenZip, err := strconv.ParseBool(r.FormValue("expand-7z"))
				if err != nil {
					respond.WriteErrorResponse(w, err)
					return
				}
				backup.ExpandSevenZip = expandSevenZip
			}

			if r.FormValue("expand-max-depth") != "" {
				expandMaxDepth, err := parseExpandLimit(r.FormValue("expand-max-depth"), "expand-max-depth", 8)
				if err != nil {
					respond.WriteErrorResponse(w, err)
					return
				}
				backup.ExpandMaxDepth = expandMaxDepth
			}
			if r.FormValue("expand-max-entries") != "" {
				expandMaxEntries, err := parseExpandLimit(r.FormValue("expand-max-entries"), "expand-max-entries", 0)
				if err != nil {
					respond.WriteErrorResponse(w, err)
					return
				}
				backup.ExpandMaxEntries = expandMaxEntries
			}

			preScript := r.FormValue("pre_script")
			if err := validate.ValidateScriptPath("pre_script", preScript); err != nil {
				respond.WriteErrorResponse(w, err)
				return
			}
			backup.PreScript = preScript

			postScript := r.FormValue("post_script")
			if err := validate.ValidateScriptPath("post_script", postScript); err != nil {
				respond.WriteErrorResponse(w, err)
				return
			}
			backup.PostScript = postScript

			retry, err := strconv.Atoi(r.FormValue("retry"))
			if err != nil {
				retry = 0
			}

			retryInterval, err := strconv.Atoi(r.FormValue("retry-interval"))
			if err != nil {
				retryInterval = 1
			}

			maxDirEntries, err := strconv.Atoi(r.FormValue("max-dir-entries"))
			if err != nil {
				maxDirEntries = 1048576
			}

			namespace := r.FormValue("ns")
			err = validate.ValidateNamespace(namespace)
			if err != nil {
				respond.WriteErrorResponse(w, err)
				return
			}

			backup.Retry = retry
			backup.RetryInterval = retryInterval
			backup.MaxDirEntries = maxDirEntries

			subpath := r.FormValue("subpath")
			if err := validate.ValidateSubpath("subpath", subpath); err != nil {
				respond.WriteErrorResponse(w, err)
				return
			}
			backup.Subpath = subpath
			backup.Namespace = namespace
			backup.Exclusions = []coredb.Exclusion{}

			if r.FormValue("rawexclusions") != "" {
				rawExclusions := r.FormValue("rawexclusions")
				for exclusion := range strings.SplitSeq(rawExclusions, "\n") {
					exclusion = strings.TrimSpace(exclusion)
					if exclusion == "" {
						continue
					}

					if err := validate.ValidateExclusionPath(exclusion); err != nil {
						respond.WriteErrorResponse(w, err)
						return
					}

					exclusionInst := coredb.Exclusion{
						Path:  exclusion,
						JobID: backup.ID,
					}

					backup.Exclusions = append(backup.Exclusions, exclusionInst)
				}
			}

			if delArr, ok := r.Form["delete"]; ok {
				for _, attr := range delArr {
					switch attr {
					case "store":
						backup.Store = ""
					case "mode":
						backup.Mode = ""
					case "sourcemode":
						backup.SourceMode = ""
					case "readmode":
						backup.ReadMode = ""
					case "target":
						backup.Target.Name = ""
					case "subpath":
						backup.Subpath = ""
					case "schedule":
						backup.Schedule = ""
					case "comment":
						backup.Comment = ""
					case "ns":
						backup.Namespace = ""
					case "retry":
						backup.Retry = 0
					case "retry-interval":
						backup.RetryInterval = 1
					case "max-dir-entries":
						backup.MaxDirEntries = 1048576
					case "notification-mode":
						backup.NotificationMode = ""
					case "pre_script":
						backup.PreScript = ""
					case "post_script":
						backup.PostScript = ""
					case "rawexclusions":
						backup.Exclusions = []coredb.Exclusion{}
					case "database_scope":
						backup.DatabaseScope = ""
					case "database_name":
						backup.DatabaseName = ""
					case "dovecot_username":
						backup.DovecotUsername = ""
					case "dovecot_mailbox":
						backup.DovecotMailbox = ""
					}
				}
			}

			err = app.Backup.UpdateBackup(backup)
			if err != nil {
				respond.WriteErrorResponse(w, err)
				return
			}

			notificationapi.ApplyJobBatchAssignment(app, "backup", backup.ID, r.FormValue("notification-batch"))

			response.Status = http.StatusOK
			response.Success = true
			if err := json.NewEncoder(w).Encode(response); err != nil {
				log.Error(err, "")
			}

			return
		}

		if r.Method == http.MethodGet {
			backupID := validate.DecodePath(r.PathValue("backup"))
			if err := validate.ValidateJobId(backupID); err != nil {
				respond.WriteErrorResponse(w, err)
				return
			}

			backup, err := app.Backup.GetBackup(backupID)
			if err != nil {
				respond.WriteErrorResponse(w, err)
				return
			}

			response.Status = http.StatusOK
			response.Success = true
			flat := extjs.FlattenBackupForEdit(backup)
			flat["notification-batch"] = notificationapi.GetJobBatchName(app, "backup", backup.ID)
			response.Data = flat
			if err := json.NewEncoder(w).Encode(response); err != nil {
				log.Error(err, "")
			}

			return
		}

		if r.Method == http.MethodDelete {
			backupID := validate.DecodePath(r.PathValue("backup"))
			if err := validate.ValidateJobId(backupID); err != nil {
				respond.WriteErrorResponse(w, err)
				return
			}

			err := app.Backup.DeleteBackup(backupID)
			if err != nil {
				respond.WriteErrorResponse(w, err)
				return
			}

			response.Status = http.StatusOK
			response.Success = true
			if err := json.NewEncoder(w).Encode(response); err != nil {
				log.Error(err, "")
			}
			return
		}
	}
}

func ExtJsBackupUPIDsHandler(app *application.Runtime) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		response := BackupUPIDsResponse{}
		if r.Method != http.MethodGet {
			http.Error(w, "Invalid HTTP method", http.StatusBadRequest)
			return
		}

		w.Header().Set("Content-Type", "application/json")

		if r.Method == http.MethodGet {
			backupID := validate.DecodePath(r.PathValue("backup"))
			if err := validate.ValidateJobId(backupID); err != nil {
				respond.WriteErrorResponse(w, err)
				return
			}

			backup, err := app.Backup.GetBackup(backupID)
			if err != nil {
				respond.WriteErrorResponse(w, err)
				return
			}

			response.Status = http.StatusOK
			response.Success = true
			tasks := backup.GetAllUPIDs()
			data := make([]extjs.TasksWithStatus, len(tasks))
			for i, t := range tasks {
				data[i] = extjs.TasksWithStatus{Tasks: t, StatusParsed: extjs.ParseTaskStatus(t.Status)}
			}
			response.Data = data
			if err := json.NewEncoder(w).Encode(response); err != nil {
				log.Error(err, "")
			}

			return
		}
	}
}

type BackupConfigResponse struct {
	Errors  map[string]string `json:"errors"`
	Message string            `json:"message"`
	Data    any               `json:"data"`
	Status  int               `json:"status"`
	Success bool              `json:"success"`
}

type BackupUPIDsResponse struct {
	Errors  map[string]string       `json:"errors"`
	Message string                  `json:"message"`
	Data    []extjs.TasksWithStatus `json:"data"`
	Status  int                     `json:"status"`
	Success bool                    `json:"success"`
}

type BackupRunResponse struct {
	Errors  map[string]string `json:"errors"`
	Message string            `json:"message"`
	Data    string            `json:"data"`
	Status  int               `json:"status"`
	Success bool              `json:"success"`
}
