package backupapi

import (
	"encoding/json"
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/pbs-plus/pbs-plus/internal/server/web/api/digest"
	"github.com/pbs-plus/pbs-plus/internal/server/web/api/extjs"
	"github.com/pbs-plus/pbs-plus/internal/server/web/api/respond"

	"github.com/pbs-plus/pbs-plus/internal/log"
	"github.com/pbs-plus/pbs-plus/internal/server/application"
)

func ExtJsBackupCSVExportHandler(app *application.Runtime) http.HandlerFunc {
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

		flatBackups := extjs.FlattenBackups(allBackups, 0, false, nil)

		if len(flatBackups) == 0 {
			http.Error(w, "No records to export", http.StatusNoContent)
			return
		}

		filename := fmt.Sprintf("disk-backups-%s.csv", time.Now().Format("20060102-150405"))
		w.Header().Set("Content-Type", "text/csv")
		w.Header().Set("Content-Disposition", fmt.Sprintf("attachment; filename=\"%s\"", filename))

		headers := []string{
			"id", "store", "mode", "sourcemode", "readmode", "subpath", "ns",
			"schedule", "comment", "target", "expected_size",
			"last-run-upid", "last-run-state", "last-run-endtime",
			"last-successful-endtime", "last-successful-upid", "duration",
			"current_file_count", "current_folder_count",
			"current_files_speed", "current_bytes_speed", "current_bytes_total",
			"retry", "retry-interval", "max-dir-entries",
			"include-xattr", "legacy-xattr",
		}

		if _, err := fmt.Fprintln(w, strings.Join(headers, ",")); err != nil {
			log.Error(err, "")
		}

		for _, rec := range flatBackups {
			row := map[string]string{
				"id":                      rec.ID,
				"store":                   rec.Store,
				"mode":                    rec.Mode,
				"sourcemode":              rec.SourceMode,
				"readmode":                rec.ReadMode,
				"subpath":                 rec.Subpath,
				"ns":                      rec.Namespace,
				"schedule":                rec.Schedule,
				"comment":                 rec.Comment,
				"target":                  rec.Target,
				"expected_size":           strconv.Itoa(rec.ExpectedSize),
				"last-run-upid":           rec.LastRunUpid,
				"last-run-state":          rec.LastRunState,
				"last-run-endtime":        strconv.FormatInt(rec.LastRunEndtime, 10),
				"last-successful-endtime": strconv.FormatInt(rec.LastSuccessfulEndtime, 10),
				"last-successful-upid":    rec.LastSuccessfulUpid,
				"duration":                strconv.FormatInt(rec.Duration, 10),
				"current_file_count":      strconv.Itoa(rec.CurrentFileCount),
				"current_folder_count":    strconv.Itoa(rec.CurrentFolderCount),
				"current_files_speed":     strconv.Itoa(rec.CurrentFilesSpeed),
				"current_bytes_speed":     strconv.Itoa(rec.CurrentBytesSpeed),
				"current_bytes_total":     strconv.Itoa(rec.CurrentBytesTotal),
				"retry":                   strconv.Itoa(rec.Retry),
				"retry-interval":          strconv.Itoa(rec.RetryInterval),
				"max-dir-entries":         strconv.Itoa(rec.MaxDirEntries),
				"include-xattr":           strconv.FormatBool(rec.IncludeXattr),
				"legacy-xattr":            strconv.FormatBool(rec.LegacyXattr),
			}

			var vals []string
			for _, h := range headers {
				v := row[h]
				vals = append(vals, extjs.CSVEscape(v))
			}
			if _, err := fmt.Fprintln(w, strings.Join(vals, ",")); err != nil {
				log.Error(err, "")
			}
		}
	}
}

func D2DTargetTreeHandler(app *application.Runtime) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			http.Error(w, "Invalid HTTP method", http.StatusMethodNotAllowed)
			return
		}

		all, err := app.Target.GetAllTargets()
		if err != nil {
			respond.WriteErrorResponse(w, err)
			return
		}

		tree := extjs.BuildTargetTree(all)

		digest, err := digest.Calculate(tree)
		if err != nil {
			respond.WriteErrorResponse(w, err)
			return
		}

		toReturn := TargetsTreeResponse{
			Data:   tree,
			Digest: digest,
		}

		w.Header().Set("Content-Type", "application/json")
		if err := json.NewEncoder(w).Encode(toReturn); err != nil {
			log.Error(err, "")
		}
	}
}

type TargetsTreeResponse struct {
	Data   []extjs.TargetTreeNode `json:"data"`
	Digest string                 `json:"digest"`
}
