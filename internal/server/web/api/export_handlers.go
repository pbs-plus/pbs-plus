package api

import (
	"encoding/json"
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/pbs-plus/pbs-plus/internal/server/store"
)

// ExtJsBackupCSVExportHandler generates and returns a CSV file of all backup jobs.
func ExtJsBackupCSVExportHandler(storeInstance *store.Store) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			http.Error(w, "Invalid HTTP method", http.StatusBadRequest)
			return
		}

		allBackups, err := storeInstance.BackupSvc.ListBackups()
		if err != nil {
			WriteErrorResponse(w, err)
			return
		}

		flatBackups := FlattenBackups(allBackups, 0, false, nil)

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

		fmt.Fprintln(w, strings.Join(headers, ","))

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
				vals = append(vals, csvEscapeField(v))
			}
			fmt.Fprintln(w, strings.Join(vals, ","))
		}
	}
}

func csvEscapeField(s string) string {
	return csvEscape(s)
}

// D2DTargetTreeHandler returns targets pre-grouped into a tree structure.
func D2DTargetTreeHandler(storeInstance *store.Store) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			http.Error(w, "Invalid HTTP method", http.StatusMethodNotAllowed)
			return
		}

		all, err := storeInstance.TargetSvc.GetAllTargets()
		if err != nil {
			WriteErrorResponse(w, err)
			return
		}

		tree := BuildTargetTree(all)

		digest, err := calculateDigest(tree)
		if err != nil {
			WriteErrorResponse(w, err)
			return
		}

		toReturn := TargetsTreeResponse{
			Data:   tree,
			Digest: digest,
		}

		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(toReturn)
	}
}
