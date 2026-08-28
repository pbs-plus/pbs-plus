//go:build linux

package mountapi

import (
	"encoding/base64"
	"fmt"
	"net/http"
	"strings"
	"time"

	"github.com/pbs-plus/pbs-plus/internal/server/application"
	"github.com/pbs-plus/pbs-plus/internal/server/jobs"
	"github.com/pbs-plus/pbs-plus/internal/server/snapshotmount"
	"github.com/pbs-plus/pbs-plus/internal/server/web/api/respond"
	"github.com/pbs-plus/pbs-plus/internal/validate"
)

const (
	maxComposePaths     = 1024
	maxComposePathBytes = 4096
)

type composeForm struct {
	Datastore  string
	SourceNS   string
	SourceType string
	SourceID   string
	SourceTime string
	SourceFile string
	TargetNS   string
	TargetType string
	TargetID   string
	Paths      []string
	StripRoot  bool
}

func parseComposeForm(r *http.Request) (composeForm, error) {
	f := composeForm{
		Datastore:  validate.DecodePath(r.PathValue("datastore")),
		SourceNS:   strings.TrimSpace(r.FormValue("ns")),
		SourceType: strings.TrimSpace(r.FormValue("backup-type")),
		SourceID:   strings.TrimSpace(r.FormValue("backup-id")),
		SourceTime: strings.TrimSpace(r.FormValue("backup-time")),
		SourceFile: strings.TrimSpace(r.FormValue("file-name")),
		TargetNS:   strings.TrimSpace(r.FormValue("target-ns")),
		TargetType: strings.TrimSpace(r.FormValue("target-type")),
		TargetID:   strings.TrimSpace(r.FormValue("target-id")),
	}
	if err := validate.ValidateDatastore(f.Datastore); err != nil {
		return f, fmt.Errorf("invalid datastore: %w", err)
	}
	if err := validate.ValidateNamespace(f.SourceNS); err != nil {
		return f, err
	}
	if err := validate.ValidateNamespace(f.TargetNS); err != nil {
		return f, fmt.Errorf("invalid target namespace: %w", err)
	}
	if err := validate.ValidateBackupType(f.SourceType); err != nil {
		return f, err
	}
	if f.TargetType == "" {
		f.TargetType = "host"
	}
	if err := validate.ValidateBackupType(f.TargetType); err != nil {
		return f, fmt.Errorf("invalid target backup type: %w", err)
	}
	if err := validate.ValidateBackupID(f.SourceID); err != nil {
		return f, err
	}
	if err := validate.ValidateBackupID(f.TargetID); err != nil {
		return f, fmt.Errorf("invalid target backup id: %w", err)
	}
	if _, err := time.Parse(time.RFC3339, f.SourceTime); err != nil {
		return f, fmt.Errorf("invalid backup-time format: %w", err)
	}
	if err := validate.ValidateFileName(f.SourceFile); err != nil {
		return f, err
	}
	if !strings.HasSuffix(f.SourceFile, ".mpxar.didx") && !strings.HasSuffix(f.SourceFile, ".pxar.didx") {
		return f, fmt.Errorf("source archive must be a .mpxar.didx or .pxar.didx file")
	}
	paths, err := decodeComposePaths(r.Form["paths"])
	if err != nil {
		return f, err
	}
	f.Paths = paths
	f.StripRoot = r.FormValue("strip-root") == "1"
	if f.StripRoot && len(f.Paths) != 1 {
		return f, fmt.Errorf("directory flatten requires exactly one selected directory")
	}
	return f, nil
}

func decodeComposePaths(values []string) ([]string, error) {
	seen := make(map[string]struct{}, len(values))
	var paths []string
	for _, joined := range values {
		for _, raw := range strings.Split(joined, ",") {
			raw = strings.TrimSpace(raw)
			if raw == "" {
				continue
			}
			decoded, err := base64.StdEncoding.DecodeString(raw)
			if err != nil {
				return nil, fmt.Errorf("invalid path encoding %q: %w", raw, err)
			}
			p := string(decoded)
			if p == "" || p[0] != '/' {
				return nil, fmt.Errorf("path %q must be absolute", p)
			}
			if len(p) > maxComposePathBytes {
				return nil, fmt.Errorf("selected path is too long")
			}
			for _, seg := range strings.Split(p, "/") {
				if seg == ".." {
					return nil, fmt.Errorf("parent traversal is not allowed")
				}
			}
			if _, dup := seen[p]; dup {
				continue
			}
			seen[p] = struct{}{}
			paths = append(paths, p)
			if len(paths) > maxComposePaths {
				return nil, fmt.Errorf("too many paths selected (max %d)", maxComposePaths)
			}
		}
	}
	if len(paths) == 0 {
		return nil, fmt.Errorf("no paths selected")
	}
	return paths, nil
}

func ExtJsComposeHandler(app *application.Runtime) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "Invalid HTTP method", http.StatusBadRequest)
			return
		}
		if err := r.ParseForm(); err != nil {
			respond.WriteErrorResponse(w, err)
			return
		}
		f, err := parseComposeForm(r)
		if err != nil {
			writeProfileInvalid(w, err)
			return
		}

		key := snapshotmount.Key(f.Datastore, f.TargetNS, f.TargetType, f.TargetID, "compose")
		task, err := newTask("compose", f.Datastore, key)
		if err != nil {
			respond.WriteErrorResponse(w, err)
			return
		}
		upid, ok := submitSnapshotWorkflow(w, r, app, jobs.WorkflowSnapshotCompose, key, "snapshot-compose:"+key, jobs.SnapshotComposeInput{
			Datastore:  f.Datastore,
			SourceNS:   f.SourceNS,
			SourceType: f.SourceType,
			SourceID:   f.SourceID,
			SourceTime: f.SourceTime,
			SourceFile: f.SourceFile,
			TargetNS:   f.TargetNS,
			TargetType: f.TargetType,
			TargetID:   f.TargetID,
			Paths:      f.Paths,
			StripRoot:  f.StripRoot,
			UPID:       upidTask(task),
			Web:        true,
		}, 10*time.Minute)
		if !ok {
			task.CloseErr(fmt.Errorf("workflow submit failed"))
			return
		}
		writeRunResponse(w, upid)
	}
}
