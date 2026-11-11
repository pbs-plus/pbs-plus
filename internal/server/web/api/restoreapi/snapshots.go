//go:build linux

package restoreapi

import (
	"cmp"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"slices"
	"strings"

	"github.com/pbs-plus/pbs-plus/internal/log"
	"github.com/pbs-plus/pbs-plus/internal/proxmox"
	"github.com/pbs-plus/pbs-plus/internal/proxmox/pbsapi"
	"github.com/pbs-plus/pbs-plus/internal/server/application"
	"github.com/pbs-plus/pbs-plus/internal/server/coredb"
	"github.com/pbs-plus/pbs-plus/internal/server/web/api/respond"
	"github.com/pbs-plus/pbs-plus/internal/validate"
)

const (
	pxarSuffix  = ".pxar.didx"
	mpxarSuffix = ".mpxar.didx"
)

type RestoreSnapshot struct {
	Value        string `json:"value"`
	BackupID     string `json:"backup-id"`
	BackupTime   int64  `json:"backup-time"`
	Size         int64  `json:"size"`
	Comment      string `json:"comment,omitempty"`
	Verification string `json:"verification,omitempty"`
	Protected    bool   `json:"protected"`
	SourceTarget string `json:"source_target,omitempty"`
	Archive      string `json:"archive,omitempty"`
}

type snapshotArchives struct {
	names  []string
	browse string
}

func archivesOf(files []pbsapi.SnapshotFile) snapshotArchives {
	var out snapshotArchives
	var pxar string
	for _, f := range files {
		switch {
		case strings.HasSuffix(f.Filename, mpxarSuffix):
			out.names = append(out.names, strings.TrimSuffix(f.Filename, mpxarSuffix))
			out.browse = f.Filename
		case strings.HasSuffix(f.Filename, pxarSuffix):
			out.names = append(out.names, strings.TrimSuffix(f.Filename, pxarSuffix))
			pxar = f.Filename
		}
	}
	if out.browse == "" {
		out.browse = pxar
	}
	return out
}

func restoreSnapshots(snapshots []pbsapi.Snapshot, targets []coredb.Target, dest string) []RestoreSnapshot {
	owners := make(map[string]*coredb.Target, len(targets))
	var destTarget *coredb.Target
	for i := range targets {
		owners[proxmox.NormalizeHostname(targets[i].Name)] = &targets[i]
		if targets[i].Name == dest {
			destTarget = &targets[i]
		}
	}

	compatible := func(archives []string) bool {
		if destTarget == nil {
			return true
		}
		for _, name := range archives {
			owner, ok := owners[name]
			if !ok {
				continue
			}
			if destTarget.IsStructured() && owner.Type == destTarget.Type {
				return true
			}
			if !destTarget.IsStructured() && owner.IsStructured() {
				return false
			}
		}
		return !destTarget.IsStructured()
	}

	out := make([]RestoreSnapshot, 0, len(snapshots))
	for _, s := range snapshots {
		archives := archivesOf(s.Files)
		if !compatible(archives.names) {
			continue
		}
		snap := RestoreSnapshot{
			Value:      fmt.Sprintf("%s/%s/%d", s.BackupType, s.BackupID, s.BackupTime),
			BackupID:   s.BackupID,
			BackupTime: s.BackupTime,
			Size:       s.Size,
			Comment:    s.Comment,
			Protected:  s.Protected,
			Archive:    archives.browse,
		}
		if s.Verification != nil {
			snap.Verification = s.Verification.State
		}
		for _, name := range archives.names {
			if owner, ok := owners[name]; ok {
				snap.SourceTarget = owner.Name
				break
			}
		}
		out = append(out, snap)
	}
	slices.SortStableFunc(out, func(a, b RestoreSnapshot) int { return cmp.Compare(b.BackupTime, a.BackupTime) })
	return out
}

func D2DRestoreSnapshotsHandler(app *application.Runtime, client *pbsapi.Client) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			respond.MethodNotAllowed(w, r)
			return
		}
		query := r.URL.Query()
		store, ns, dest := query.Get("store"), query.Get("ns"), query.Get("target")
		if err := validate.ValidateDatastore(store); err != nil {
			respond.Error(w, http.StatusBadRequest, err)
			return
		}
		if err := validate.ValidateNamespace(ns); err != nil {
			respond.Error(w, http.StatusBadRequest, err)
			return
		}

		snapshots, err := client.HostSnapshots(r.Context(), r, store, ns)
		if apiErr, ok := errors.AsType[*pbsapi.Error](err); ok {
			respond.Error(w, apiErr.Status, apiErr)
			return
		}
		if err != nil {
			respond.WriteErrorResponse(w, err)
			return
		}
		targets, err := app.Target.GetAllTargets()
		if err != nil {
			respond.WriteErrorResponse(w, err)
			return
		}

		w.Header().Set("Content-Type", "application/json")
		if err := json.NewEncoder(w).Encode(map[string]any{"data": restoreSnapshots(snapshots, targets, dest), "success": true}); err != nil {
			log.Error(err, "")
		}
	}
}
