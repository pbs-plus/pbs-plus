//go:build linux

package mtfapi

import (
	"strconv"
	"strings"

	"github.com/pbs-plus/pbs-plus/internal/server/web/api/extjs"

	"github.com/pbs-plus/pbs-plus/internal/proxmox/tasklog"
	"github.com/pbs-plus/pbs-plus/internal/server/mtf"
	"github.com/pbs-plus/pbs-plus/internal/server/mtf/mtfdb"
)

// flatMtfJob is the flattened API response for an MTF job. The history block
type flatMtfJob struct {
	mtfdb.MTFJob
	LastRunUpid           string                 `json:"last-run-upid"`
	LastRunStarttime      int64                  `json:"last-run-starttime"`
	LastRunState          string                 `json:"last-run-state"`
	LastRunStatus         int                    `json:"last-run-status"`
	LastRunEndtime        int64                  `json:"last-run-endtime"`
	LastSuccessfulEndtime int64                  `json:"last-successful-endtime"`
	LastSuccessfulUpid    string                 `json:"last-successful-upid"`
	RetryCount            int                    `json:"retry-count"`
	Duration              int64                  `json:"duration"`
	StatusParsed          extjs.ParsedTaskStatus `json:"status_parsed"`
	CurrentFilesSpeed     int                    `json:"current_files_speed,omitempty"`
	CurrentBytesSpeed     int                    `json:"current_bytes_speed,omitempty"`
	CurrentBytesTotal     int64                  `json:"current_bytes_total,omitempty"`
	CurrentFileCount      int64                  `json:"current_file_count,omitempty"`
	CurrentFolderCount    int64                  `json:"current_folder_count,omitempty"`
	ReadSpeedHuman        string                 `json:"read_speed_human"`
	ReadTotalHuman        string                 `json:"read_total_human"`
	ProcessingSpeedHuman  string                 `json:"processing_speed_human"`
}

func flattenMtfJob(j mtfdb.MTFJob) flatMtfJob {
	f := flatMtfJob{
		MTFJob:                j,
		LastRunUpid:           j.History.LastRunUpid,
		LastRunStarttime:      j.History.LastRunStarttime,
		LastRunState:          j.History.LastRunState,
		LastRunStatus:         int(j.History.LastRunStatus),
		LastRunEndtime:        j.History.LastRunEndtime,
		LastSuccessfulEndtime: j.History.LastSuccessfulEndtime,
		LastSuccessfulUpid:    j.History.LastSuccessfulUpid,
		RetryCount:            j.History.RetryCount,
		Duration:              j.History.Duration,
		StatusParsed:          extjs.ParseTaskStatus(j.History.LastRunState),
	}
	if j.History.LastRunUpid != "" && (int(j.History.LastRunStatus) == 0 || j.History.LastRunState == "") {
		if r, ok := tasklog.ResolveHistoryFields(j.History.LastRunUpid); ok {
			tasklog.ApplyResolved(r, &f.LastRunStarttime, &f.LastRunEndtime, &f.Duration, &f.LastRunState)
			if f.LastRunState != "" {
				f.StatusParsed = extjs.ParseTaskStatus(f.LastRunState)
			}
		}
	}
	if p, ok := mtf.ProgressFor(j.ID); ok {
		f.CurrentFileCount = p.Files
		f.CurrentFolderCount = p.Dirs
		f.CurrentBytesTotal = p.Bytes
		f.CurrentBytesSpeed = int(p.PhysInst * 1e6)
		f.CurrentFilesSpeed = int(p.FilesInst)
		extjs.FillSpeedFields(&extjs.LiveStats{
			FileCount:   p.Files,
			FolderCount: p.Dirs,
			BytesTotal:  p.Bytes,
			BytesSpeed:  int(p.PhysInst * 1e6),
			FilesSpeed:  int(p.FilesInst),
		}, &f.ReadSpeedHuman, &f.ReadTotalHuman, &f.ProcessingSpeedHuman)
	}
	return f
}

func flattenMtfJobForEdit(j mtfdb.MTFJob) map[string]any {
	return map[string]any{
		"id":                 j.ID,
		"source_kind":        j.SourceKind,
		"source_ref":         j.SourceRef,
		"datastore":          j.Datastore,
		"namespace":          j.Namespace,
		"comment":            j.Comment,
		"notification-mode":  j.NotificationMode,
		"notification-batch": "",
		"changer":            j.Changer,
		"drive":              j.Drive,
		"spanning":           j.Spanning,
		"overwrite_mappings": j.OverwriteMappings,
	}
}

func atoiDefault(s string, def int) int {
	s = strings.TrimSpace(s)
	if s == "" {
		return def
	}
	n, err := strconv.Atoi(s)
	if err != nil {
		return def
	}
	return n
}
