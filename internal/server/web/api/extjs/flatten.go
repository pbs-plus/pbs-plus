package extjs

import (
	"sort"
	"time"

	"github.com/pbs-plus/pbs-plus/internal/server/coredb"
)

func FlattenBackup(b coredb.Backup) FlatBackup {
	fb := FlatBackup{
		ID:               b.ID,
		Store:            b.Store,
		Mode:             b.Mode,
		SourceMode:       b.SourceMode,
		ReadMode:         b.ReadMode,
		Subpath:          b.Subpath,
		Namespace:        b.Namespace,
		Schedule:         b.Schedule,
		Comment:          b.Comment,
		NotificationMode: b.NotificationMode,
		PreScript:        b.PreScript,
		PostScript:       b.PostScript,
		NextRun:          b.NextRun,
		Retry:            b.Retry,
		RetryInterval:    b.RetryInterval,
		MaxDirEntries:    b.MaxDirEntries,
		RawExclusions:    b.RawExclusions,
		IncludeXattr:     b.IncludeXattr,
		LegacyXattr:      b.LegacyXattr,

		Target: b.Target.Name,

		LastRunUpid:           b.History.LastRunUpid,
		LastRunState:          b.History.LastRunState,
		LastRunEndtime:        b.History.LastRunEndtime,
		LastSuccessfulEndtime: b.History.LastSuccessfulEndtime,
		LastSuccessfulUpid:    b.History.LastSuccessfulUpid,
		Duration:              b.History.Duration,

		CurrentFileCount:   b.CurrentStats.CurrentFileCount,
		CurrentFolderCount: b.CurrentStats.CurrentFolderCount,
		CurrentFilesSpeed:  b.CurrentStats.CurrentFilesSpeed,
		CurrentBytesSpeed:  b.CurrentStats.CurrentBytesSpeed,
		CurrentBytesTotal:  b.CurrentStats.CurrentBytesTotal,
	}

	if b.Target.Name != "" {
		fb.ExpectedSize = b.Target.VolumeUsedBytes
		fb.TargetSizeHuman = HumanReadableBytes(b.Target.VolumeUsedBytes)
	}

	FillSpeedFields(&LiveStats{
		FileCount:   int64(b.CurrentStats.CurrentFileCount),
		FolderCount: int64(b.CurrentStats.CurrentFolderCount),
		FilesSpeed:  b.CurrentStats.CurrentFilesSpeed,
		BytesSpeed:  b.CurrentStats.CurrentBytesSpeed,
		BytesTotal:  int64(b.CurrentStats.CurrentBytesTotal),
	}, &fb.ReadSpeedHuman, &fb.ReadTotalHuman, &fb.ProcessingSpeedHuman)

	fb.StatusParsed = ParseTaskStatus(b.History.LastRunState)

	return fb
}

// If staleDays > 0, sets Stale=true for jobs whose last-successful-endtime
// is older than staleDays. If skipUnscheduled is true, jobs with no schedule
// are never marked stale. excludedJobs is a set of job IDs to skip.
func FlattenBackups(backups []coredb.Backup, staleDays int, skipUnscheduled bool, excludedJobs map[string]bool) []FlatBackup {
	result := make([]FlatBackup, len(backups))
	var cutoff int64
	if staleDays > 0 {
		cutoff = time.Now().Unix() - int64(staleDays)*24*60*60
	}
	for i := range backups {
		result[i] = FlattenBackup(backups[i])
		if staleDays <= 0 {
			continue
		}
		b := &backups[i]
		if excludedJobs != nil && excludedJobs[b.ID] {
			continue
		}
		if skipUnscheduled && b.Schedule == "" {
			continue
		}
		if b.History.LastSuccessfulEndtime == 0 {
			// Never ran  -  only stale if has a schedule (or not skipping unscheduled)
			if !skipUnscheduled || b.Schedule != "" {
				result[i].Stale = true
			}
		} else if b.History.LastSuccessfulEndtime < cutoff {
			result[i].Stale = true
		}
	}
	return result
}

func FlattenRestore(r coredb.Restore) FlatRestore {
	fr := FlatRestore{
		ID:               r.ID,
		Store:            r.Store,
		Namespace:        r.Namespace,
		Snapshot:         r.Snapshot,
		SnapshotHuman:    formatSnapshotLabel(r.Snapshot, r.Namespace),
		SrcPath:          r.SrcPath,
		DestSubpath:      r.DestSubpath,
		PreScript:        r.PreScript,
		PostScript:       r.PostScript,
		Comment:          r.Comment,
		NotificationMode: r.NotificationMode,
		Retry:            r.Retry,
		RetryInterval:    r.RetryInterval,

		DestTarget: r.DestTarget.Name,

		LastRunUpid:           r.History.LastRunUpid,
		LastRunState:          r.History.LastRunState,
		LastRunEndtime:        r.History.LastRunEndtime,
		LastSuccessfulEndtime: r.History.LastSuccessfulEndtime,
		LastSuccessfulUpid:    r.History.LastSuccessfulUpid,
		Duration:              r.History.Duration,

		CurrentFileCount:   r.CurrentStats.CurrentFileCount,
		CurrentFolderCount: r.CurrentStats.CurrentFolderCount,
		CurrentFilesSpeed:  r.CurrentStats.CurrentFilesSpeed,
		CurrentBytesSpeed:  r.CurrentStats.CurrentBytesSpeed,
		CurrentBytesTotal:  r.CurrentStats.CurrentBytesTotal,
	}

	if r.DestTarget.Name != "" {
		fr.ExpectedSize = r.DestTarget.VolumeUsedBytes
		fr.TargetSizeHuman = HumanReadableBytes(r.DestTarget.VolumeUsedBytes)
	}

	FillSpeedFields(&LiveStats{
		FileCount:   int64(r.CurrentStats.CurrentFileCount),
		FolderCount: int64(r.CurrentStats.CurrentFolderCount),
		FilesSpeed:  r.CurrentStats.CurrentFilesSpeed,
		BytesSpeed:  r.CurrentStats.CurrentBytesSpeed,
		BytesTotal:  int64(r.CurrentStats.CurrentBytesTotal),
	}, &fr.ReadSpeedHuman, &fr.ReadTotalHuman, &fr.ProcessingSpeedHuman)

	fr.StatusParsed = ParseTaskStatus(r.History.LastRunState)

	return fr
}

func FlattenRestores(restores []coredb.Restore) []FlatRestore {
	result := make([]FlatRestore, len(restores))
	for i := range restores {
		result[i] = FlattenRestore(restores[i])
	}
	return result
}

func FlattenVerificationJob(vj coredb.VerificationJob) FlatVerificationJob {
	fvj := FlatVerificationJob{
		ID:                  vj.ID,
		BackupJobID:         vj.BackupJobID,
		Store:               vj.Store,
		Namespace:           vj.Namespace,
		Mode:                vj.Mode,
		Schedule:            vj.Schedule,
		Comment:             vj.Comment,
		NotificationMode:    vj.NotificationMode,
		NextRun:             vj.NextRun,
		Retry:               vj.Retry,
		RetryInterval:       vj.RetryInterval,
		TargetMode:          vj.TargetMode,
		Recursive:           vj.Recursive,
		RunOnBackupComplete: vj.RunOnBackupComplete,
		CreatedAt:           vj.CreatedAt,

		LastRunUpid:           vj.History.LastRunUpid,
		LastRunState:          vj.History.LastRunState,
		LastRunStarttime:      vj.History.LastRunStarttime,
		LastRunEndtime:        vj.History.LastRunEndtime,
		LastSuccessfulEndtime: vj.History.LastSuccessfulEndtime,
		LastSuccessfulUpid:    vj.History.LastSuccessfulUpid,
		Duration:              vj.History.Duration,

		SpotConfig: SpotCheckConfigJSON{
			SampleCount:        vj.SpotConfig.SampleCount,
			SampleCountPercent: vj.SpotConfig.SampleCountPercent,
			SamplingStrategy:   vj.SpotConfig.SamplingStrategy,
			UseLatest:          vj.SpotConfig.UseLatest,
			DateFrom:           vj.SpotConfig.DateFrom,
			DateTo:             vj.SpotConfig.DateTo,
			FailThreshold:      vj.SpotConfig.FailThreshold,
		},
	}

	for _, f := range vj.SpotConfig.Filters {
		fvj.SpotConfig.Filters = append(fvj.SpotConfig.Filters, SpotCheckFilterJSON{
			PathPattern: f.PathPattern,
			MinSize:     f.MinSize,
			MaxSize:     f.MaxSize,
		})
	}

	fvj.StatusParsed = ParseTaskStatus(vj.History.LastRunState)

	return fvj
}

func FlattenVerificationJobs(jobs []coredb.VerificationJob) []FlatVerificationJob {
	result := make([]FlatVerificationJob, len(jobs))
	for i := range jobs {
		result[i] = FlattenVerificationJob(jobs[i])
	}
	return result
}

func FlattenVerificationResult(r coredb.VerificationResult, namespace string) FlatVerificationResult {
	fr := FlatVerificationResult{
		ID:                r.ID,
		VerificationJobID: r.VerificationJobID,
		UPID:              r.UPID,
		Snapshot:          r.Snapshot,
		SnapshotTime:      r.SnapshotTime,
		SnapshotHuman:     formatSnapshotLabel(r.Snapshot, namespace),
		TotalPopulation:   r.TotalPopulation,
		TotalFiles:        r.TotalFiles,
		VerifiedFiles:     r.VerifiedFiles,
		FailedFiles:       r.FailedFiles,
		SkippedFiles:      r.SkippedFiles,
		Status:            r.Status,
		StartedAt:         r.StartedAt,
		CompletedAt:       r.CompletedAt,
		Confidence:        ComputeConfidence(r.TotalPopulation, r.TotalFiles, r.FailedFiles),
	}

	switch {
	case r.Status == "completed" && r.FailedFiles == 0:
		fr.StatusBadge = "passed"
	case r.FailedFiles > 0:
		fr.StatusBadge = "failed"
	default:
		fr.StatusBadge = "warning"
	}

	if r.StartedAt > 0 && r.CompletedAt > r.StartedAt {
		secs := r.CompletedAt - r.StartedAt
		fr.DurationHuman = FormatDuration(secs)
	}

	if r.TotalFiles > 0 {
		fr.PassRate = float64(r.VerifiedFiles) / float64(r.TotalFiles) * 100
	}

	for _, f := range r.Details {
		fr.Details = append(fr.Details, FlatVerificationFileResult{
			Path:        f.Path,
			Size:        f.Size,
			SizeHuman:   HumanReadableBytes(int(f.Size)),
			Status:      f.Status,
			StatusHuman: renderFileStatusHuman(f.Status),
			Message:     f.Message,
		})
	}

	return fr
}

func FlattenVerificationResults(results []coredb.VerificationResult, namespace string) []FlatVerificationResult {
	fr := make([]FlatVerificationResult, len(results))
	for i := range results {
		fr[i] = FlattenVerificationResult(results[i], namespace)
	}
	return fr
}

func formatSnapshotLabel(snapshot, namespace string) string {
	if namespace == "" || namespace == "root" {
		return snapshot
	}
	return namespace + ": " + snapshot
}

func BuildTargetTree(targets []coredb.Target, kind coredb.TargetType) []TargetTreeNode {
	var localTargets []TargetTreeNode
	agentGroups := map[string]*TargetTreeNode{}
	var s3Targets []TargetTreeNode
	var postgreSQLTargets []TargetTreeNode
	var mysqlTargets []TargetTreeNode

	for i := range targets {
		t := targets[i]
		if kind != "" && t.Type != kind {
			continue
		}
		node := TargetTreeNode{
			Text:                     t.Name,
			Name:                     t.Name,
			Path:                     t.Path,
			TargetType:               t.LegacyType(),
			Kind:                     string(t.Type),
			Access:                   string(t.Access),
			MountScript:              t.MountScript,
			VolumeID:                 t.VolumeID,
			JobCount:                 t.JobCount,
			AgentVersion:             t.AgentVersion,
			ConnectionStatus:         t.ConnectionStatus,
			VolumeType:               t.VolumeType,
			VolumeName:               t.VolumeName,
			VolumeFS:                 t.VolumeFS,
			VolumeTotalBytes:         t.VolumeTotalBytes,
			VolumeUsedBytes:          t.VolumeUsedBytes,
			VolumeFreeBytes:          t.VolumeFreeBytes,
			VolumeTotalHuman:         t.VolumeTotal,
			VolumeUsedHuman:          t.VolumeUsed,
			VolumeFreeHuman:          t.VolumeFree,
			DatabaseHost:             t.DatabaseHost,
			DatabasePort:             t.DatabasePort,
			DatabaseUsername:         t.DatabaseUsername,
			DatabaseTLSMode:          t.DatabaseTLSMode,
			DatabaseCACertificate:    t.DatabaseCACertificate,
			DatabaseDefaultClientDir: t.DatabaseDefaultClientDir,
			DatabaseVariant:          t.DatabaseVariant,
			DatabaseClientFamily:     t.DatabaseClientFamily,
			Leaf:                     true,
			IsGroup:                  false,
		}

		switch {
		case t.IsAgent():
			hostname := t.AgentHost.Name
			node.AgentHostname = hostname
			node.OS = t.AgentHost.OperatingSystem
			node.IP = t.AgentHost.IP
			node.IconCls = "fa fa-hdd-o"

			if hostname != "" {
				if _, ok := agentGroups[hostname]; !ok {
					agentGroups[hostname] = &TargetTreeNode{
						Text:      hostname,
						IconCls:   "fa fa-server",
						IsGroup:   true,
						GroupType: "agent",
						Expanded:  true,
						OS:        t.AgentHost.OperatingSystem,
						IP:        t.AgentHost.IP,
					}
				}
				agentGroups[hostname].Children = append(agentGroups[hostname].Children, node)
			} else {
				node.IconCls = "fa fa-hdd-o"
				localTargets = append(localTargets, node)
			}

		case t.IsS3():
			node.IconCls = "fa fa-cloud"
			s3Targets = append(s3Targets, node)
		case t.Type == coredb.TargetTypePostgreSQL:
			node.IconCls = "fa fa-database"
			postgreSQLTargets = append(postgreSQLTargets, node)
		case t.Type == coredb.TargetTypeMySQL:
			node.IconCls = "fa fa-database"
			mysqlTargets = append(mysqlTargets, node)

		default:
			node.IconCls = "fa fa-folder"
			localTargets = append(localTargets, node)
		}
	}

	var rootChildren []TargetTreeNode

	if len(localTargets) > 0 {
		rootChildren = append(rootChildren, TargetTreeNode{
			Text:      "Local Targets",
			IconCls:   "fa fa-desktop",
			IsGroup:   true,
			GroupType: "local",
			Expanded:  true,
			Children:  localTargets,
		})
	}

	if len(agentGroups) > 0 {
		var agentChildren []TargetTreeNode
		hostnames := make([]string, 0, len(agentGroups))
		for name := range agentGroups {
			hostnames = append(hostnames, name)
		}
		sort.Strings(hostnames)
		for _, name := range hostnames {
			agentChildren = append(agentChildren, *agentGroups[name])
		}
		rootChildren = append(rootChildren, TargetTreeNode{
			Text:      "Agent Targets",
			IconCls:   "fa fa-sitemap",
			IsGroup:   true,
			GroupType: "agent-root",
			Expanded:  true,
			Children:  agentChildren,
		})
	}

	if len(s3Targets) > 0 {
		rootChildren = append(rootChildren, TargetTreeNode{
			Text:      "S3 Targets",
			IconCls:   "fa fa-cloud",
			IsGroup:   true,
			GroupType: "s3",
			Expanded:  true,
			Children:  s3Targets,
		})
	}

	if len(postgreSQLTargets) > 0 {
		rootChildren = append(rootChildren, TargetTreeNode{
			Text: "PostgreSQL Targets", IconCls: "fa fa-database", IsGroup: true,
			GroupType: "postgresql", Expanded: true, Children: postgreSQLTargets,
		})
	}

	if len(mysqlTargets) > 0 {
		rootChildren = append(rootChildren, TargetTreeNode{
			Text: "MySQL / MariaDB Targets", IconCls: "fa fa-database", IsGroup: true,
			GroupType: "mysql", Expanded: true, Children: mysqlTargets,
		})
	}

	return rootChildren
}

func renderFileStatusHuman(status string) string {
	switch status {
	case "ok":
		return "✓ OK"
	case "failed":
		return "✗ Failed"
	case "skipped":
		return "○ Skipped"
	case "warning":
		return "⚠ Warning"
	case "error":
		return "⚠ Error"
	default:
		return status
	}
}

func FlattenBackupForEdit(b coredb.Backup) map[string]any {
	return map[string]any{
		"id":                     b.ID,
		"store":                  b.Store,
		"mode":                   b.Mode,
		"sourcemode":             b.SourceMode,
		"readmode":               b.ReadMode,
		"target":                 b.Target.Name,
		"subpath":                b.Subpath,
		"ns":                     b.Namespace,
		"schedule":               b.Schedule,
		"comment":                b.Comment,
		"notification-mode":      b.NotificationMode,
		"notification-batch":     "",
		"pre_script":             b.PreScript,
		"post_script":            b.PostScript,
		"retry":                  b.Retry,
		"retry-interval":         b.RetryInterval,
		"max-dir-entries":        b.MaxDirEntries,
		"rawexclusions":          b.RawExclusions,
		"include-xattr":          b.IncludeXattr,
		"legacy-xattr":           b.LegacyXattr,
		"database_scope":         b.DatabaseScope,
		"database_name":          b.DatabaseName,
	}
}

func FlattenRestoreForEdit(r coredb.Restore) map[string]any {
	return map[string]any{
		"id":                     r.ID,
		"store":                  r.Store,
		"ns":                     r.Namespace,
		"snapshot":               r.Snapshot,
		"src-path":               r.SrcPath,
		"dest-target":            r.DestTarget.Name,
		"dest-subpath":           r.DestSubpath,
		"mode":                   r.Mode,
		"comment":                r.Comment,
		"notification-mode":      r.NotificationMode,
		"notification-batch":     "",
		"pre_script":             r.PreScript,
		"post_script":            r.PostScript,
		"retry":                  r.Retry,
		"retry-interval":         r.RetryInterval,
		"source_database":        r.SourceDatabase,
		"destination_database":   r.DestinationDatabase,
		"replace_existing":       r.ReplaceExisting,
		"history": map[string]any{
			"last-run-state":          r.History.LastRunState,
			"last-run-upid":           r.History.LastRunUpid,
			"last-run-endtime":        r.History.LastRunEndtime,
			"last-successful-endtime": r.History.LastSuccessfulEndtime,
			"last-successful-upid":    r.History.LastSuccessfulUpid,
		},
	}
}

type FlatBackup struct {
	ID                   string `json:"id"`
	Store                string `json:"store"`
	Mode                 string `json:"mode"`
	SourceMode           string `json:"sourcemode"`
	ReadMode             string `json:"readmode"`
	Subpath              string `json:"subpath"`
	Namespace            string `json:"ns"`
	Schedule             string `json:"schedule"`
	Comment              string `json:"comment"`
	NotificationMode     string `json:"notification-mode"`
	PreScript            string `json:"pre_script"`
	PostScript           string `json:"post_script"`
	NextRun              int64  `json:"next-run"`
	Retry                int    `json:"retry"`
	RetryInterval        int    `json:"retry-interval"`
	MaxDirEntries        int    `json:"max-dir-entries"`
	RawExclusions        string `json:"rawexclusions"`
	IncludeXattr         bool   `json:"include-xattr"`
	LegacyXattr          bool   `json:"legacy-xattr"`
	DatabaseScope        string `json:"database_scope,omitempty"`
	DatabaseName         string `json:"database_name,omitempty"`

	Target       string `json:"target"`
	ExpectedSize int    `json:"expected_size,omitempty"`

	LastRunUpid           string `json:"last-run-upid"`
	LastRunState          string `json:"last-run-state"`
	LastRunEndtime        int64  `json:"last-run-endtime"`
	LastSuccessfulEndtime int64  `json:"last-successful-endtime"`
	LastSuccessfulUpid    string `json:"last-successful-upid"`
	Duration              int64  `json:"duration"`

	CurrentFileCount   int `json:"current_file_count,omitempty"`
	CurrentFolderCount int `json:"current_folder_count,omitempty"`
	CurrentFilesSpeed  int `json:"current_files_speed,omitempty"`
	CurrentBytesSpeed  int `json:"current_bytes_speed,omitempty"`
	CurrentBytesTotal  int `json:"current_bytes_total,omitempty"`

	TargetSizeHuman      string           `json:"target_size_human"`
	ReadSpeedHuman       string           `json:"read_speed_human"`
	ReadTotalHuman       string           `json:"read_total_human"`
	ProcessingSpeedHuman string           `json:"processing_speed_human"`
	StatusParsed         ParsedTaskStatus `json:"status_parsed"`

	// Computed server-side from alert settings
	Stale bool `json:"stale"`
}

type FlatRestore struct {
	ID                   string `json:"id"`
	Store                string `json:"store"`
	Namespace            string `json:"ns"`
	Snapshot             string `json:"snapshot"`
	SnapshotHuman        string `json:"snapshot_human"`
	SrcPath              string `json:"src-path"`
	DestSubpath          string `json:"dest-subpath"`
	PreScript            string `json:"pre_script"`
	PostScript           string `json:"post_script"`
	Comment              string `json:"comment"`
	NotificationMode     string `json:"notification-mode"`
	Retry                int    `json:"retry"`
	RetryInterval        int    `json:"retry-interval"`
	ExpectedSize         int    `json:"expected_size,omitempty"`
	SourceDatabase       string `json:"source_database,omitempty"`
	DestinationDatabase  string `json:"destination_database,omitempty"`
	ReplaceExisting      bool   `json:"replace_existing,omitempty"`

	DestTarget string `json:"dest-target"`

	LastRunUpid           string `json:"last-run-upid"`
	LastRunState          string `json:"last-run-state"`
	LastRunEndtime        int64  `json:"last-run-endtime"`
	LastSuccessfulEndtime int64  `json:"last-successful-endtime"`
	LastSuccessfulUpid    string `json:"last-successful-upid"`
	Duration              int64  `json:"duration"`

	CurrentFileCount   int `json:"current_file_count,omitempty"`
	CurrentFolderCount int `json:"current_folder_count,omitempty"`
	CurrentFilesSpeed  int `json:"current_files_speed,omitempty"`
	CurrentBytesSpeed  int `json:"current_bytes_speed,omitempty"`
	CurrentBytesTotal  int `json:"current_bytes_total,omitempty"`

	TargetSizeHuman      string           `json:"target_size_human"`
	ReadSpeedHuman       string           `json:"read_speed_human"`
	ReadTotalHuman       string           `json:"read_total_human"`
	ProcessingSpeedHuman string           `json:"processing_speed_human"`
	StatusParsed         ParsedTaskStatus `json:"status_parsed"`
}

type FlatVerificationJob struct {
	ID                  string              `json:"id"`
	BackupJobID         string              `json:"backup_job_id"`
	Store               string              `json:"store"`
	Namespace           string              `json:"ns"`
	Mode                string              `json:"mode"`
	Schedule            string              `json:"schedule"`
	Comment             string              `json:"comment"`
	NotificationMode    string              `json:"notification-mode"`
	SpotConfig          SpotCheckConfigJSON `json:"spot_config"`
	NextRun             int64               `json:"next-run"`
	Retry               int                 `json:"retry"`
	RetryInterval       int                 `json:"retry-interval"`
	TargetMode          string              `json:"target_mode"`
	Recursive           bool                `json:"recursive"`
	RunOnBackupComplete bool                `json:"run_on_backup_complete"`
	CreatedAt           int64               `json:"created_at"`

	LastRunUpid           string `json:"last-run-upid"`
	LastRunState          string `json:"last-run-state"`
	LastRunStarttime      int64  `json:"last-run-starttime"`
	LastRunEndtime        int64  `json:"last-run-endtime"`
	LastSuccessfulEndtime int64  `json:"last-successful-endtime"`
	LastSuccessfulUpid    string `json:"last-successful-upid"`
	Duration              int64  `json:"duration"`

	StatusParsed ParsedTaskStatus `json:"status_parsed"`
}

type SpotCheckConfigJSON struct {
	SampleCount        int                   `json:"sample_count"`
	SampleCountPercent float64               `json:"sample_count_percent"`
	SamplingStrategy   string                `json:"sampling_strategy"`
	UseLatest          bool                  `json:"use_latest"`
	DateFrom           string                `json:"date_from"`
	DateTo             string                `json:"date_to"`
	Filters            []SpotCheckFilterJSON `json:"filters"`
	FailThreshold      int                   `json:"fail_threshold"`
}

type SpotCheckFilterJSON struct {
	PathPattern string `json:"path_pattern"`
	MinSize     int64  `json:"min_size"`
	MaxSize     int64  `json:"max_size"`
}

type FlatVerificationResult struct {
	ID                int                          `json:"id"`
	VerificationJobID string                       `json:"verification_job_id"`
	UPID              string                       `json:"upid"`
	Snapshot          string                       `json:"snapshot"`
	SnapshotHuman     string                       `json:"snapshot_human"`
	SnapshotTime      int64                        `json:"snapshot_time"`
	TotalPopulation   int                          `json:"total_population"`
	TotalFiles        int                          `json:"total_files"`
	VerifiedFiles     int                          `json:"verified_files"`
	FailedFiles       int                          `json:"failed_files"`
	SkippedFiles      int                          `json:"skipped_files"`
	Status            string                       `json:"status"`
	StartedAt         int64                        `json:"started_at"`
	CompletedAt       int64                        `json:"completed_at"`
	DurationHuman     string                       `json:"duration_human"`
	PassRate          float64                      `json:"pass_rate"`
	Confidence        ConfidenceInfo               `json:"confidence"`
	StatusBadge       string                       `json:"status_badge"`
	Details           []FlatVerificationFileResult `json:"details"`
}

type FlatVerificationFileResult struct {
	Path        string `json:"path"`
	Size        int64  `json:"size"`
	SizeHuman   string `json:"size_human"`
	Status      string `json:"status"`
	StatusHuman string `json:"status_human"`
	Message     string `json:"message"`
}

type TargetTreeNode struct {
	Text      string           `json:"text"`
	IconCls   string           `json:"iconCls,omitempty"`
	Expanded  bool             `json:"expanded"`
	IsGroup   bool             `json:"isGroup"`
	GroupType string           `json:"groupType,omitempty"`
	Leaf      bool             `json:"leaf"`
	Children  []TargetTreeNode `json:"children,omitempty"`

	Name                     string `json:"name,omitempty"`
	Path                     string `json:"path,omitempty"`
	TargetType               string `json:"target_type,omitempty"`
	Kind                     string `json:"kind,omitempty"`
	Access                   string `json:"access,omitempty"`
	MountScript              string `json:"mount_script,omitempty"`
	VolumeID                 string `json:"volume_id,omitempty"`
	JobCount                 int    `json:"job_count,omitempty"`
	AgentVersion             string `json:"agent_version,omitempty"`
	ConnectionStatus         bool   `json:"connection_status,omitempty"`
	VolumeType               string `json:"volume_type,omitempty"`
	VolumeName               string `json:"volume_name,omitempty"`
	VolumeFS                 string `json:"volume_fs,omitempty"`
	VolumeTotalBytes         int    `json:"volume_total_bytes,omitempty"`
	VolumeUsedBytes          int    `json:"volume_used_bytes,omitempty"`
	VolumeFreeBytes          int    `json:"volume_free_bytes,omitempty"`
	VolumeTotalHuman         string `json:"volume_total,omitempty"`
	VolumeUsedHuman          string `json:"volume_used,omitempty"`
	VolumeFreeHuman          string `json:"volume_free,omitempty"`
	AgentHostname            string `json:"agent_hostname,omitempty"`
	OS                       string `json:"os,omitempty"`
	IP                       string `json:"ip,omitempty"`
	DatabaseHost             string `json:"database_host,omitempty"`
	DatabasePort             int    `json:"database_port,omitempty"`
	DatabaseUsername         string `json:"database_username,omitempty"`
	DatabaseTLSMode          string `json:"database_tls_mode,omitempty"`
	DatabaseCACertificate    string `json:"database_ca_certificate,omitempty"`
	DatabaseDefaultClientDir string `json:"database_default_client_dir,omitempty"`
	DatabaseVariant          string `json:"database_variant,omitempty"`
	DatabaseClientFamily     string `json:"database_default_client_family,omitempty"`
}
