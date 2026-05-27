package api

import (
	"sort"

	"github.com/pbs-plus/pbs-plus/internal/server/database"
)

// FlattenBackup converts a database.Backup into a flat API response.
func FlattenBackup(b database.Backup) FlatBackup {
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

		// Flatten target
		Target: b.Target.Name,

		// Flatten history
		LastRunUpid:           b.History.LastRunUpid,
		LastRunState:          b.History.LastRunState,
		LastRunEndtime:        b.History.LastRunEndtime,
		LastSuccessfulEndtime: b.History.LastSuccessfulEndtime,
		LastSuccessfulUpid:    b.History.LastSuccessfulUpid,
		Duration:              b.History.Duration,

		// Flatten current-stats
		CurrentFileCount:   b.CurrentStats.CurrentFileCount,
		CurrentFolderCount: b.CurrentStats.CurrentFolderCount,
		CurrentFilesSpeed:  b.CurrentStats.CurrentFilesSpeed,
		CurrentBytesSpeed:  b.CurrentStats.CurrentBytesSpeed,
		CurrentBytesTotal:  b.CurrentStats.CurrentBytesTotal,
	}

	// Flatten target object info
	if b.Target.Name != "" {
		fb.ExpectedSize = b.Target.VolumeUsedBytes
		fb.TargetSizeHuman = HumanReadableBytes(b.Target.VolumeUsedBytes)
	}

	// Pre-format display fields
	if b.CurrentStats.CurrentBytesSpeed > 0 {
		fb.ReadSpeedHuman = HumanReadableSpeed(b.CurrentStats.CurrentBytesSpeed)
	}
	if b.CurrentStats.CurrentBytesTotal > 0 {
		fb.ReadTotalHuman = HumanReadableBytes(b.CurrentStats.CurrentBytesTotal)
	}
	if b.CurrentStats.CurrentFilesSpeed > 0 {
		fb.ProcessingSpeedHuman = FormatSpeed(b.CurrentStats.CurrentFilesSpeed)
	}

	// Parse task status
	fb.StatusParsed = ParseTaskStatus(b.History.LastRunState)

	return fb
}

// FlattenBackups converts a slice of backups to flat responses.
func FlattenBackups(backups []database.Backup) []FlatBackup {
	result := make([]FlatBackup, len(backups))
	for i := range backups {
		result[i] = FlattenBackup(backups[i])
	}
	return result
}

// FlattenRestore converts a database.Restore into a flat API response.
func FlattenRestore(r database.Restore) FlatRestore {
	fr := FlatRestore{
		ID:            r.ID,
		Store:         r.Store,
		Namespace:     r.Namespace,
		Snapshot:      r.Snapshot,
		SrcPath:       r.SrcPath,
		DestSubpath:   r.DestSubpath,
		PreScript:     r.PreScript,
		PostScript:    r.PostScript,
		Comment:       r.Comment,
		Retry:         r.Retry,
		RetryInterval: r.RetryInterval,

		// Flatten dest-target
		DestTarget: r.DestTarget.Name,

		// Flatten history
		LastRunUpid:           r.History.LastRunUpid,
		LastRunState:          r.History.LastRunState,
		LastRunEndtime:        r.History.LastRunEndtime,
		LastSuccessfulEndtime: r.History.LastSuccessfulEndtime,
		LastSuccessfulUpid:    r.History.LastSuccessfulUpid,
		Duration:              r.History.Duration,

		// Flatten current-stats
		CurrentFileCount:   r.CurrentStats.CurrentFileCount,
		CurrentFolderCount: r.CurrentStats.CurrentFolderCount,
		CurrentFilesSpeed:  r.CurrentStats.CurrentFilesSpeed,
		CurrentBytesSpeed:  r.CurrentStats.CurrentBytesSpeed,
		CurrentBytesTotal:  r.CurrentStats.CurrentBytesTotal,
	}

	// Flatten dest-target object info
	if r.DestTarget.Name != "" {
		fr.ExpectedSize = r.DestTarget.VolumeUsedBytes
		fr.TargetSizeHuman = HumanReadableBytes(r.DestTarget.VolumeUsedBytes)
	}

	// Pre-format display fields
	if r.CurrentStats.CurrentBytesSpeed > 0 {
		fr.ReadSpeedHuman = HumanReadableSpeed(r.CurrentStats.CurrentBytesSpeed)
	}
	if r.CurrentStats.CurrentBytesTotal > 0 {
		fr.ReadTotalHuman = HumanReadableBytes(r.CurrentStats.CurrentBytesTotal)
	}
	if r.CurrentStats.CurrentFilesSpeed > 0 {
		fr.ProcessingSpeedHuman = FormatSpeed(r.CurrentStats.CurrentFilesSpeed)
	}

	// Parse task status
	fr.StatusParsed = ParseTaskStatus(r.History.LastRunState)

	return fr
}

// FlattenRestores converts a slice of restores to flat responses.
func FlattenRestores(restores []database.Restore) []FlatRestore {
	result := make([]FlatRestore, len(restores))
	for i := range restores {
		result[i] = FlattenRestore(restores[i])
	}
	return result
}

// FlattenVerificationJob converts a database.VerificationJob into a flat API response.
func FlattenVerificationJob(vj database.VerificationJob) FlatVerificationJob {
	fvj := FlatVerificationJob{
		ID:                  vj.ID,
		BackupJobID:         vj.BackupJobID,
		Store:               vj.Store,
		Namespace:           vj.Namespace,
		Mode:                vj.Mode,
		Schedule:            vj.Schedule,
		Comment:             vj.Comment,
		NextRun:             vj.NextRun,
		Retry:               vj.Retry,
		RetryInterval:       vj.RetryInterval,
		TargetMode:          vj.TargetMode,
		Recursive:           vj.Recursive,
		RunOnBackupComplete: vj.RunOnBackupComplete,
		CreatedAt:           vj.CreatedAt,

		// Flatten history
		LastRunUpid:           vj.History.LastRunUpid,
		LastRunState:          vj.History.LastRunState,
		LastRunStarttime:      vj.History.LastRunStarttime,
		LastRunEndtime:        vj.History.LastRunEndtime,
		LastSuccessfulEndtime: vj.History.LastSuccessfulEndtime,
		LastSuccessfulUpid:    vj.History.LastSuccessfulUpid,
		Duration:              vj.History.Duration,

		// Flatten spot_config
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

	// Convert filters
	for _, f := range vj.SpotConfig.Filters {
		fvj.SpotConfig.Filters = append(fvj.SpotConfig.Filters, SpotCheckFilterJSON{
			PathPattern: f.PathPattern,
			MinSize:     f.MinSize,
			MaxSize:     f.MaxSize,
		})
	}

	// Parse task status
	fvj.StatusParsed = ParseTaskStatus(vj.History.LastRunState)

	return fvj
}

// FlattenVerificationJobs converts a slice of verification jobs to flat responses.
func FlattenVerificationJobs(jobs []database.VerificationJob) []FlatVerificationJob {
	result := make([]FlatVerificationJob, len(jobs))
	for i := range jobs {
		result[i] = FlattenVerificationJob(jobs[i])
	}
	return result
}

// FlattenVerificationResult converts a VerificationResult with pre-computed display fields.
func FlattenVerificationResult(r database.VerificationResult) FlatVerificationResult {
	fr := FlatVerificationResult{
		ID:                r.ID,
		VerificationJobID: r.VerificationJobID,
		UPID:              r.UPID,
		Snapshot:          r.Snapshot,
		SnapshotTime:      r.SnapshotTime,
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

	// Duration
	if r.StartedAt > 0 && r.CompletedAt > r.StartedAt {
		secs := r.CompletedAt - r.StartedAt
		fr.DurationHuman = FormatDuration(secs)
	}

	// Pass rate
	if r.TotalFiles > 0 {
		fr.PassRate = float64(r.VerifiedFiles) / float64(r.TotalFiles) * 100
	}

	// Flatten file details
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

// FlattenVerificationResults converts a slice of verification results.
func FlattenVerificationResults(results []database.VerificationResult) []FlatVerificationResult {
	fr := make([]FlatVerificationResult, len(results))
	for i := range results {
		fr[i] = FlattenVerificationResult(results[i])
	}
	return fr
}

// BuildTargetTree groups targets into a tree structure by type (local/agent/s3).
func BuildTargetTree(targets []database.Target) []TargetTreeNode {
	var localTargets []TargetTreeNode
	agentGroups := map[string]*TargetTreeNode{}
	var s3Targets []TargetTreeNode

	for i := range targets {
		t := targets[i]
		node := TargetTreeNode{
			Text:             t.Name,
			Name:             t.Name,
			Path:             t.Path,
			TargetType:       string(t.Type),
			MountScript:      t.MountScript,
			VolumeID:         t.VolumeID,
			JobCount:         t.JobCount,
			AgentVersion:     t.AgentVersion,
			ConnectionStatus: t.ConnectionStatus,
			VolumeType:       t.VolumeType,
			VolumeName:       t.VolumeName,
			VolumeFS:         t.VolumeFS,
			VolumeTotalBytes: t.VolumeTotalBytes,
			VolumeUsedBytes:  t.VolumeUsedBytes,
			VolumeFreeBytes:  t.VolumeFreeBytes,
			VolumeTotalHuman: t.VolumeTotal,
			VolumeUsedHuman:  t.VolumeUsed,
			VolumeFreeHuman:  t.VolumeFree,
			Leaf:             true,
			IsGroup:          false,
		}

		switch t.Type {
		case database.TargetTypeAgent:
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

		case database.TargetTypeS3:
			node.IconCls = "fa fa-cloud"
			s3Targets = append(s3Targets, node)

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

// FlattenBackupForEdit flattens a Backup for the edit form GET response.
// The edit form expects target as a string, not an object.
func FlattenBackupForEdit(b database.Backup) map[string]any {
	return map[string]any{
		"id":                b.ID,
		"store":             b.Store,
		"mode":              b.Mode,
		"sourcemode":        b.SourceMode,
		"readmode":          b.ReadMode,
		"target":            b.Target.Name,
		"subpath":           b.Subpath,
		"ns":                b.Namespace,
		"schedule":          b.Schedule,
		"comment":           b.Comment,
		"notification-mode": b.NotificationMode,
		"pre_script":        b.PreScript,
		"post_script":       b.PostScript,
		"retry":             b.Retry,
		"retry-interval":    b.RetryInterval,
		"max-dir-entries":   b.MaxDirEntries,
		"rawexclusions":     b.RawExclusions,
		"include-xattr":     b.IncludeXattr,
		"legacy-xattr":      b.LegacyXattr,
	}
}

// FlattenRestoreForEdit flattens a Restore for the edit form GET response.
func FlattenRestoreForEdit(r database.Restore) map[string]any {
	return map[string]any{
		"id":             r.ID,
		"store":          r.Store,
		"ns":             r.Namespace,
		"snapshot":       r.Snapshot,
		"src-path":       r.SrcPath,
		"dest-target":    r.DestTarget.Name,
		"dest-subpath":   r.DestSubpath,
		"mode":           r.Mode,
		"comment":        r.Comment,
		"pre_script":     r.PreScript,
		"post_script":    r.PostScript,
		"retry":          r.Retry,
		"retry-interval": r.RetryInterval,
		"history": map[string]any{
			"last-run-state":          r.History.LastRunState,
			"last-run-upid":           r.History.LastRunUpid,
			"last-run-endtime":        r.History.LastRunEndtime,
			"last-successful-endtime": r.History.LastSuccessfulEndtime,
			"last-successful-upid":    r.History.LastSuccessfulUpid,
		},
	}
}
