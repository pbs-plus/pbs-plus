//go:build linux

package backup

import (
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/pbs-plus/pbs-plus/internal/server/coredb"
	"github.com/pbs-plus/pbs-plus/internal/server/jobs"
	"github.com/pbs-plus/pbs-plus/internal/server/vfs/arpcfs"
	"github.com/pbs-plus/pbs-plus/internal/server/vfs/sessions"
)

func sourceLabel(job coredb.Backup) string {
	switch {
	case job.Target.AgentHost.Name != "":
		return fmt.Sprintf("agent %s (%s)", job.Target.AgentHost.Name, job.Target.AgentHost.IP)
	case job.Target.S3Info != nil:
		return "S3 target " + job.Target.Name
	default:
		return "target " + job.Target.Name
	}
}

func sourcePath(job coredb.Backup) string {
	path := job.Target.Path
	if job.Subpath != "" {
		path = strings.TrimRight(path, "/") + "/" + strings.TrimLeft(job.Subpath, "/")
	}
	return path
}

func failureSummary(err error, job coredb.Backup) string {
	source := sourceLabel(job)

	switch {
	case err == nil:
		return "backup finished"
	case errors.Is(err, jobs.ErrTargetUnreachable), errors.Is(err, arpcfs.ErrSessionLost):
		return "cannot reach " + source
	case errors.Is(err, ErrBackupStalled):
		return "cancelled after the connection to " + source + " stayed down"
	case errors.Is(err, jobs.ErrTargetNotFound):
		return source + " is not registered on this server"
	case errors.Is(err, jobs.ErrMountEmpty):
		return "source " + sourcePath(job) + " is empty, nothing was backed up"
	case errors.Is(err, jobs.ErrSubpathNotFound):
		return "subpath " + job.Subpath + " does not exist on " + source
	case errors.Is(err, jobs.ErrOneInstance), errors.Is(err, sessions.ErrMountInUse):
		return "a previous run of this job is still active"
	case errors.Is(err, jobs.ErrCanceled):
		return "cancelled before it finished"
	case errors.Is(err, ErrPrepareBackupCommand), errors.Is(err, ErrProxmoxBackupClientStart):
		return "proxmox-backup-client could not be started"
	case errors.Is(err, ErrTaskMonitoringTimedOut), errors.Is(err, ErrTaskDetectionFailed),
		errors.Is(err, ErrTaskMonitoringInitializationFailed):
		return "Proxmox Backup Server never reported this backup starting"
	case errors.Is(err, sessions.ErrMountStale):
		return "the mount of " + source + " was released before the backup finished"
	case strings.Contains(err.Error(), "mounted directory not accessible"):
		return "files of " + source + " did not become readable in time"
	case errors.Is(err, ErrTaskFailed):
		return "proxmox-backup-client reported a failure"
	}
	return "backup failed"
}

func formatStamp(unix int64) string {
	if unix <= 0 {
		return "unknown"
	}
	return time.Unix(unix, 0).Format(time.RFC3339)
}

func formatRunDuration(start, end int64) string {
	if start <= 0 || end <= start {
		return "unknown"
	}
	return (time.Duration(end-start) * time.Second).Round(time.Second).String()
}

func reportLines(job coredb.Backup, err error, start, end int64) []string {
	namespace := job.Namespace
	if namespace == "" {
		namespace = "(root)"
	}
	lines := []string{
		"Job: " + job.ID,
		fmt.Sprintf("Datastore: %s, namespace %s", job.Store, namespace),
		fmt.Sprintf("Source: %s, path %s, mode %s", sourceLabel(job), sourcePath(job), job.SourceMode),
		fmt.Sprintf("Started: %s, ended %s, duration %s", formatStamp(start), formatStamp(end), formatRunDuration(start, end)),
	}
	if err == nil {
		return append(lines, "Result: "+failureSummary(nil, job))
	}
	return append(lines,
		"Result: "+failureSummary(err, job),
		"Detail: "+err.Error(),
	)
}
