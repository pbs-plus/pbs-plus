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

type diagnosis struct {
	Problem string
	Cause   string
	Action  string
}

func sourceLabel(job coredb.Backup) string {
	switch {
	case job.Target.AgentHost.Name != "":
		return fmt.Sprintf("agent %q (%s)", job.Target.AgentHost.Name, job.Target.AgentHost.IP)
	case job.Target.S3Info != nil:
		return fmt.Sprintf("S3 target %q", job.Target.Name)
	default:
		return fmt.Sprintf("target %q", job.Target.Name)
	}
}

func sourcePath(job coredb.Backup) string {
	path := job.Target.Path
	if job.Subpath != "" {
		path = strings.TrimRight(path, "/") + "/" + strings.TrimLeft(job.Subpath, "/")
	}
	return path
}

func diagnose(err error, job coredb.Backup) diagnosis {
	source := sourceLabel(job)

	switch {
	case err == nil:
		return diagnosis{Problem: "The backup finished."}

	case errors.Is(err, jobs.ErrTargetUnreachable),
		errors.Is(err, arpcfs.ErrSessionLost):
		return diagnosis{
			Problem: "The backup could not reach " + source + ".",
			Cause:   "The agent service stopped, the machine went offline, or the network dropped the connection.",
			Action:  "Confirm the machine is powered on and the PBS Plus agent service is running, then run the job again.",
		}

	case errors.Is(err, ErrBackupStalled):
		return diagnosis{
			Problem: "The backup stopped transferring data and was cancelled.",
			Cause:   "The connection to " + source + " stayed down, so no further files could be read.",
			Action:  "Check the agent machine and the network between it and this server. The job retries on its next schedule.",
		}

	case errors.Is(err, jobs.ErrTargetNotFound):
		return diagnosis{
			Problem: source + " is not registered on this server.",
			Cause:   "The agent is not in the targets list, so its connection is refused.",
			Action:  "Add the agent under Targets, or re-run the agent bootstrap so it registers under the expected hostname.",
		}

	case errors.Is(err, jobs.ErrMountEmpty):
		return diagnosis{
			Problem: "The source " + sourcePath(job) + " contained no files.",
			Cause:   "The drive or share was not present, or every file was excluded.",
			Action:  "Verify the path exists on the source machine and review this job's exclusions. Nothing was backed up.",
		}

	case errors.Is(err, jobs.ErrSubpathNotFound):
		return diagnosis{
			Problem: "The configured subpath " + job.Subpath + " does not exist on " + source + ".",
			Cause:   "The folder was renamed, removed, or lives on a drive that was not mounted.",
			Action:  "Correct the subpath in this job's settings, or restore the folder on the source machine.",
		}

	case errors.Is(err, jobs.ErrOneInstance), errors.Is(err, sessions.ErrMountInUse):
		return diagnosis{
			Problem: "A previous run of this job is still active.",
			Cause:   "PBS Plus refuses to start a second run against the same source while the first still holds it.",
			Action:  "Wait for the running job to finish, or stop it from the task list before starting another.",
		}

	case errors.Is(err, jobs.ErrCanceled):
		return diagnosis{
			Problem: "The backup was cancelled before it finished.",
			Action:  "Start the job again when the source is ready. No snapshot was written for this run.",
		}

	case errors.Is(err, ErrPrepareBackupCommand), errors.Is(err, ErrProxmoxBackupClientStart):
		return diagnosis{
			Problem: "The backup program could not be started.",
			Cause:   "proxmox-backup-client failed to launch with this job's settings.",
			Action:  "Check the datastore, namespace, and credentials for this job, then contact support with this log.",
		}

	case errors.Is(err, ErrTaskMonitoringTimedOut), errors.Is(err, ErrTaskDetectionFailed),
		errors.Is(err, ErrTaskMonitoringInitializationFailed):
		return diagnosis{
			Problem: "Proxmox Backup Server never reported this backup starting.",
			Cause:   "The backup task was not visible in the PBS task list within the allowed time.",
			Action:  "Check that the Proxmox Backup Server service is healthy and not overloaded, then run the job again.",
		}

	case errors.Is(err, sessions.ErrMountStale):
		return diagnosis{
			Problem: "The files of " + source + " were released before the backup finished.",
			Cause:   "An earlier run's cleanup reclaimed the mount this run was using.",
			Action:  "Run the job again. Report this if it repeats, because it should not normally happen.",
		}

	case strings.Contains(err.Error(), "mounted directory not accessible"):
		return diagnosis{
			Problem: "The files of " + source + " did not become readable in time.",
			Cause:   "The agent accepted the backup but never served its files, usually a stuck agent or a very slow disk.",
			Action:  "Restart the PBS Plus agent on the source machine, then run the job again.",
		}

	case errors.Is(err, ErrTaskFailed):
		return diagnosis{
			Problem: "The backup program reported a failure.",
			Action:  "Read the proxmox-backup-client lines below; they name the file or operation that failed.",
		}
	}

	return diagnosis{
		Problem: "The backup failed.",
		Action:  "Read the lines below for details, and include this log when contacting support.",
	}
}

func (d diagnosis) summaryLines(err error) []string {
	lines := []string{"PROBLEM: " + d.Problem}
	if d.Cause != "" {
		lines = append(lines, "LIKELY CAUSE: "+d.Cause)
	}
	if d.Action != "" {
		lines = append(lines, "WHAT TO DO: "+d.Action)
	}
	if err != nil {
		lines = append(lines, "TECHNICAL DETAIL: "+err.Error())
	}
	return lines
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
		fmt.Sprintf("Backup job %q into datastore %q, namespace %s", job.ID, job.Store, namespace),
		fmt.Sprintf("Source: %s, path %s, source mode %s", sourceLabel(job), sourcePath(job), job.SourceMode),
		fmt.Sprintf("Started %s, ended %s, ran for %s", formatStamp(start), formatStamp(end), formatRunDuration(start, end)),
		"",
	}
	return append(lines, diagnose(err, job).summaryLines(err)...)
}
