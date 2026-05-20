//go:build linux

package backup

import (
	"errors"
	"os/exec"
)

// ClientLogEvidence is structured evidence parsed from the
// proxmox-backup-client stdout log.
type ClientLogEvidence struct {
	// HasDuration is true if a "Duration: " line was found.
	HasDuration bool
	// HasEndTime is true if an "End Time: " line was found.
	HasEndTime bool
	// WarningCount is the number of WARNING: lines found.
	WarningCount int
	// HasTaskError is true if a TASK ERROR: line appeared.
	HasTaskError bool
	// UploadErrors are lines containing "upload failed:".
	UploadErrors []string
}

// ProxyLogEvidence is structured evidence parsed from the PBS proxy
// task log (/var/log/proxmox-backup/tasks/XX/<UPID>).
type ProxyLogEvidence struct {
	// HasCompletionMarker is true if any of these were found:
	//   "TASK OK", "TASK WARNINGS", "backup finished successfully",
	//   "successfully finished backup"
	HasCompletionMarker bool

	// TaskErrors are all TASK ERROR: messages found (without the prefix).
	TaskErrors []string

	// HasSpuriousConnectionError is true if at least one TASK ERROR
	// contains "connection error:".
	HasSpuriousConnectionError bool

	// HasOnlySpuriousErrors is true if all TASK ERROR entries are
	// spurious connection errors (no real errors).
	HasOnlySpuriousErrors bool
}

// DeterminationConfig holds the inputs needed to determine backup status.
// All fields must be populated before calling Determine — no I/O inside.
type DeterminationConfig struct {
	// ExitCode from cmd.Wait() (0 on success).
	ExitCode int

	// Canceled is true if the job context was canceled.
	Canceled bool

	// ClientEv is parsed from the proxmox-backup-client stdout log.
	ClientEv ClientLogEvidence

	// ProxyEv is parsed from the PBS proxy task log.
	ProxyEv ProxyLogEvidence

	// AgentConnected is the mount connection state at completion time.
	AgentConnected bool
}

// Determine computes the authoritative BackupStatus from collected evidence.
// Pure function — no I/O, no side effects, fully deterministic.
func Determine(cfg DeterminationConfig) BackupStatus {
	if cfg.Canceled {
		return StatusCanceled
	}

	clientCompleted := cfg.ClientEv.HasDuration && cfg.ClientEv.HasEndTime

	// Rule 1: Client exited 0 AND logged Duration/EndTime.
	// The client called finish() successfully — this is definitive
	// success regardless of what PBS's proxy log says (which can race).
	if cfg.ExitCode == 0 && clientCompleted {
		if cfg.ProxyEv.HasOnlySpuriousErrors {
			return statusWithWarnings(cfg.ClientEv)
		}
		return statusWithWarnings(cfg.ClientEv)
	}

	// Rule 2: Client exited 0 but didn't log Duration/EndTime.
	// This means the client exited cleanly but didn't complete its work.
	// Trust PBS if it confirms completion.
	if cfg.ExitCode == 0 && !clientCompleted {
		if cfg.ProxyEv.HasCompletionMarker {
			return statusWithWarnings(cfg.ClientEv)
		}
		// No completion evidence from either side — treat as failure.
		return StatusFailed
	}

	// Rule 3: Client exited non-zero.
	// Check if it's a false failure from the PBS proxy race.
	if cfg.ExitCode != 0 {
		// If the client log shows Duration/EndTime, the backup data was
		// actually committed. The non-zero exit is from the connection
		// dropping after finish.
		if clientCompleted && cfg.ProxyEv.HasOnlySpuriousErrors {
			return statusWithWarnings(cfg.ClientEv)
		}
		return StatusFailed
	}

	return StatusFailed
}

// statusWithWarnings returns StatusOK if there are no warnings,
// StatusWarnings otherwise.
func statusWithWarnings(ev ClientLogEvidence) BackupStatus {
	if ev.WarningCount > 0 || len(ev.UploadErrors) > 0 || ev.HasTaskError {
		return StatusWarnings
	}
	return StatusOK
}

// exitCodeFromErr extracts the process exit code from a cmd.Wait error.
// Returns 0 if err is nil, -1 if the exit code cannot be determined.
func exitCodeFromErr(err error) int {
	if err == nil {
		return 0
	}
	var exitErr *exec.ExitError
	if errors.As(err, &exitErr) {
		return exitErr.ExitCode()
	}
	return -1
}
