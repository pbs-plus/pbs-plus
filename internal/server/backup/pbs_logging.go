//go:build linux

package backup

import (
	"bufio"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/pbs-plus/pbs-plus/internal/syslog"
)

var (
	commonErrorMap = map[string]string{
		"exit status 255": "lost connection with backup agent",
		"signal: killed":  ErrCanceled.Error(),
	}
)

func systemLocation() *time.Location {
	target, err := os.Readlink("/etc/localtime")
	if err != nil {
		return time.Local
	}

	const prefix = "/usr/share/zoneinfo/"
	_, after, ok := strings.Cut(target, prefix)
	if !ok {
		return time.Local
	}

	name := after
	loc, err := time.LoadLocation(name)
	if err != nil {
		return time.Local
	}

	return loc
}

// processPBSProxyLogs is the main entry point for post-backup log
// processing. It collects structured evidence from both the PBS proxy
// task log and the client stdout log, determines the backup status
// using the pure Determine function, and appends the client log to the
// PBS task log file.
//
// Returns (succeeded, cancelled, warningCount, error).
func processPBSProxyLogs(
	isGraceful bool,
	upid string,
	clientLogFile *syslog.JobLogger,
	customErr error,
) (bool, bool, int, error) {
	logFilePath := getTaskLogPath(upid)

	// Collect evidence from both log sources.
	clientEv, err := CollectClientEvidence(clientLogFile.Path)
	if err != nil {
		syslog.L.Error(err).WithMessage("failed to collect client evidence").Write()
	}

	proxyEv, err := CollectProxyEvidence(logFilePath)
	if err != nil {
		syslog.L.Error(err).WithMessage("failed to collect proxy evidence").Write()
	}

	exitCode := exitCodeFromErr(customErr)

	status := Determine(DeterminationConfig{
		ExitCode:       exitCode,
		Canceled:       false, // cancellation is handled by the caller
		ClientEv:       clientEv,
		ProxyEv:        proxyEv,
		AgentConnected: isGraceful,
	})

	warningsNum := clientEv.WarningCount + len(clientEv.UploadErrors)
	if clientEv.HasTaskError {
		warningsNum++
	}

	// Write the merged log (client log appended to PBS task log).
	if err := writeMergedLog(logFilePath, clientLogFile, status, warningsNum, customErr); err != nil {
		syslog.L.Error(err).WithMessage("failed to write merged log").Write()
	}

	succeeded := status.IsSuccess()
	cancelled := status == StatusCanceled || (status == StatusFailed && customErr != nil)

	return succeeded, cancelled, warningsNum, nil
}

// writeMergedLog appends the client log to the PBS task log and writes
// a final status line. Unlike the old implementation, this appends to
// the existing file rather than rewriting it.
func writeMergedLog(
	logFilePath string,
	clientLogFile *syslog.JobLogger,
	status BackupStatus,
	warningsNum int,
	customErr error,
) error {
	// Open the PBS task log for append.
	f, err := os.OpenFile(logFilePath, os.O_WRONLY|os.O_APPEND, 0644)
	if err != nil {
		return fmt.Errorf("opening task log for append: %w", err)
	}
	defer f.Close()

	writer := bufio.NewWriter(f)

	// Append client log.
	clientFile, err := os.Open(clientLogFile.Path)
	if err != nil {
		return fmt.Errorf("opening client log: %w", err)
	}
	defer clientFile.Close()

	writer.WriteString("--- proxmox-backup-client log starts here ---\n")

	scanner := bufio.NewScanner(clientFile)
	scanner.Buffer(make([]byte, 0, 64*1024), 1024*1024)

	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line == "" {
			continue
		}

		// Rewrite errors as warnings — the real status is in Determine().
		if strings.Contains(line, "TASK ERROR:") {
			line = "WARNING [proxmox-backup-client error]:" + line
		} else if strings.Contains(line, "upload failed:") {
			line = "WARNING [proxmox-backup-client error]:" + line
		}

		// Skip completion markers from client log — we write our own.
		if strings.Contains(line, "TASK OK") ||
			strings.Contains(line, "TASK WARNINGS") ||
			strings.Contains(line, "backup finished successfully") ||
			strings.Contains(line, "successfully finished backup") {
			continue
		}

		writer.WriteString(line)
		writer.WriteByte('\n')
	}

	if err := scanner.Err(); err != nil {
		return fmt.Errorf("scanning client log: %w", err)
	}

	// Write final status line.
	timestamp := time.Now().In(systemLocation()).Format(time.RFC3339)
	switch status {
	case StatusOK:
		writer.WriteString(timestamp)
		writer.WriteString(": TASK OK\n")
	case StatusWarnings:
		writer.WriteString(timestamp)
		writer.WriteString(": TASK WARNINGS: ")
		writer.WriteString(strconv.Itoa(warningsNum))
		writer.WriteByte('\n')
	case StatusCanceled, StatusFailed:
		writer.WriteString(timestamp)
		writer.WriteString(": TASK ERROR: ")
		if customErr != nil {
			errStr := customErr.Error()
			if mapped, ok := commonErrorMap[errStr]; ok {
				errStr = mapped
			}
			writer.WriteString(errStr)
		} else {
			writer.WriteString(ErrUnexpected.Error())
		}
		writer.WriteByte('\n')
	}

	if err := writer.Flush(); err != nil {
		return fmt.Errorf("flushing merged log: %w", err)
	}

	return nil
}

// getTaskLogPath returns the path to the PBS task log file for a UPID.
func getTaskLogPath(upid string) string {
	upidSplit := strings.Split(upid, ":")
	if len(upidSplit) < 4 {
		return ""
	}
	parsed := upidSplit[3]
	logFolder := parsed[len(parsed)-2:]
	return filepath.Join("/var/log/proxmox-backup/tasks", logFolder, upid)
}
