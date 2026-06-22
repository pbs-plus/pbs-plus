//go:build linux

package backup

import (
	"bufio"
	"fmt"
	"os"
	"strings"
)

// CollectClientEvidence parses the proxmox-backup-client stdout log file
// and returns structured evidence. Does not modify any files.
func CollectClientEvidence(logPath string) (ClientLogEvidence, error) {
	f, err := os.Open(logPath)
	if err != nil {
		return ClientLogEvidence{}, fmt.Errorf("opening client log: %w", err)
	}
	defer f.Close()

	var ev ClientLogEvidence
	scanner := bufio.NewScanner(f)
	scanner.Buffer(make([]byte, 0, 64*1024), 1024*1024)

	for scanner.Scan() {
		line := scanner.Text()

		if strings.Contains(line, "Duration: ") {
			ev.HasDuration = true
		}
		if strings.Contains(line, "End Time: ") {
			ev.HasEndTime = true
		}
		if strings.Contains(line, "WARNING: ") {
			ev.WarningCount++
		}
		if strings.Contains(line, "TASK ERROR:") {
			ev.HasTaskError = true
		}
		if strings.Contains(line, "upload failed:") {
			ev.UploadErrors = append(ev.UploadErrors, line)
		}
	}

	if err := scanner.Err(); err != nil {
		return ev, fmt.Errorf("scanning client log: %w", err)
	}

	return ev, nil
}

// CollectProxyEvidence parses the PBS proxy task log file and returns
// structured evidence. Does not modify any files.
func CollectProxyEvidence(logPath string) (ProxyLogEvidence, error) {
	f, err := os.Open(logPath)
	if err != nil {
		return ProxyLogEvidence{}, fmt.Errorf("opening proxy log: %w", err)
	}
	defer f.Close()

	var ev ProxyLogEvidence
	scanner := bufio.NewScanner(f)
	scanner.Buffer(make([]byte, 0, 64*1024), 1024*1024)

	completionMarkers := []string{
		"TASK OK",
		"TASK WARNINGS",
		"backup finished successfully",
		"successfully finished backup",
	}

	for scanner.Scan() {
		line := scanner.Text()

		if IsJunkLog(line) {
			continue
		}

		for _, m := range completionMarkers {
			if strings.Contains(line, m) {
				ev.HasCompletionMarker = true
			}
		}

		if idx := strings.Index(line, "TASK ERROR:"); idx >= 0 {
			errMsg := line[idx+len("TASK ERROR: "):]
			// Strip leading timestamp if present
			errMsg = strings.TrimSpace(errMsg)
			ev.TaskErrors = append(ev.TaskErrors, errMsg)

			if strings.Contains(errMsg, "connection error:") {
				ev.HasSpuriousConnectionError = true
			}
		}
	}

	if err := scanner.Err(); err != nil {
		return ev, fmt.Errorf("scanning proxy log: %w", err)
	}

	// Determine if all errors are spurious
	ev.HasOnlySpuriousErrors = len(ev.TaskErrors) > 0 && ev.HasSpuriousConnectionError
	if ev.HasOnlySpuriousErrors {
		for _, e := range ev.TaskErrors {
			if !strings.Contains(e, "connection error:") {
				ev.HasOnlySpuriousErrors = false
				break
			}
		}
	}

	return ev, nil
}
