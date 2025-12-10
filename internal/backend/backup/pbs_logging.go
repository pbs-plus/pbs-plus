//go:build linux

package backup

import (
	"bufio"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/pbs-plus/pbs-plus/internal/syslog"
	"github.com/pbs-plus/pbs-plus/internal/utils"
)

var JunkSubstrings = []string{
	"upload_chunk done:",
	"POST /dynamic_chunk",
	"POST /dynamic_index",
	"PUT /dynamic_index",
	"dynamic_append",
	"successfully added chunk",
	"created new dynamic index",
	"GET /previous",
	"from previous backup.",
}

func isJunkLog(line string) bool {
	for _, junk := range JunkSubstrings {
		if strings.Contains(line, junk) {
			return true
		}
	}
	return false
}

func processPBSProxyLogs(isGraceful bool, upid string, clientLogFile *syslog.BackupLogger) (bool, bool, int, string, error) {
	logFilePath := utils.GetTaskLogPath(upid)
	inFile, err := os.Open(logFilePath)
	if err != nil {
		return false, false, 0, "", fmt.Errorf("opening input log file: %w", err)
	}
	defer inFile.Close()

	// Retrieve original file's metadata (permissions, ownership, and timestamps)
	info, err := inFile.Stat()
	if err != nil {
		return false, false, 0, "", fmt.Errorf("getting stat of file %s: %w", logFilePath, err)
	}
	origMode := info.Mode()
	origModTime := info.ModTime()
	statT, ok := info.Sys().(*syscall.Stat_t)
	if !ok {
		return false, false, 0, "", fmt.Errorf("failed to retrieve underlying stat for file %s", logFilePath)
	}
	origUid := int(statT.Uid)
	origGid := int(statT.Gid)
	origAccessTime := time.Unix(statT.Atim.Sec, statT.Atim.Nsec)

	// Create a temporary file in the same directory
	dir := filepath.Dir(logFilePath)
	tmpFile, err := os.CreateTemp(dir, "processed_*.tmp")
	if err != nil {
		return false, false, 0, "", fmt.Errorf("creating temporary file: %w", err)
	}
	tmpName := tmpFile.Name()
	defer func() {
		if tmpFile != nil {
			tmpFile.Close()
			os.Remove(tmpFile.Name()) // Clean up in case of error
		}
	}()

	tmpWriter := bufio.NewWriter(tmpFile)

	// Filter existing log content
	scanner := bufio.NewScanner(inFile)
	const maxCapacity = 1024 * 1024 // 1 MB
	buf := make([]byte, 0, 64*1024)
	scanner.Buffer(buf, maxCapacity)

	for scanner.Scan() {
		line := scanner.Text()
		if isJunkLog(line) {
			continue // Skip junk lines
		}
		if _, err := tmpWriter.WriteString(line + "\n"); err != nil {
			return false, false, 0, "", fmt.Errorf("writing to temporary file: %w", err)
		}
	}
	if err := scanner.Err(); err != nil {
		return false, false, 0, "", fmt.Errorf("scanning input file: %w", err)
	}

	// Write header for proxmox backup client logs
	if _, err := tmpWriter.WriteString(
		"--- proxmox-backup-client log starts here ---\n",
	); err != nil {
		return false, false, 0, "", fmt.Errorf("failed to write log header: %w", err)
	}

	// Process output files and analyze for status info
	hasError := false
	incomplete := true
	disconnected := false
	var errorString string
	var errorPath string
	pbsWarningRawCount := 0

	// Process status info while streaming the logs to avoid storing everything in memory
	processLogFile := func(path string) error {
		file, err := os.Open(path)
		if err != nil {
			return fmt.Errorf("failed to open log file: %w", err)
		}
		defer file.Close()

		scanner := bufio.NewScanner(file)
		for scanner.Scan() {
			line := scanner.Text()

			if strings.Contains(line, "warning: ") {
				pbsWarningRawCount++
			}

			skipLine := false
			// Check for indicators before writing the line
			if strings.Contains(line, "Error: upload failed:") {
				errorString = strings.Replace(line, "Error:", "TASK ERROR:", 1)

				re := regexp.MustCompile(`upload failed: error at "([^"]+)"`)
				matches := re.FindStringSubmatch(line)
				if len(matches) >= 2 {
					errorPath = matches[1]
				}

				hasError = true
				skipLine = true
			} else if strings.Contains(line, "TASK ERROR:") {
				errorString = line
				hasError = true
				skipLine = true
			}

			if skipLine {
				continue
			}

			if strings.Contains(line, "connection failed") || strings.Contains(line, "connection error: not connected") {
				disconnected = true
			}
			if strings.Contains(line, "End Time:") {
				incomplete = false
			}

			if _, err := tmpWriter.WriteString(line + "\n"); err != nil {
				return fmt.Errorf("failed to write log line: %w", err)
			}
		}

		if err := scanner.Err(); err != nil {
			return fmt.Errorf("scanning log file: %w", err)
		}

		return nil
	}

	// Process stdout and stderr
	if err := processLogFile(clientLogFile.Path); err != nil {
		return false, false, 0, "", err
	}

	succeeded := false
	cancelled := false
	warningsNum := pbsWarningRawCount

	// Build and write final status line
	var sb strings.Builder

	timestamp := time.Now().Format(time.RFC3339)
	if !hasError {
		sb.WriteString(timestamp)
	}

	if hasError {
		sb.WriteString(errorString)
	} else if incomplete || disconnected {
		sb.WriteString(": TASK ERROR: task aborted")
		cancelled = true
	} else {
		succeeded = true
		if warningsNum > 0 {
			sb.WriteString(": TASK WARNINGS: ")
			sb.WriteString(strconv.Itoa(warningsNum))
		} else {
			if isGraceful {
				sb.WriteString(": TASK OK")
			} else {
				succeeded = false
				sb.WriteString(": TASK ERROR: Agent crashed unexpectedly")
			}
		}
	}
	sb.WriteString("\n")

	if _, err := tmpWriter.WriteString(sb.String()); err != nil {
		return false, false, warningsNum, errorPath, fmt.Errorf("failed to write final status: %w", err)
	}

	if err := tmpWriter.Flush(); err != nil {
		return false, false, warningsNum, errorPath, fmt.Errorf("failed to flush temporary writer: %w", err)
	}

	// Close the temp file before renaming
	if err := tmpFile.Close(); err != nil {
		return false, false, warningsNum, errorPath, fmt.Errorf("closing temporary file: %w", err)
	}
	tmpFile = nil // Prevent cleanup in deferred function

	// Ensure the temporary file has the same permissions, ownership, and timestamps
	if err := os.Chmod(tmpName, origMode); err != nil {
		return false, false, warningsNum, errorPath, fmt.Errorf("setting permissions on temporary file: %w", err)
	}
	if err := os.Chown(tmpName, origUid, origGid); err != nil {
		return false, false, warningsNum, errorPath, fmt.Errorf("setting ownership on temporary file: %w", err)
	}
	if err := os.Chtimes(tmpName, origAccessTime, origModTime); err != nil {
		return false, false, warningsNum, errorPath, fmt.Errorf("setting timestamps on temporary file: %w", err)
	}

	// Replace the original log file with the processed temporary file.
	if err := os.Rename(tmpName, logFilePath); err != nil {
		return false, false, warningsNum, errorPath, fmt.Errorf("replacing original file: %w", err)
	}

	return succeeded, cancelled, warningsNum, errorPath, nil
}
