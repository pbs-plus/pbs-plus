package backup

import (
	"bufio"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"time"

	"github.com/sonroyaalmerol/pbs-plus/internal/utils"
)

const PBS_JUNK_LOGS = `^\d{4}-\d{2}-\d{2}T[0-9:\-+]+: (successfully added chunk [0-9a-f]+|` +
	`PUT /dynamic_index|dynamic_append \d+ chunks|POST /dynamic_chunk|upload_chunk done:)`

var removePattern = regexp.MustCompile(PBS_JUNK_LOGS)

func processPBSProxyLogs(upid, clientLogPath string) error {
	logFilePath := utils.GetTaskLogPath(upid)
	inFile, err := os.Open(logFilePath)
	if err != nil {
		return fmt.Errorf("opening input log file: %w", err)
	}
	defer inFile.Close()

	// Create a temporary file in the same directory
	dir := filepath.Dir(logFilePath)
	tmpFile, err := os.CreateTemp(dir, "processed_*.tmp")
	if err != nil {
		return fmt.Errorf("creating temporary file: %w", err)
	}
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
		if removePattern.MatchString(line) {
			continue // Skip junk lines
		}
		if _, err := tmpWriter.WriteString(line + "\n"); err != nil {
			return fmt.Errorf("writing to temporary file: %w", err)
		}
	}
	if err := scanner.Err(); err != nil {
		return fmt.Errorf("scanning input file: %w", err)
	}

	// Write header for proxmox backup client logs
	if _, err := tmpWriter.WriteString("--- proxmox-backup-client log starts here ---\n"); err != nil {
		return fmt.Errorf("failed to write log header: %w", err)
	}

	// Process output files and analyze for status info
	timestamp := time.Now().Format(time.RFC3339)
	hasError := false
	incomplete := true
	disconnected := false
	var errorString string

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

			// Check for indicators before writing the line
			if strings.Contains(line, "Error: upload failed:") {
				errorString = strings.Replace(line, "Error:", "TASK ERROR:", 1)
				hasError = true
				continue // Skip this line as we'll use it in the final status
			}
			if strings.Contains(line, "connection failed") {
				disconnected = true
			}
			if strings.Contains(line, "End Time:") {
				incomplete = false
			}

			// Write each line with timestamp
			if _, err := tmpWriter.WriteString(fmt.Sprintf("%s: %s\n", timestamp, line)); err != nil {
				return fmt.Errorf("failed to write log line: %w", err)
			}
		}

		if err := scanner.Err(); err != nil {
			return fmt.Errorf("scanning log file: %w", err)
		}

		return nil
	}

	// Process stdout and stderr
	if err := processLogFile(clientLogPath); err != nil {
		return err
	}

	// Build and write final status line
	var sb strings.Builder
	sb.WriteString(timestamp)
	if hasError {
		sb.WriteString(": ")
		sb.WriteString(errorString)
	} else if incomplete && disconnected {
		sb.WriteString(": TASK ERROR: Job cancelled")
	} else {
		sb.WriteString(": TASK OK")
	}
	sb.WriteString("\n")

	if _, err := tmpWriter.WriteString(sb.String()); err != nil {
		return fmt.Errorf("failed to write final status: %w", err)
	}

	if err := tmpWriter.Flush(); err != nil {
		return fmt.Errorf("failed to flush temporary writer: %w", err)
	}

	// Close the temp file before renaming
	if err := tmpFile.Close(); err != nil {
		return fmt.Errorf("closing temporary file: %w", err)
	}
	tmpFile = nil // Prevent cleanup in deferred function

	// Replace the original log file with the filtered temporary file
	if err := os.Rename(tmpFile.Name(), logFilePath); err != nil {
		return fmt.Errorf("replacing original file: %w", err)
	}

	return nil
}
