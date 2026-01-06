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

	"github.com/pbs-plus/pbs-plus/internal/backend/helpers"
	"github.com/pbs-plus/pbs-plus/internal/syslog"
	"github.com/pbs-plus/pbs-plus/internal/utils"
)

var (
	errorPathRegex = regexp.MustCompile(`upload failed: error at "([^"]+)"`)
)

func processPBSProxyLogs(isGraceful bool, upid string, clientLogFile *syslog.BackupLogger) (bool, bool, int, string, error) {
	logFilePath := utils.GetTaskLogPath(upid)
	inFile, err := os.Open(logFilePath)
	if err != nil {
		return false, false, 0, "", fmt.Errorf("opening input log file: %w", err)
	}
	defer inFile.Close()

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

	dir := filepath.Dir(logFilePath)
	tmpFile, err := os.CreateTemp(dir, "processed_*.tmp")
	if err != nil {
		return false, false, 0, "", fmt.Errorf("creating temporary file: %w", err)
	}
	tmpName := tmpFile.Name()
	defer func() {
		if tmpFile != nil {
			tmpFile.Close()
			os.Remove(tmpName)
		}
	}()

	tmpWriter := bufio.NewWriter(tmpFile)

	scanner := bufio.NewScanner(inFile)
	const maxCapacity = 1024 * 1024
	buf := make([]byte, 0, 64*1024)
	scanner.Buffer(buf, maxCapacity)

	for scanner.Scan() {
		line := scanner.Text()
		if helpers.IsJunkLog(line) {
			continue
		}
		tmpWriter.WriteString(line)
		tmpWriter.WriteByte('\n')
	}
	if err := scanner.Err(); err != nil {
		return false, false, 0, "", fmt.Errorf("scanning input file: %w", err)
	}

	tmpWriter.WriteString("--- proxmox-backup-client log starts here ---\n")

	hasError := false
	incomplete := true
	disconnected := false
	var errorString string
	var errorPath string
	pbsWarningRawCount := 0

	clientFile, err := os.Open(clientLogFile.Path)
	if err != nil {
		return false, false, 0, "", fmt.Errorf("failed to open client log file: %w", err)
	}
	defer clientFile.Close()

	clientScanner := bufio.NewScanner(clientFile)
	clientScanner.Buffer(buf, maxCapacity)

	for clientScanner.Scan() {
		line := clientScanner.Text()

		if strings.Contains(line, "warning: ") {
			pbsWarningRawCount++
		}

		if strings.Contains(line, "Error: upload failed:") {
			errorString = strings.Replace(line, "Error:", "TASK ERROR:", 1)
			if matches := errorPathRegex.FindStringSubmatch(line); len(matches) >= 2 {
				errorPath = matches[1]
			}
			hasError = true
			continue
		} else if strings.Contains(line, "TASK ERROR:") {
			errorString = line
			hasError = true
			continue
		}

		if strings.Contains(line, "connection failed") || strings.Contains(line, "connection error: not connected") {
			disconnected = true
		}
		if strings.Contains(line, "End Time:") || strings.Contains(line, "TASK OK") || strings.Contains(line, "backup finished successfully") {
			incomplete = false
		}

		tmpWriter.WriteString(line)
		tmpWriter.WriteByte('\n')
	}

	if err := clientScanner.Err(); err != nil {
		return false, false, 0, "", fmt.Errorf("scanning client log file: %w", err)
	}

	succeeded := false
	cancelled := false
	warningsNum := pbsWarningRawCount

	timestamp := time.Now().Format(time.RFC3339)

	if hasError {
		tmpWriter.WriteString(errorString)
		tmpWriter.WriteByte('\n')
	} else if incomplete || disconnected {
		tmpWriter.WriteString(timestamp)
		tmpWriter.WriteString(": TASK ERROR: ")
		tmpWriter.WriteString(ErrCanceled.Error())
		tmpWriter.WriteByte('\n')
		cancelled = true
	} else {
		tmpWriter.WriteString(timestamp)
		succeeded = true
		if warningsNum > 0 {
			tmpWriter.WriteString(": TASK WARNINGS: ")
			tmpWriter.WriteString(strconv.Itoa(warningsNum))
		} else {
			if isGraceful {
				tmpWriter.WriteString(": TASK OK")
			} else {
				succeeded = false
				tmpWriter.WriteString(": TASK ERROR: Agent crashed unexpectedly")
			}
		}
		tmpWriter.WriteByte('\n')
	}

	if err := tmpWriter.Flush(); err != nil {
		return false, false, warningsNum, errorPath, fmt.Errorf("failed to flush temporary writer: %w", err)
	}

	if err := tmpFile.Close(); err != nil {
		return false, false, warningsNum, errorPath, fmt.Errorf("closing temporary file: %w", err)
	}
	tmpFile = nil

	if err := os.Chmod(tmpName, origMode); err != nil {
		return false, false, warningsNum, errorPath, fmt.Errorf("setting permissions on temporary file: %w", err)
	}
	if err := os.Chown(tmpName, origUid, origGid); err != nil {
		return false, false, warningsNum, errorPath, fmt.Errorf("setting ownership on temporary file: %w", err)
	}
	if err := os.Chtimes(tmpName, origAccessTime, origModTime); err != nil {
		return false, false, warningsNum, errorPath, fmt.Errorf("setting timestamps on temporary file: %w", err)
	}

	if err := os.Rename(tmpName, logFilePath); err != nil {
		return false, false, warningsNum, errorPath, fmt.Errorf("replacing original file: %w", err)
	}

	return succeeded, cancelled, warningsNum, errorPath, nil
}
