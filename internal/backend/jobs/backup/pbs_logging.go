//go:build linux

package backup

import (
	"bufio"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/pbs-plus/pbs-plus/internal/backend/helpers"
	"github.com/pbs-plus/pbs-plus/internal/syslog"
	"github.com/pbs-plus/pbs-plus/internal/utils"
)

var (
	// errorPathRegex = regexp.MustCompile(`upload failed: error at "([^"]+)"`)
	commonErrorMap = map[string]string{
		"exit status 255": "lost connection with backup agent",
		"signal: killed":  ErrCanceled.Error(),
	}

	pbsCompletionMarkers = []string{
		"TASK OK",
		"TASK WARNINGS",
		"backup finished successfully",
		"successfully finished backup",
	}

	pbsTaskErrorMarkers = []string{
		"TASK ERROR:",
	}

	pbsUploadErrorMarkers = []string{
		"upload failed:",
	}

	pbsWarningMarkers = []string{
		"WARNING: ",
	}
)

func containsAny(s string, markers []string) (string, bool) {
	for _, m := range markers {
		if strings.Contains(s, m) {
			return m, true
		}
	}
	return "", false
}

func processPBSProxyLogs(isGraceful bool, upid string, clientLogFile *syslog.JobLogger, customErr error) (bool, bool, int, error) {
	customErrStr := ""
	if customErr != nil {
		customErrStr = customErr.Error()
		if mapped, ok := commonErrorMap[customErrStr]; ok {
			customErrStr = mapped
		}
	}

	logFilePath := utils.GetTaskLogPath(upid)
	inFile, err := os.Open(logFilePath)
	if err != nil {
		return false, false, 0, fmt.Errorf("opening input log file: %w", err)
	}
	defer inFile.Close()

	info, err := inFile.Stat()
	if err != nil {
		return false, false, 0, fmt.Errorf("getting stat of file %s: %w", logFilePath, err)
	}
	origMode := info.Mode()
	origModTime := info.ModTime()
	statT, ok := info.Sys().(*syscall.Stat_t)
	if !ok {
		return false, false, 0, fmt.Errorf("failed to retrieve underlying stat for file %s", logFilePath)
	}
	origUid := int(statT.Uid)
	origGid := int(statT.Gid)
	origAccessTime := time.Unix(statT.Atim.Sec, statT.Atim.Nsec)

	dir := filepath.Dir(logFilePath)
	tmpFile, err := os.CreateTemp(dir, "processed_*.tmp")
	if err != nil {
		return false, false, 0, fmt.Errorf("creating temporary file: %w", err)
	}
	tmpName := tmpFile.Name()
	defer func() {
		if tmpFile != nil {
			tmpFile.Close()
			os.Remove(tmpName)
		}
	}()

	const maxCapacity = 1024 * 1024
	buf := make([]byte, 0, 64*1024)
	tmpWriter := bufio.NewWriter(tmpFile)

	hasError := false
	incomplete := true
	alreadyHasClientLogs := false

	scanner := bufio.NewScanner(inFile)
	scanner.Buffer(buf, maxCapacity)

	for scanner.Scan() {
		skip := false
		line := scanner.Text()
		if helpers.IsJunkLog(line) {
			continue
		}
		if _, has := containsAny(line, pbsCompletionMarkers); has {
			incomplete = false
		} else if _, has := containsAny(line, pbsTaskErrorMarkers); has {
			hasError = true
			skip = true
		}

		if line == "--- proxmox-backup-client log starts here ---" {
			alreadyHasClientLogs = true
		}

		if skip {
			continue
		}

		tmpWriter.WriteString(line)
		tmpWriter.WriteByte('\n')
	}
	if err := scanner.Err(); err != nil {
		return false, false, 0, fmt.Errorf("scanning input file: %w", err)
	}

	var errorString string
	pbsWarningRawCount := 0

	clientFile, err := os.Open(clientLogFile.Path)
	if err != nil {
		return false, false, 0, fmt.Errorf("failed to open client log file: %w", err)
	}
	defer clientFile.Close()

	if !alreadyHasClientLogs {
		tmpWriter.WriteString("--- proxmox-backup-client log starts here ---\n")

		clientScanner := bufio.NewScanner(clientFile)
		clientScanner.Buffer(buf, maxCapacity)

		for clientScanner.Scan() {
			skip := false
			line := clientScanner.Text()

			if _, has := containsAny(line, pbsWarningMarkers); has {
				pbsWarningRawCount++
			}

			if _, has := containsAny(line, pbsUploadErrorMarkers); has {
				line = "WARNING [proxmox-backup-client error]:" + line
				pbsWarningRawCount++
			} else if markerFound, has := containsAny(line, pbsTaskErrorMarkers); has {
				line = strings.Replace(line, markerFound, "WARNING [proxmox-backup-client error]:", 1)
				pbsWarningRawCount++
			} else if _, has := containsAny(line, pbsCompletionMarkers); has {
				incomplete = false
				skip = true
			}

			if skip {
				continue
			}

			tmpWriter.WriteString(line)
			tmpWriter.WriteByte('\n')
		}

		if err := clientScanner.Err(); err != nil {
			return false, false, 0, fmt.Errorf("scanning client log file: %w", err)
		}
	}

	succeeded := false
	cancelled := false
	warningsNum := pbsWarningRawCount
	timestamp := time.Now().Format(time.RFC3339)

	switch {
	case hasError:
		tmpWriter.WriteString(errorString)
	case incomplete:
		tmpWriter.WriteString(timestamp)
		tmpWriter.WriteString(": TASK ERROR: ")
		if customErr != nil {
			tmpWriter.WriteString(customErrStr)
		} else {
			tmpWriter.WriteString(ErrUnexpected.Error())
		}
		cancelled = true
	default:
		tmpWriter.WriteString(timestamp)
		succeeded = true
		if warningsNum > 0 {
			tmpWriter.WriteString(": TASK WARNINGS: ")
			tmpWriter.WriteString(strconv.Itoa(warningsNum))
		} else if isGraceful {
			tmpWriter.WriteString(": TASK OK")
		} else {
			succeeded = false
			tmpWriter.WriteString(": TASK ERROR: Agent crashed unexpectedly")
		}
	}

	tmpWriter.WriteByte('\n')

	if err := tmpWriter.Flush(); err != nil {
		return false, false, warningsNum, fmt.Errorf("flushing temporary writer: %w", err)
	}
	if err := tmpFile.Close(); err != nil {
		return false, false, warningsNum, fmt.Errorf("closing temporary file: %w", err)
	}
	tmpFile = nil

	if err := os.Chmod(tmpName, origMode); err != nil {
		return false, false, warningsNum, fmt.Errorf("setting permissions on temporary file: %w", err)
	}
	if err := os.Chown(tmpName, origUid, origGid); err != nil {
		return false, false, warningsNum, fmt.Errorf("setting ownership on temporary file: %w", err)
	}
	if err := os.Chtimes(tmpName, origAccessTime, origModTime); err != nil {
		return false, false, warningsNum, fmt.Errorf("setting timestamps on temporary file: %w", err)
	}
	if err := os.Rename(tmpName, logFilePath); err != nil {
		return false, false, warningsNum, fmt.Errorf("replacing original file: %w", err)
	}

	return succeeded, cancelled, warningsNum, nil
}
