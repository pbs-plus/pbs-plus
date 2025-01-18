package backup

import (
	"bufio"
	"fmt"
	"io"
	"log"
	"strings"
	"time"

	"github.com/sonroyaalmerol/pbs-plus/internal/utils"
)

func collectLogs(stdout, stderr io.ReadCloser) []string {
	logLines := []string{}
	reader := bufio.NewScanner(io.MultiReader(stdout, stderr))

	for reader.Scan() {
		line := reader.Text()

		log.Println(line)
		logLines = append(logLines, line)
	}

	return logLines
}

func writeLogsToFile(upid string, logLines []string) error {
	if logLines == nil {
		return fmt.Errorf("logLines is nil")
	}

	logFilePath := utils.GetTaskLogPath(upid)
	logFile, err := utils.WaitForLogFile(logFilePath, 5*time.Second)
	if err != nil {
		return fmt.Errorf("log file cannot be opened: %w", err)
	}
	defer logFile.Close()

	writer := bufio.NewWriter(logFile)
	defer writer.Flush()

	if _, err := writer.WriteString("--- proxmox-backup-client log starts here ---\n"); err != nil {
		return fmt.Errorf("failed to write log header: %w", err)
	}

	hasError := false
	var errorString string
	timestamp := time.Now().Format(time.RFC3339)

	for _, logLine := range logLines {
		if strings.Contains(logLine, "Error: upload failed:") {
			errorString = strings.Replace(logLine, "Error:", "TASK ERROR:", 1)
			hasError = true
			continue
		}

		if _, err := writer.WriteString(fmt.Sprintf("%s: %s\n", timestamp, logLine)); err != nil {
			return fmt.Errorf("failed to write log line: %w", err)
		}
	}

	// Write final status
	if hasError {
		_, err = writer.WriteString(fmt.Sprintf("%s: %s", timestamp, errorString))
	} else {
		_, err = writer.WriteString(fmt.Sprintf("%s: TASK OK", timestamp))
	}

	if err != nil {
		return fmt.Errorf("failed to write final status: %w", err)
	}

	return nil
}
