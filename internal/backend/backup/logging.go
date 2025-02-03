package backup

import (
	"bufio"
	"fmt"
	"io"
	"log"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/sonroyaalmerol/pbs-plus/internal/utils"
)

func collectLogs(stdout, stderr io.ReadCloser) ([]string, error) {
	defer stdout.Close()
	defer stderr.Close()

	linesCh := make(chan string)
	errCh := make(chan error, 2)
	var wg sync.WaitGroup

	scanner := func(r io.Reader) {
		defer wg.Done()
		scanner := bufio.NewScanner(r)
		for scanner.Scan() {
			line := scanner.Text()
			log.Println(line) // Log to console
			linesCh <- line
		}
		if err := scanner.Err(); err != nil {
			errCh <- fmt.Errorf("error reading logs: %w", err)
		}
	}

	wg.Add(2)
	go scanner(stdout)
	go scanner(stderr)

	go func() {
		wg.Wait()
		close(linesCh)
		close(errCh)
	}()

	var logLines []string
	for line := range linesCh {
		logLines = append(logLines, line)
	}

	var errs []error
	for err := range errCh {
		errs = append(errs, err)
	}

	if len(errs) > 0 {
		return nil, fmt.Errorf("errors reading logs: %v", errs)
	}

	return logLines, nil
}

func writeLogsToFile(upid string, logLines []string) error {
	if logLines == nil {
		return fmt.Errorf("logLines is nil")
	}

	if err := utils.WaitForLogFile(upid, 1*time.Minute); err != nil {
		return fmt.Errorf("log file cannot be opened: %w", err)
	}

	time.Sleep(5 * time.Second)

	logFilePath := utils.GetTaskLogPath(upid)
	logFile, err := os.OpenFile(logFilePath, os.O_WRONLY|os.O_CREATE|os.O_APPEND, 0644)
	if err != nil {
		return fmt.Errorf("failed to open log file: %w", err)
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

	finalStatus := fmt.Sprintf("%s: TASK OK\n", timestamp)
	if hasError {
		finalStatus = fmt.Sprintf("%s: %s\n", timestamp, errorString)
	}

	if _, err := writer.WriteString(finalStatus); err != nil {
		return fmt.Errorf("failed to write final status: %w", err)
	}

	return nil
}
