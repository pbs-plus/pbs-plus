package utils

import (
	"bufio"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/fsnotify/fsnotify"
)

func WaitForLogFile(taskUpid string, maxWait time.Duration) error {
	// Path to the active tasks
	logPath := "/var/log/proxmox-backup/tasks/active"

	if _, found := checkForLine(logPath, taskUpid); found {
		return nil
	}

	// Create new watcher
	watcher, err := fsnotify.NewWatcher()
	if err != nil {
		return fmt.Errorf("error creating watcher: %w", err)
	}
	defer watcher.Close()

	// Start watching the file
	err = watcher.Add(logPath)
	if err != nil {
		return fmt.Errorf("error watching file %s: %w", logPath, err)
	}

	// Create a timeout channel
	timeout := time.After(maxWait)

	// First check if the line already exists
	if _, found := checkForLine(logPath, taskUpid); found {
		return nil
	}

	for {
		select {
		case event, ok := <-watcher.Events:
			if !ok {
				return fmt.Errorf("watcher channel closed")
			}

			if event.Op&fsnotify.Write == fsnotify.Write {
				if _, found := checkForLine(logPath, taskUpid); found {
					return nil
				}
			}

		case err, ok := <-watcher.Errors:
			if !ok {
				return fmt.Errorf("watcher error channel closed")
			}
			return fmt.Errorf("watcher error: %w", err)

		case <-timeout:
			return fmt.Errorf("timeout waiting for log file after %v", maxWait)
		}
	}
}

func checkForLine(filePath, taskUpid string) (*os.File, bool) {
	file, err := os.Open(filePath)
	if err != nil {
		return nil, false
	}
	defer file.Close()

	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		if strings.Contains(scanner.Text(), taskUpid) {
			return file, true
		}
	}

	return nil, false
}
