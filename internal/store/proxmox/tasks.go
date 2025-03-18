//go:build linux

package proxmox

import (
	"context"
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/fsnotify/fsnotify"
	"github.com/sonroyaalmerol/pbs-plus/internal/store/types"
)

func (ps *ProxmoxSession) GetJobTask(
	ctx context.Context,
	readyChan chan struct{},
	job types.Job,
	target types.Target,
) (Task, error) {
	tasksParentPath := "/var/log/proxmox-backup/tasks"

	watcher, err := fsnotify.NewWatcher()
	if err != nil {
		return Task{}, fmt.Errorf("failed to create watcher: %w", err)
	}
	defer watcher.Close()

	// Recursively add all directories under tasksParentPath.
	err = filepath.WalkDir(tasksParentPath, func(path string, info os.DirEntry, walkErr error) error {
		if walkErr != nil {
			log.Printf("error walking the path %q: %v", path, walkErr)
			// Continue walking so that one error does not abort the entire walk.
			return nil
		}
		if info.IsDir() {
			if addErr := watcher.Add(path); addErr != nil {
				log.Printf("failed to add directory %q: %v", path, addErr)
			}
		}
		return nil
	})
	if err != nil {
		return Task{}, fmt.Errorf("failed to walk folder: %w", err)
	}

	// Determine the hostname, factoring in potential fallbacks.
	hostname, err := os.Hostname()
	if err != nil {
		hostnameData, readErr := os.ReadFile("/etc/hostname")
		if readErr != nil {
			hostname = "localhost"
		} else {
			hostname = strings.TrimSpace(string(hostnameData))
		}
	}

	// Determine the backup identifier.
	isAgent := strings.HasPrefix(target.Path, "agent://")
	backupID := hostname
	if isAgent {
		backupID = strings.TrimSpace(strings.Split(target.Name, " - ")[0])
	}

	// Build the matching substring using the job's store and backupID.
	searchString := fmt.Sprintf(
		":backup:%s%shost-%s",
		job.Store,
		encodeToHexEscapes(":"),
		encodeToHexEscapes(backupID),
	)

	// checkFile returns a Task if filePath matches our search criteria.
	checkFile := func(filePath string, searchString string) (Task, error) {
		// We skip temporary files.
		if !strings.Contains(filePath, ".tmp_") &&
			strings.Contains(filePath, searchString) {
			log.Printf("Matching file found: %s", filePath)
			fileName := filepath.Base(filePath)
			task, err := ps.GetTaskByUPID(fileName)
			if err != nil {
				return Task{}, fmt.Errorf("error getting task for UPID %q: %w", fileName, err)
			}
			return task, nil
		}
		return Task{}, os.ErrNotExist
	}

	// scanRecursive walks the entire tree starting at 'root' and returns the Task
	// as soon as it finds a matching file. We use io.EOF as a sentinel error to break early.
	scanRecursive := func(root, searchString string) (Task, error) {
		var result Task
		err := filepath.WalkDir(root, func(path string, info os.DirEntry, walkErr error) error {
			if walkErr != nil {
				// Log and ignore individual errors.
				return nil
			}
			if !info.IsDir() {
				if task, err := checkFile(path, searchString); err == nil {
					result = task
					// Use io.EOF as a sentinel error to exit early.
					return io.EOF
				}
			}
			return nil
		})
		if err == io.EOF {
			return result, nil
		}
		return Task{}, os.ErrNotExist
	}

	// Perform an initial full scan of the tasks directory.
	if task, err := scanRecursive(tasksParentPath, searchString); err == nil {
		return task, nil
	}

	// Signal readiness.
	close(readyChan)

	// Set up a ticker for periodic rescanning to catch any files that might be missed
	// because of race condition or fsnotify event drops.
	ticker := time.NewTicker(500 * time.Millisecond)
	defer ticker.Stop()

	// Main event loop: wait for file events or ticker ticks.
	for {
		select {
		case event, ok := <-watcher.Events:
			if !ok {
				return Task{}, fmt.Errorf("fsnotify events channel closed")
			}
			// Look for file events that might indicate a new task file.
			if event.Op&(fsnotify.Create|fsnotify.Write|fsnotify.Rename) != 0 {
				// If a new directory is created, add it (and later scan it recursively).
				stat, statErr := os.Stat(event.Name)
				if statErr != nil {
					// The file/directory may have been removed already.
					continue
				}
				if stat.IsDir() {
					if addErr := watcher.Add(event.Name); addErr != nil {
						log.Printf("failed to add new directory %q: %v", event.Name, addErr)
					}
					// Immediately scan the new directory.
					if task, err := scanRecursive(event.Name, searchString); err ==
						nil {
						return task, nil
					}
				} else {
					// Check individual file event.
					if task, err := checkFile(event.Name, searchString); err == nil {
						return task, nil
					}
				}
			}
		case err := <-watcher.Errors:
			log.Printf("fsnotify error: %v", err)
		case <-ticker.C:
			// Periodically rescan the entire tree.
			if task, err := scanRecursive(tasksParentPath, searchString); err == nil {
				return task, nil
			}
		case <-ctx.Done():
			return Task{}, ctx.Err()
		}
	}
}

func (proxmoxSess *ProxmoxSession) GetTaskByUPID(upid string) (Task, error) {
	resp, err := ParseUPID(upid)
	if err != nil {
		return Task{}, err
	}

	resp.Status = "stopped"
	if IsUPIDRunning(upid) {
		resp.Status = "running"
		return resp, nil
	}

	lastLog, err := parseLastLogMessage(upid)
	if err != nil {
		resp.ExitStatus = "unknown"
	}
	if lastLog == "TASK OK" {
		resp.ExitStatus = "OK"
	} else {
		resp.ExitStatus = strings.TrimPrefix(lastLog, "TASK ERROR: ")
	}

	endTime, err := proxmoxSess.GetTaskEndTime(resp)
	if err != nil {
		return Task{}, fmt.Errorf("GetTaskByUPID: error getting task end time -> %w", err)
	}

	resp.EndTime = endTime

	return resp, nil
}

func (proxmoxSess *ProxmoxSession) GetTaskEndTime(task Task) (int64, error) {
	if proxmoxSess.APIToken == nil {
		return -1, fmt.Errorf("GetTaskEndTime: token is required")
	}

	logPath, err := GetLogPath(task.UPID)
	if err != nil {
		return -1, fmt.Errorf("GetTaskEndTime: error getting log path (%s) -> %w", logPath, err)
	}

	logStat, err := os.Stat(logPath)
	if err == nil {
		return logStat.ModTime().Unix(), nil
	}

	return -1, fmt.Errorf("GetTaskEndTime: error getting tasks: not found (%s) -> %w", logPath, err)
}
