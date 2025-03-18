//go:build linux

package system

import (
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/sonroyaalmerol/pbs-plus/internal/store/constants"
	"github.com/sonroyaalmerol/pbs-plus/internal/store/types"
)

func generateRetryTimer(job types.Job, schedule string, attempt int) error {
	if strings.Contains(job.ID, "/") ||
		strings.Contains(job.ID, "\\") ||
		strings.Contains(job.ID, "..") {
		return fmt.Errorf("generateRetryTimer: invalid job ID -> %s", job.ID)
	}

	content := fmt.Sprintf(`[Unit]
Description=%s Backup Job Retry Timer (Attempt %d)

[Timer]
OnCalendar=%s
Persistent=false

[Install]
WantedBy=timers.target`, job.ID, attempt, schedule)

	fileName := fmt.Sprintf("pbs-plus-job-%s-retry-%d.timer", strings.ReplaceAll(job.ID, " ", "-"), attempt)
	fullPath := filepath.Join(constants.TimerBasePath, fileName)

	file, err := os.OpenFile(fullPath, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0644)
	if err != nil {
		return fmt.Errorf("generateRetryTimer: error opening timer file -> %w", err)
	}
	defer file.Close()

	if _, err = file.WriteString(content); err != nil {
		return fmt.Errorf("generateRetryTimer: error writing content -> %w", err)
	}

	return nil
}

func generateRetryService(job types.Job, attempt int) error {
	if strings.Contains(job.ID, "/") ||
		strings.Contains(job.ID, "\\") ||
		strings.Contains(job.ID, "..") {
		return fmt.Errorf("generateRetryService: invalid job ID -> %s", job.ID)
	}

	content := fmt.Sprintf(`[Unit]
Description=%s Backup Job Retry Service (Attempt %d)
After=network-online.target
Wants=network-online.target

[Service]
Type=oneshot
ExecStart=/usr/bin/pbs-plus -job="%s"`, job.ID, attempt, job.ID)

	fileName := fmt.Sprintf("pbs-plus-job-%s-retry-%d.service",
		strings.ReplaceAll(job.ID, " ", "-"), attempt)
	fullPath := filepath.Join(constants.TimerBasePath, fileName)

	file, err := os.OpenFile(fullPath, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0644)
	if err != nil {
		return fmt.Errorf("generateRetryService: error opening service file -> %w", err)
	}
	defer file.Close()

	if _, err = file.WriteString(content); err != nil {
		return fmt.Errorf("generateRetryService: error writing content -> %w", err)
	}

	return nil
}

func RemoveAllRetrySchedules(job types.Job) {
	retryPattern := filepath.Join(
		constants.TimerBasePath,
		fmt.Sprintf("pbs-plus-job-%s-retry-*.timer",
			strings.ReplaceAll(job.ID, " ", "-")),
	)
	retryFiles, err := filepath.Glob(retryPattern)
	if err == nil {
		for _, file := range retryFiles {
			cmd := exec.Command("/usr/bin/systemctl", "disable", "--now", file)
			cmd.Env = os.Environ()
			_ = cmd.Run()
			_ = os.Remove(file)
		}
	}

	svcRetryPattern := filepath.Join(
		constants.TimerBasePath,
		fmt.Sprintf("pbs-plus-job-%s-retry-*.service",
			strings.ReplaceAll(job.ID, " ", "-")),
	)
	svcRetryFiles, err := filepath.Glob(svcRetryPattern)
	if err == nil {
		for _, svcFile := range svcRetryFiles {
			_ = os.Remove(svcFile)
		}
	}

	cmd := exec.Command("/usr/bin/systemctl", "daemon-reload")
	cmd.Env = os.Environ()
	_ = cmd.Run()
}

func SetRetrySchedule(job types.Job) error {
	maxRetry := job.Retry
	retryPattern := filepath.Join(
		constants.TimerBasePath,
		fmt.Sprintf("pbs-plus-job-%s-retry-*.timer",
			strings.ReplaceAll(job.ID, " ", "-")),
	)
	retryFiles, err := filepath.Glob(retryPattern)
	if err != nil {
		return fmt.Errorf("SetRetrySchedule: error globbing retry timer files: %w", err)
	}

	// Determine the current highest attempt number.
	currentAttempt := 0
	for _, file := range retryFiles {
		base := filepath.Base(file) // e.g. "pbs-plus-job-<jobID>-retry-1.timer"
		idx := strings.Index(base, "retry-")
		if idx < 0 {
			continue
		}
		attemptStrWithSuffix := base[idx+len("retry-"):]
		attemptStr := strings.TrimSuffix(attemptStrWithSuffix, ".timer")
		if attemptInt, err := strconv.Atoi(attemptStr); err == nil {
			if attemptInt > currentAttempt {
				currentAttempt = attemptInt
			}
		}
	}
	newAttempt := currentAttempt + 1
	if newAttempt > maxRetry {
		fmt.Printf("Job %s reached max retry count (%d). No further retry scheduled.\n",
			job.ID, maxRetry)
		RemoveAllRetrySchedules(job)
		return nil
	}

	// Now remove all existing retry timer files so that the new one is unique.
	for _, file := range retryFiles {
		cmd := exec.Command("/usr/bin/systemctl", "disable", "--now", file)
		cmd.Env = os.Environ()
		_ = cmd.Run()

		if err := os.Remove(file); err != nil {
			return fmt.Errorf("SetRetrySchedule: error removing old retry timer file %s: %w", file, err)
		}
	}

	// Also clear any existing retry service unit files.
	svcRetryPattern := filepath.Join(
		constants.TimerBasePath,
		fmt.Sprintf("pbs-plus-job-%s-retry-*.service",
			strings.ReplaceAll(job.ID, " ", "-")),
	)
	svcRetryFiles, err := filepath.Glob(svcRetryPattern)
	if err != nil {
		return fmt.Errorf("SetRetrySchedule: error globbing retry service files: %w", err)
	}
	for _, svcFile := range svcRetryFiles {
		if err := os.Remove(svcFile); err != nil {
			return fmt.Errorf("SetRetrySchedule: error removing old retry service file %s: %w",
				svcFile, err)
		}
	}

	// Compute the new retry time
	retryTime := time.Now().Add(time.Duration(job.RetryInterval) * time.Minute)
	layout := "Mon 2006-01-02 15:04:05 MST"
	retrySchedule := retryTime.Format(layout)

	// If an original schedule exists and fires sooner than the retry, then skip.
	if job.Schedule != "" {
		originalTime, err := time.Parse(layout, job.Schedule)
		if err != nil {
			return fmt.Errorf("SetRetrySchedule: error parsing original schedule: %w", err)
		}
		if retryTime.After(originalTime) {
			fmt.Printf("Original schedule (%s) is sooner than retry schedule (%s). "+
				"No retry scheduled.\n",
				originalTime.Format(layout), retrySchedule)
			return nil
		}
	}

	// Create the new retry service and timer unit files.
	if err := generateRetryService(job, newAttempt); err != nil {
		return fmt.Errorf("SetRetrySchedule: error generating retry service: %w", err)
	}
	if err := generateRetryTimer(job, retrySchedule, newAttempt); err != nil {
		return fmt.Errorf("SetRetrySchedule: error generating retry timer: %w", err)
	}

	// Reload systemd daemon and enable the new retry timer.
	cmd := exec.Command("/usr/bin/systemctl", "daemon-reload")
	cmd.Env = os.Environ()
	if err := cmd.Run(); err != nil {
		return fmt.Errorf("SetRetrySchedule: error reloading daemon: %w", err)
	}

	timerFile := fmt.Sprintf("pbs-plus-job-%s-retry-%d.timer",
		strings.ReplaceAll(job.ID, " ", "-"), newAttempt)
	cmd = exec.Command("/usr/bin/systemctl", "enable", "--now", timerFile)
	cmd.Env = os.Environ()
	if err := cmd.Run(); err != nil {
		return fmt.Errorf("SetRetrySchedule: error enabling retry timer: %w", err)
	}

	return nil
}
