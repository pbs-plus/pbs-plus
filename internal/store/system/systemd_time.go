//go:build linux

package system

import (
	"bufio"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/pbs-plus/pbs-plus/internal/store/constants"
	"github.com/pbs-plus/pbs-plus/internal/store/types"
)

func sanitizeUnitName(id string) (string, error) {
	if strings.Contains(id, "/") || strings.Contains(id, "\\") || strings.Contains(id, "..") {
		return "", fmt.Errorf("invalid job ID: %s", id)
	}
	return strings.ReplaceAll(id, " ", "-"), nil
}

func getUnitName(jobID string) (string, error) {
	sanitized, err := sanitizeUnitName(jobID)
	if err != nil {
		return "", err
	}
	return fmt.Sprintf("pbs-plus-job-%s", sanitized), nil
}

func getRetryUnitName(jobID string, attempt int) (string, error) {
	sanitized, err := sanitizeUnitName(jobID)
	if err != nil {
		return "", err
	}
	return fmt.Sprintf("pbs-plus-job-%s-retry-%d", sanitized, attempt), nil
}

func stopAllJobTimers(sanitized string) {
	primaryTimer := fmt.Sprintf("pbs-plus-job-%s.timer", sanitized)

	checkCmd := exec.Command("systemctl", "is-active", primaryTimer)
	checkCmd.Env = os.Environ()
	if err := checkCmd.Run(); err == nil {
		stopCmd := exec.Command("systemctl", "stop", primaryTimer)
		stopCmd.Env = os.Environ()
		_ = stopCmd.Run()

		time.Sleep(50 * time.Millisecond)
	}

	pattern := fmt.Sprintf("pbs-plus-job-%s-retry-*.timer", sanitized)
	listCmd := exec.Command("systemctl", "list-units", pattern, "--all", "--no-legend", "--state=active")
	listCmd.Env = os.Environ()
	output, err := listCmd.Output()
	if err == nil {
		scanner := bufio.NewScanner(strings.NewReader(string(output)))
		for scanner.Scan() {
			line := scanner.Text()
			fields := strings.Fields(line)
			if len(fields) > 0 {
				stopCmd := exec.Command("systemctl", "stop", fields[0])
				stopCmd.Env = os.Environ()
				_ = stopCmd.Run()
			}
		}

		if len(strings.TrimSpace(string(output))) > 0 {
			time.Sleep(50 * time.Millisecond)
		}
	}
}

func SetSchedule(job types.Job) error {
	unitName, err := getUnitName(job.ID)
	if err != nil {
		return fmt.Errorf("SetSchedule: %w", err)
	}

	sanitized, _ := sanitizeUnitName(job.ID)
	stopAllJobTimers(sanitized)

	if job.Schedule == "" {
		return nil
	}

	args := []string{
		"--unit=" + unitName,
		"--on-calendar=" + job.Schedule,
		"--timer-property=Persistent=false",
		"--description=" + job.ID + " Backup Job",
		"/usr/bin/pbs-plus",
		"-job=" + job.ID,
	}

	cmd := exec.Command("systemd-run", args...)
	cmd.Env = os.Environ()

	output, err := cmd.CombinedOutput()
	if err != nil {
		return fmt.Errorf("SetSchedule: error creating timer (output: %s) -> %w",
			string(output), err)
	}

	return nil
}

func DeleteSchedule(id string) error {
	sanitized, err := sanitizeUnitName(id)
	if err != nil {
		return fmt.Errorf("DeleteSchedule: %w", err)
	}
	stopAllJobTimers(sanitized)

	timerBasePath := "/etc/systemd/system"

	timerFile := filepath.Join(timerBasePath, fmt.Sprintf("pbs-plus-job-%s.timer", sanitized))
	serviceFile := filepath.Join(timerBasePath, fmt.Sprintf("pbs-plus-job-%s.service", sanitized))

	_ = os.Remove(timerFile)
	_ = os.Remove(serviceFile)

	retryTimerPattern := filepath.Join(timerBasePath,
		fmt.Sprintf("pbs-plus-job-%s-retry-*.timer", sanitized))
	retryServicePattern := filepath.Join(timerBasePath,
		fmt.Sprintf("pbs-plus-job-%s-retry-*.service", sanitized))

	retryTimers, _ := filepath.Glob(retryTimerPattern)
	retryServices, _ := filepath.Glob(retryServicePattern)

	for _, file := range retryTimers {
		_ = os.Remove(file)
	}

	for _, file := range retryServices {
		_ = os.Remove(file)
	}

	reloadCmd := exec.Command("systemctl", "daemon-reload")
	reloadCmd.Env = os.Environ()
	_ = reloadCmd.Run()

	return nil
}

func getCurrentRetryAttempt(sanitized string) int {
	pattern := fmt.Sprintf("pbs-plus-job-%s-retry-*.timer", sanitized)
	listCmd := exec.Command("systemctl", "list-units", pattern, "--all", "--no-legend", "--no-pager")
	listCmd.Env = os.Environ()
	output, err := listCmd.Output()
	if err != nil {
		return 0
	}

	maxAttempt := 0
	scanner := bufio.NewScanner(strings.NewReader(string(output)))
	for scanner.Scan() {
		line := scanner.Text()
		fields := strings.Fields(line)
		if len(fields) > 0 {
			unitName := fields[0]

			// Extract attempt number from unit name: pbs-plus-job-{id}-retry-{N}.timer
			parts := strings.Split(unitName, "-retry-")
			if len(parts) == 2 {
				attemptStr := strings.TrimSuffix(parts[1], ".timer")
				if attemptInt, err := strconv.Atoi(attemptStr); err == nil {
					if attemptInt > maxAttempt {
						maxAttempt = attemptInt
					}
				}
			}
		}
	}

	return maxAttempt
}

func SetRetrySchedule(job types.Job, extraExclusions []string) error {
	sanitized, err := sanitizeUnitName(job.ID)
	if err != nil {
		return fmt.Errorf("SetRetrySchedule: %w", err)
	}

	currentAttempt := getCurrentRetryAttempt(sanitized)
	newAttempt := currentAttempt + 1

	if newAttempt > job.Retry {
		fmt.Printf("Job %s reached max retry count (%d). No further retry scheduled.\n",
			job.ID, job.Retry)
		RemoveAllRetrySchedules(job)
		return nil
	}

	pattern := fmt.Sprintf("pbs-plus-job-%s-retry-*.timer", sanitized)
	listCmd := exec.Command("systemctl", "list-units", pattern, "--all", "--no-legend")
	listCmd.Env = os.Environ()
	output, _ := listCmd.Output()

	scanner := bufio.NewScanner(strings.NewReader(string(output)))
	for scanner.Scan() {
		line := scanner.Text()
		fields := strings.Fields(line)
		if len(fields) > 0 {
			stopCmd := exec.Command("systemctl", "stop", fields[0])
			stopCmd.Env = os.Environ()
			stopCmd.Stderr = nil
			_ = stopCmd.Run()
		}
	}

	retryUnitName, err := getRetryUnitName(job.ID, newAttempt)
	if err != nil {
		return fmt.Errorf("SetRetrySchedule: %w", err)
	}

	delay := fmt.Sprintf("%dm", job.RetryInterval)

	args := []string{
		"--unit=" + retryUnitName,
		"--on-active=" + delay,
		"--timer-property=Persistent=false",
		"--description=" + fmt.Sprintf("%s Backup Job Retry (Attempt %d)", job.ID, newAttempt),
		"/usr/bin/pbs-plus",
		"-job=" + job.ID,
		"-retry=" + strconv.Itoa(newAttempt),
	}

	for _, exclusion := range extraExclusions {
		if !strings.Contains(exclusion, `"`) {
			args = append(args, "-skip="+exclusion)
		}
	}

	cmd := exec.Command("systemd-run", args...)
	cmd.Env = os.Environ()

	output, err = cmd.CombinedOutput()
	if err != nil {
		return fmt.Errorf("SetRetrySchedule: error creating retry timer (output: %s) -> %w",
			string(output), err)
	}

	fmt.Printf("Scheduled retry %d/%d for job %s in %d minutes\n",
		newAttempt, job.Retry, job.ID, job.RetryInterval)

	return nil
}

func RemoveAllRetrySchedules(job types.Job) {
	sanitized, err := sanitizeUnitName(job.ID)
	if err != nil {
		return
	}

	pattern := fmt.Sprintf("pbs-plus-job-%s-retry-*.timer", sanitized)
	listCmd := exec.Command("systemctl", "list-units", pattern, "--all", "--no-legend")
	listCmd.Env = os.Environ()
	output, err := listCmd.Output()
	if err != nil {
		return
	}

	scanner := bufio.NewScanner(strings.NewReader(string(output)))
	for scanner.Scan() {
		line := scanner.Text()
		fields := strings.Fields(line)
		if len(fields) > 0 {
			stopCmd := exec.Command("systemctl", "stop", fields[0])
			stopCmd.Env = os.Environ()
			stopCmd.Stderr = nil
			_ = stopCmd.Run()
		}
	}

	timerBasePath := "/etc/systemd/system"

	retryTimerPattern := filepath.Join(timerBasePath,
		fmt.Sprintf("pbs-plus-job-%s-retry-*.timer", sanitized))
	retryServicePattern := filepath.Join(timerBasePath,
		fmt.Sprintf("pbs-plus-job-%s-retry-*.service", sanitized))

	retryTimers, _ := filepath.Glob(retryTimerPattern)
	retryServices, _ := filepath.Glob(retryServicePattern)

	for _, file := range retryTimers {
		_ = os.Remove(file)
	}

	for _, file := range retryServices {
		_ = os.Remove(file)
	}

	if len(retryTimers) > 0 || len(retryServices) > 0 {
		reloadCmd := exec.Command("systemctl", "daemon-reload")
		reloadCmd.Env = os.Environ()
		_ = reloadCmd.Run()
	}
}

var lastSchedMux sync.Mutex
var lastSchedUpdate time.Time
var lastSchedString []byte

func GetNextSchedule(job types.Job) (*time.Time, error) {
	var output []byte

	lastSchedMux.Lock()
	if !lastSchedUpdate.IsZero() && time.Since(lastSchedUpdate) <= 5*time.Second {
		output = lastSchedString
	} else {
		cmd := exec.Command("systemctl", "list-timers", "--all", "--no-pager")
		cmd.Env = os.Environ()

		var err error
		output, err = cmd.Output()
		if err != nil {
			lastSchedMux.Unlock()
			return nil, fmt.Errorf("GetNextSchedule: error running systemctl command: %w", err)
		}

		lastSchedUpdate = time.Now()
		lastSchedString = output
	}
	lastSchedMux.Unlock()

	scanner := bufio.NewScanner(strings.NewReader(string(output)))
	layout := "Mon 2006-01-02 15:04:05 MST"

	sanitized, err := sanitizeUnitName(job.ID)
	if err != nil {
		return nil, fmt.Errorf("GetNextSchedule: %w", err)
	}

	primaryTimer := fmt.Sprintf("pbs-plus-job-%s.timer", sanitized)
	retryPrefix := fmt.Sprintf("pbs-plus-job-%s-retry", sanitized)

	var nextTimes []time.Time

	for scanner.Scan() {
		line := scanner.Text()
		if strings.Contains(line, primaryTimer) || strings.Contains(line, retryPrefix) {
			fields := strings.Fields(line)
			if len(fields) < 4 {
				continue
			}

			nextStr := strings.Join(fields[0:4], " ")
			if strings.TrimSpace(nextStr) == "-" || nextStr == "n/a" {
				continue
			}

			nextTime, err := time.Parse(layout, nextStr)
			if err != nil {
				continue
			}

			nextTimes = append(nextTimes, nextTime)
		}
	}

	if err := scanner.Err(); err != nil {
		return nil, fmt.Errorf("GetNextSchedule: error reading command output: %w", err)
	}

	if err := migrateLegacyUnit(job, sanitized); err != nil {
		fmt.Printf("Warning: failed to migrate legacy unit for job %s: %v\n", job.ID, err)
	}

	if len(nextTimes) == 0 {
		return nil, nil
	}

	earliest := nextTimes[0]
	for _, t := range nextTimes[1:] {
		if t.Before(earliest) {
			earliest = t
		}
	}

	return &earliest, nil
}

func migrateLegacyUnit(job types.Job, sanitized string) error {
	timerBasePath := constants.TimerBasePath

	timerFile := filepath.Join(timerBasePath, fmt.Sprintf("pbs-plus-job-%s.timer", sanitized))
	serviceFile := filepath.Join(timerBasePath, fmt.Sprintf("pbs-plus-job-%s.service", sanitized))

	timerExists := false
	serviceExists := false

	if _, err := os.Stat(timerFile); err == nil {
		timerExists = true
	}
	if _, err := os.Stat(serviceFile); err == nil {
		serviceExists = true
	}

	if !timerExists && !serviceExists {
		return nil
	}

	fmt.Printf("Migrating legacy unit files for job %s to transient units...\n", job.ID)

	needsRecreation := job.Schedule != ""

	if timerExists {
		unitName := fmt.Sprintf("pbs-plus-job-%s.timer", sanitized)

		stopCmd := exec.Command("systemctl", "stop", unitName)
		stopCmd.Env = os.Environ()
		stopCmd.Stderr = nil
		_ = stopCmd.Run()

		disableCmd := exec.Command("systemctl", "disable", unitName)
		disableCmd.Env = os.Environ()
		disableCmd.Stderr = nil
		_ = disableCmd.Run()
	}

	if timerExists {
		if err := os.Remove(timerFile); err != nil && !os.IsNotExist(err) {
			return fmt.Errorf("failed to remove legacy timer file: %w", err)
		}
	}
	if serviceExists {
		if err := os.Remove(serviceFile); err != nil && !os.IsNotExist(err) {
			return fmt.Errorf("failed to remove legacy service file: %w", err)
		}
	}

	retryTimerPattern := filepath.Join(timerBasePath,
		fmt.Sprintf("pbs-plus-job-%s-retry-*.timer", sanitized))
	retryServicePattern := filepath.Join(timerBasePath,
		fmt.Sprintf("pbs-plus-job-%s-retry-*.service", sanitized))

	retryTimers, _ := filepath.Glob(retryTimerPattern)
	retryServices, _ := filepath.Glob(retryServicePattern)

	for _, file := range retryTimers {
		unitName := filepath.Base(file)
		stopCmd := exec.Command("systemctl", "stop", unitName)
		stopCmd.Env = os.Environ()
		stopCmd.Stderr = nil
		_ = stopCmd.Run()

		disableCmd := exec.Command("systemctl", "disable", unitName)
		disableCmd.Env = os.Environ()
		disableCmd.Stderr = nil
		_ = disableCmd.Run()

		_ = os.Remove(file)
	}

	for _, file := range retryServices {
		_ = os.Remove(file)
	}

	reloadCmd := exec.Command("systemctl", "daemon-reload")
	reloadCmd.Env = os.Environ()
	if err := reloadCmd.Run(); err != nil {
		return fmt.Errorf("failed to reload systemd daemon: %w", err)
	}

	if needsRecreation {
		fmt.Printf("Recreating job %s as transient unit with schedule: %s\n",
			job.ID, job.Schedule)
		if err := SetSchedule(job); err != nil {
			return fmt.Errorf("failed to recreate schedule as transient unit: %w", err)
		}
	}

	fmt.Printf("Successfully migrated job %s from legacy to transient units\n", job.ID)
	return nil
}
