//go:build linux

package system

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/coreos/go-systemd/v22/dbus"
	godbus "github.com/godbus/dbus/v5"
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
	return fmt.Sprintf("pbs-plus-backup-%s", sanitized), nil
}

func getRetryUnitName(jobID string, attempt int) (string, error) {
	sanitized, err := sanitizeUnitName(jobID)
	if err != nil {
		return "", err
	}
	return fmt.Sprintf("pbs-plus-backup-%s-retry-%d", sanitized, attempt), nil
}

func stopBackupTimer(ctx context.Context, sanitized string) {
	conn, err := getConn()
	if err != nil {
		return
	}

	primaryTimer := fmt.Sprintf("pbs-plus-backup-%s.timer", sanitized)
	_, _ = conn.StopUnitContext(ctx, primaryTimer, "replace", nil)
}

func stopAllRetries(ctx context.Context, sanitized string) {
	conn, err := getConn()
	if err != nil {
		return
	}

	pattern := fmt.Sprintf("pbs-plus-backup-%s-retry-*.timer", sanitized)
	units, err := conn.ListUnitsByPatternsContext(ctx, nil, []string{pattern})
	if err == nil && len(units) > 0 {
		for _, unit := range units {
			_, _ = conn.StopUnitContext(ctx, unit.Name, "replace", nil)
			_ = conn.ResetFailedUnitContext(ctx, unit.Name)
		}
	}

	runPath := "/run/systemd/system"
	filePattern := fmt.Sprintf("pbs-plus-backup-%s-retry-*", sanitized)
	matches, _ := filepath.Glob(filepath.Join(runPath, filePattern))
	for _, match := range matches {
		_ = os.Remove(match)
	}
}

func SetSchedule(ctx context.Context, backup types.Job) error {
	err := setSchedule(ctx, backup)

	conn, err := getConn()
	if err != nil {
		return err
	}

	if err := conn.ReloadContext(ctx); err != nil {
		return fmt.Errorf("SetSchedule: daemon-reload failed: %w", err)
	}

	return err
}

func setSchedule(ctx context.Context, backup types.Job) error {
	unitName, err := getUnitName(backup.ID)
	if err != nil {
		return fmt.Errorf("SetSchedule: %w", err)
	}

	if backup.Schedule == "" {
		_ = DeleteSchedule(ctx, backup.ID)
		return nil
	}

	timerPath := filepath.Join("/etc/systemd/system", unitName+".timer")
	servicePath := filepath.Join("/etc/systemd/system", unitName+".service")

	serviceContent := fmt.Sprintf(`[Unit]
Description=%s Backup Service
After=network.target

[Service]
Type=oneshot
ExecStart=/usr/bin/pbs-plus -job=%s
`, backup.ID, backup.ID)

	timerContent := fmt.Sprintf(`[Unit]
Description=%s Backup Timer

[Timer]
OnCalendar=%s
Persistent=false

[Install]
WantedBy=timers.target
`, backup.ID, backup.Schedule)

	existingService, _ := os.ReadFile(servicePath)
	existingTimer, _ := os.ReadFile(timerPath)
	if string(existingService) == serviceContent && string(existingTimer) == timerContent {
		return nil
	}

	if err := os.WriteFile(servicePath, []byte(serviceContent), 0644); err != nil {
		return fmt.Errorf("SetSchedule: error writing service: %w", err)
	}
	if err := os.WriteFile(timerPath, []byte(timerContent), 0644); err != nil {
		return fmt.Errorf("SetSchedule: error writing timer: %w", err)
	}

	conn, err := getConn()
	if err != nil {
		return fmt.Errorf("SetSchedule: failed to connect to dbus: %w", err)
	}

	if err := conn.ReloadContext(ctx); err != nil {
		return fmt.Errorf("SetSchedule: daemon-reload failed: %w", err)
	}

	_, err = conn.StartUnitContext(ctx, unitName+".timer", "replace", nil)
	return err
}

func DeleteSchedule(ctx context.Context, id string) error {
	sanitized, err := sanitizeUnitName(id)
	if err != nil {
		return fmt.Errorf("DeleteSchedule: %w", err)
	}

	stopBackupTimer(ctx, sanitized)
	stopAllRetries(ctx, sanitized)

	timerBasePath := "/etc/systemd/system"
	filesToRemove := []string{
		filepath.Join(timerBasePath, fmt.Sprintf("pbs-plus-backup-%s.timer", sanitized)),
		filepath.Join(timerBasePath, fmt.Sprintf("pbs-plus-backup-%s.service", sanitized)),
	}

	for _, file := range filesToRemove {
		_ = os.Remove(file)
	}

	if conn, err := getConn(); err == nil {
		_ = conn.ReloadContext(ctx)
	}

	return nil
}

func SetRetrySchedule(ctx context.Context, backup types.Job, extraExclusions []string) error {
	sanitized, err := sanitizeUnitName(backup.ID)
	if err != nil {
		return fmt.Errorf("SetRetrySchedule: %w", err)
	}

	currentAttempt := getCurrentRetryAttempt(ctx, sanitized)
	newAttempt := currentAttempt + 1

	if newAttempt > backup.Retry {
		fmt.Printf("Backup %s reached max retry count (%d). No further retry scheduled.\n",
			backup.ID, backup.Retry)
		RemoveAllRetrySchedules(ctx, backup)
		return nil
	}

	stopAllRetries(ctx, sanitized)

	retryUnitName, err := getRetryUnitName(backup.ID, newAttempt)
	if err != nil {
		return fmt.Errorf("SetRetrySchedule: %w", err)
	}

	timerName := retryUnitName + ".timer"
	serviceName := retryUnitName + ".service"
	delay := fmt.Sprintf("%dm", backup.RetryInterval)

	execArgs := []string{"/usr/bin/pbs-plus", "-job=" + backup.ID, "-retry=" + strconv.Itoa(newAttempt)}
	for _, exclusion := range extraExclusions {
		if !strings.Contains(exclusion, `"`) {
			execArgs = append(execArgs, "-skip="+exclusion)
		}
	}

	servicePath := filepath.Join("/run/systemd/system", serviceName)
	serviceContent := fmt.Sprintf(`[Unit]
Description=%s Backup Retry Service (Attempt %d)

[Service]
Type=oneshot
ExecStart=%s
`, backup.ID, newAttempt, strings.Join(execArgs, " "))

	if err := os.WriteFile(servicePath, []byte(serviceContent), 0644); err != nil {
		return fmt.Errorf("SetRetrySchedule: error writing service: %w", err)
	}

	conn, err := getConn()
	if err != nil {
		return fmt.Errorf("SetRetrySchedule: failed to connect to dbus: %w", err)
	}

	if err := conn.ReloadContext(ctx); err != nil {
		return fmt.Errorf("SetRetrySchedule: daemon-reload failed: %w", err)
	}

	timerProps := []dbus.Property{
		dbus.PropDescription(fmt.Sprintf("%s Backup Retry Timer (Attempt %d)", backup.ID, newAttempt)),
		{Name: "OnActiveSec", Value: godbus.MakeVariant(delay)},
	}

	_, err = conn.StartTransientUnitContext(ctx, timerName, "replace", timerProps, nil)
	if err != nil {
		return fmt.Errorf("SetRetrySchedule: error creating timer: %w", err)
	}

	fmt.Printf("Scheduled retry %d/%d for backup %s in %s\n",
		newAttempt, backup.Retry, backup.ID, delay)
	return nil
}

func getCurrentRetryAttempt(ctx context.Context, sanitized string) int {
	conn, err := getConn()
	if err != nil {
		return 0
	}

	pattern := fmt.Sprintf("pbs-plus-backup-%s-retry-*.timer", sanitized)
	units, err := conn.ListUnitsByPatternsContext(ctx, nil, []string{pattern})
	if err != nil {
		return 0
	}

	maxAttempt := 0
	parseAttempt := func(name string) {
		parts := strings.Split(name, "-retry-")
		if len(parts) == 2 {
			attemptStr := strings.TrimSuffix(parts[1], ".timer")
			if attemptInt, err := strconv.Atoi(attemptStr); err == nil {
				if attemptInt > maxAttempt {
					maxAttempt = attemptInt
				}
			}
		}
	}

	for _, unit := range units {
		parseAttempt(unit.Name)
	}

	matches, _ := filepath.Glob(filepath.Join("/run/systemd/system", pattern))
	for _, match := range matches {
		parseAttempt(filepath.Base(match))
	}

	return maxAttempt
}

func RemoveAllRetrySchedules(ctx context.Context, backup types.Job) {
	sanitized, _ := sanitizeUnitName(backup.ID)
	stopAllRetries(ctx, sanitized)
}

func GetNextSchedule(ctx context.Context, backup types.Job) (*time.Time, error) {
	conn, err := getConn()
	if err != nil {
		return nil, err
	}

	sanitized, err := sanitizeUnitName(backup.ID)
	if err != nil {
		return nil, fmt.Errorf("GetNextSchedule: %w", err)
	}

	primaryTimer := fmt.Sprintf("pbs-plus-backup-%s.timer", sanitized)
	retryPattern := fmt.Sprintf("pbs-plus-backup-%s-retry-*.timer", sanitized)

	units, err := conn.ListUnitsByPatternsContext(ctx, nil, []string{primaryTimer, retryPattern})
	if err != nil {
		return nil, err
	}

	if len(units) == 0 {
		return nil, nil
	}

	var earliest *time.Time

	for _, unit := range units {
		if unit.ActiveState != "active" {
			continue
		}

		prop, err := conn.GetUnitPropertyContext(ctx, unit.Name, "NextElapseUSecRealtime")
		if err != nil {
			continue
		}

		usec := getUint64FromVariant(prop.Value)

		if usec > 0 && usec != ^uint64(0) {
			nextTime := time.Unix(0, int64(usec)*int64(time.Microsecond))
			if earliest == nil || nextTime.Before(*earliest) {
				t := nextTime
				earliest = &t
			}
		}
	}

	return earliest, nil
}

func getUint64FromVariant(v godbus.Variant) uint64 {
	switch val := v.Value().(type) {
	case uint64:
		return val
	case int64:
		if val >= 0 {
			return uint64(val)
		}
	}
	return 0
}

func SetBatchSchedules(ctx context.Context, jobs []types.Job) error {
	for _, job := range jobs {
		if err := setSchedule(ctx, job); err != nil {
			fmt.Printf("Batch error for %s: %v\n", job.ID, err)
		}
	}

	conn, err := getConn()
	if err != nil {
		return err
	}

	if err := conn.ReloadContext(ctx); err != nil {
		return fmt.Errorf("SetSchedule: daemon-reload failed: %w", err)
	}
	return nil
}
