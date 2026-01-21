//go:build linux

package database

import (
	"context"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/coreos/go-systemd/v22/dbus"
	godbus "github.com/godbus/dbus/v5"
	"github.com/pbs-plus/pbs-plus/internal/store/system"
)

func sanitizeUnitName(id string) (string, error) {
	if strings.Contains(id, "/") || strings.Contains(id, "\\") || strings.Contains(id, "..") {
		return "", fmt.Errorf("invalid backup ID: %s", id)
	}
	return strings.ReplaceAll(id, " ", "-"), nil
}

func getUnitName(backupID string) (string, error) {
	sanitized, err := sanitizeUnitName(backupID)
	if err != nil {
		return "", err
	}
	return fmt.Sprintf("pbs-plus-backup-%s", sanitized), nil
}

func getRetryUnitName(backupID string, attempt int) (string, error) {
	sanitized, err := sanitizeUnitName(backupID)
	if err != nil {
		return "", err
	}
	return fmt.Sprintf("pbs-plus-backup-%s-retry-%d", sanitized, attempt), nil
}

func stopBackupTimer(ctx context.Context, sanitized string) {
	conn, err := system.GetSystemdConn()
	if err != nil {
		return
	}

	primaryTimer := fmt.Sprintf("pbs-plus-backup-%s.timer", sanitized)
	_, _ = conn.StopUnitContext(ctx, primaryTimer, "replace", nil)
}

func stopAllRetries(ctx context.Context, sanitized string) {
	conn, err := system.GetSystemdConn()
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

func (backup *Backup) setSchedule(ctx context.Context) error {
	unitName, err := backup.createScheduleUnits(ctx)

	conn, err := system.GetSystemdConn()
	if err != nil {
		return err
	}

	if err := conn.ReloadContext(ctx); err != nil {
		return fmt.Errorf("SetSchedule: daemon-reload failed: %w", err)
	}

	if unitName != "" {
		_, err = conn.StartUnitContext(ctx, unitName+".timer", "replace", nil)
	}

	return err
}

func (backup *Backup) createScheduleUnits(ctx context.Context) (string, error) {
	unitName, err := getUnitName(backup.ID)
	if err != nil {
		return "", fmt.Errorf("SetSchedule: %w", err)
	}

	if backup.Schedule == "" {
		_ = deleteBackupSchedule(ctx, backup.ID)
		return "", nil
	}

	timerPath := filepath.Join("/etc/systemd/system", unitName+".timer")
	servicePath := filepath.Join("/etc/systemd/system", unitName+".service")

	serviceContent := fmt.Sprintf(`[Unit]
Description=%s Backup Service
After=network.target

[Service]
Type=oneshot
ExecStart=/usr/bin/pbs-plus -backup-job=%s
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
		return unitName, nil
	}

	if err := os.WriteFile(servicePath, []byte(serviceContent), 0644); err != nil {
		return "", fmt.Errorf("SetSchedule: error writing service: %w", err)
	}
	if err := os.WriteFile(timerPath, []byte(timerContent), 0644); err != nil {
		return "", fmt.Errorf("SetSchedule: error writing timer: %w", err)
	}

	return unitName, nil
}

func deleteBackupSchedule(ctx context.Context, id string) error {
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

	if conn, err := system.GetSystemdConn(); err == nil {
		_ = conn.ReloadContext(ctx)
	}

	return nil
}

func (backup *Backup) SetBackupRetrySchedule(ctx context.Context, extraExclusions []string) error {
	sanitized, err := sanitizeUnitName(backup.ID)
	if err != nil {
		return fmt.Errorf("SetBackupRetrySchedule: %w", err)
	}

	currentAttempt := getCurrentBackupRetryAttempt(ctx, sanitized)
	newAttempt := currentAttempt + 1

	if newAttempt > backup.Retry {
		fmt.Printf("Backup %s reached max retry count (%d). No further retry scheduled.\n",
			backup.ID, backup.Retry)
		backup.RemoveAllRetrySchedules(ctx)
		return nil
	}

	stopAllRetries(ctx, sanitized)

	retryUnitName, err := getRetryUnitName(backup.ID, newAttempt)
	if err != nil {
		return fmt.Errorf("SetBackupRetrySchedule: %w", err)
	}

	timerName := retryUnitName + ".timer"
	serviceName := retryUnitName + ".service"
	delay := fmt.Sprintf("%dm", backup.RetryInterval)

	execArgs := []string{"/usr/bin/pbs-plus", "-backup-job=" + backup.ID, "-retry=" + strconv.Itoa(newAttempt)}
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

	conn, err := system.GetSystemdConn()
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
		return fmt.Errorf("SetBackupRetrySchedule: error creating timer: %w", err)
	}

	fmt.Printf("Scheduled retry %d/%d for backup %s in %s\n",
		newAttempt, backup.Retry, backup.ID, delay)
	return nil
}

func getCurrentBackupRetryAttempt(ctx context.Context, sanitized string) int {
	conn, err := system.GetSystemdConn()
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

func (backup *Backup) RemoveAllRetrySchedules(ctx context.Context) {
	sanitized, _ := sanitizeUnitName(backup.ID)
	stopAllRetries(ctx, sanitized)
}

func (backup *Backup) getNextSchedule(ctx context.Context) (*time.Time, error) {
	conn, err := system.GetSystemdConn()
	if err != nil {
		return nil, err
	}

	sanitized, err := sanitizeUnitName(backup.ID)
	if err != nil {
		return nil, fmt.Errorf("GetNextSchedule: %w", err)
	}

	primaryTimer := fmt.Sprintf("pbs-plus-backup-%s.timer", sanitized)
	retryPattern := fmt.Sprintf("pbs-plus-backup-%s-retry*.timer", sanitized)

	units, err := conn.ListUnitsByPatternsContext(ctx, []string{}, []string{primaryTimer, retryPattern})
	if err != nil {
		return nil, fmt.Errorf("GetNextSchedule: failed to list units: %w", err)
	}

	var earliest time.Time
	found := false

	for _, unit := range units {
		if !strings.HasSuffix(unit.Name, ".timer") {
			continue
		}

		prop, err := conn.GetUnitTypePropertyContext(ctx, unit.Name, "Timer", "NextElapseUSecRealtime")
		if err != nil {
			continue
		}

		val, ok := prop.Value.Value().(uint64)
		if !ok || val == 0 || val == math.MaxUint64 {
			continue
		}

		nextTime := time.Unix(0, int64(val)*int64(time.Microsecond))
		if !found || nextTime.Before(earliest) {
			earliest = nextTime
			found = true
		}
	}

	if !found {
		return nil, nil
	}

	return &earliest, nil
}

func PurgeAllLegacyTimerUnits(ctx context.Context) error {
	timerBasePath := "/etc/systemd/system"
	pattern := filepath.Join(timerBasePath, "pbs-plus-job-*.timer")

	matches, err := filepath.Glob(pattern)
	if err != nil {
		return fmt.Errorf("failed to glob legacy units: %w", err)
	}

	if len(matches) == 0 {
		return nil
	}

	conn, err := system.GetSystemdConn()
	if err != nil {
		return fmt.Errorf("failed to connect to dbus: %w", err)
	}

	fmt.Printf("Found %d legacy unit(s) to migrate...\n", len(matches))

	for _, timerFile := range matches {
		fileName := filepath.Base(timerFile)
		serviceFile := strings.TrimSuffix(timerFile, ".timer") + ".service"

		_, _ = conn.StopUnitContext(ctx, fileName, "replace", nil)
		_, _ = conn.DisableUnitFilesContext(ctx, []string{fileName}, false)

		_ = os.Remove(timerFile)
		_ = os.Remove(serviceFile)

		fmt.Printf("Purged legacy unit: %s\n", fileName)
	}

	err = conn.ReloadContext(ctx)
	if err != nil {
		return fmt.Errorf("failed to reload daemon after purge: %w", err)
	}

	return nil
}

func SetBatchBackupSchedules(ctx context.Context, jobs []Backup) error {
	unitNames := make([]string, 0, len(jobs))

	for _, job := range jobs {
		if unitName, err := job.createScheduleUnits(ctx); err != nil {
			fmt.Printf("Batch error for %s: %v\n", job.ID, err)
		} else if unitName != "" {
			unitNames = append(unitNames, unitName)
		}
	}

	conn, err := system.GetSystemdConn()
	if err != nil {
		return err
	}

	if err := conn.ReloadContext(ctx); err != nil {
		return fmt.Errorf("SetSchedule: daemon-reload failed: %w", err)
	}

	for _, unitName := range unitNames {
		_, err = conn.StartUnitContext(ctx, unitName+".timer", "replace", nil)
	}
	return nil
}
