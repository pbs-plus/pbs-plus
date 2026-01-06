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
	return fmt.Sprintf("pbs-plus-job-%s", sanitized), nil
}

func getRetryUnitName(jobID string, attempt int) (string, error) {
	sanitized, err := sanitizeUnitName(jobID)
	if err != nil {
		return "", err
	}
	return fmt.Sprintf("pbs-plus-job-%s-retry-%d", sanitized, attempt), nil
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
}

func SetSchedule(ctx context.Context, backup types.Job) error {
	unitName, err := getUnitName(backup.ID)
	if err != nil {
		return fmt.Errorf("SetSchedule: %w", err)
	}

	if backup.Schedule == "" {
		stopBackupTimer(ctx, unitName)
		return nil
	}

	conn, err := getConn()
	if err != nil {
		return fmt.Errorf("SetSchedule: failed to connect to dbus: %w", err)
	}

	timerName := unitName + ".timer"
	serviceName := unitName + ".service"

	serviceProps := []dbus.Property{
		dbus.PropDescription(backup.ID + " Backup Service"),
		dbus.PropExecStart([]string{"/usr/bin/pbs-plus", "-backup=" + backup.ID}, false),
	}

	timerProps := []dbus.Property{
		dbus.PropDescription(backup.ID + " Backup Timer"),
		{Name: "OnCalendar", Value: godbus.MakeVariant(backup.Schedule)},
		{Name: "Persistent", Value: godbus.MakeVariant(false)},
	}

	_, err = conn.StartTransientUnitContext(ctx, serviceName, "replace", serviceProps, nil)
	if err != nil {
		return fmt.Errorf("SetSchedule: error creating service: %w", err)
	}

	_, err = conn.StartTransientUnitContext(ctx, timerName, "replace", timerProps, nil)
	if err != nil {
		return fmt.Errorf("SetSchedule: error creating timer: %w", err)
	}

	return nil
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

	retryPatterns := []string{
		filepath.Join(timerBasePath, fmt.Sprintf("pbs-plus-backup-%s-retry-*.timer", sanitized)),
		filepath.Join(timerBasePath, fmt.Sprintf("pbs-plus-backup-%s-retry-*.service", sanitized)),
	}
	for _, p := range retryPatterns {
		matches, _ := filepath.Glob(p)
		filesToRemove = append(filesToRemove, matches...)
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

	conn, err := getConn()
	if err != nil {
		return fmt.Errorf("SetRetrySchedule: failed to connect to dbus: %w", err)
	}

	retryUnitName, err := getRetryUnitName(backup.ID, newAttempt)
	if err != nil {
		return fmt.Errorf("SetRetrySchedule: %w", err)
	}

	timerName := retryUnitName + ".timer"
	serviceName := retryUnitName + ".service"
	delay := fmt.Sprintf("%dm", backup.RetryInterval)

	execArgs := []string{"/usr/bin/pbs-plus", "-backup=" + backup.ID, "-retry=" + strconv.Itoa(newAttempt)}
	for _, exclusion := range extraExclusions {
		if !strings.Contains(exclusion, `"`) {
			execArgs = append(execArgs, "-skip="+exclusion)
		}
	}

	serviceProps := []dbus.Property{
		dbus.PropDescription(fmt.Sprintf("%s Backup Retry (Attempt %d)", backup.ID, newAttempt)),
		dbus.PropExecStart(execArgs, false),
	}

	timerProps := []dbus.Property{
		dbus.PropDescription(fmt.Sprintf("%s Backup Retry (Attempt %d)", backup.ID, newAttempt)),
		{Name: "OnUnitActiveSec", Value: godbus.MakeVariant(delay)},
		{Name: "Persistent", Value: godbus.MakeVariant(false)},
	}

	_, err = conn.StartTransientUnitContext(ctx, serviceName, "replace", serviceProps, nil)
	if err != nil {
		return fmt.Errorf("SetRetrySchedule: error creating service: %w", err)
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

	matches, _ := filepath.Glob(filepath.Join("/run/systemd/transient", pattern))
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

	var earliest *time.Time

	for _, unit := range units {
		prop, err := conn.GetUnitPropertyContext(ctx, unit.Name, "NextElapseUSecRealtime")
		if err != nil {
			continue
		}

		usec, ok := prop.Value.Value().(uint64)
		if !ok || usec == 0 || usec == ^uint64(0) {
			continue
		}

		nextTime := time.Unix(0, int64(usec)*int64(time.Microsecond))
		if earliest == nil || nextTime.Before(*earliest) {
			earliest = &nextTime
		}
	}

	return earliest, nil
}

func PurgeAllLegacyUnits(ctx context.Context) error {
	timerBasePath := "/etc/systemd/system"
	pattern := filepath.Join(timerBasePath, "pbs-plus-backup-*.timer")

	matches, err := filepath.Glob(pattern)
	if err != nil {
		return fmt.Errorf("failed to glob legacy units: %w", err)
	}

	if len(matches) == 0 {
		return nil
	}

	conn, err := getConn()
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

func SetBatchSchedules(ctx context.Context, jobs []types.Job) error {
	if len(jobs) == 0 {
		return nil
	}

	conn, err := getConn()
	if err != nil {
		return fmt.Errorf("SetBatchSchedules: failed to connect to dbus: %w", err)
	}

	for _, job := range jobs {
		unitName, err := getUnitName(job.ID)
		if err != nil {
			fmt.Printf("Error generating unit name for %s: %v\n", job.ID, err)
			continue
		}

		if job.Schedule == "" {
			timerName := unitName + ".timer"
			_, _ = conn.StopUnitContext(ctx, timerName, "replace", nil)
			continue
		}

		timerName := unitName + ".timer"
		serviceName := unitName + ".service"

		serviceProps := []dbus.Property{
			dbus.PropDescription(fmt.Sprintf("PBS-Plus Backup: %s", job.ID)),
			dbus.PropExecStart([]string{"/usr/bin/pbs-plus", "-backup=" + job.ID}, false),
		}

		timerProps := []dbus.Property{
			dbus.PropDescription(fmt.Sprintf("PBS-Plus Timer: %s", job.ID)),
			{
				Name:  "OnCalendar",
				Value: godbus.MakeVariant(job.Schedule),
			},
			{
				Name:  "Persistent",
				Value: godbus.MakeVariant(false),
			},
		}

		_, err = conn.StartTransientUnitContext(ctx, serviceName, "replace", serviceProps, nil)
		if err != nil {
			fmt.Printf("Batch error starting service for %s: %v\n", job.ID, err)
			continue
		}

		_, err = conn.StartTransientUnitContext(ctx, timerName, "replace", timerProps, nil)
		if err != nil {
			fmt.Printf("Batch error starting timer for %s: %v\n", job.ID, err)
			continue
		}
	}

	return nil
}
