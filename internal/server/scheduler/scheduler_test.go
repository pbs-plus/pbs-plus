package scheduler

import (
	"testing"
	"time"

	"github.com/pbs-plus/pbs-plus/internal/server/database"
)

func TestShouldRunScheduled(t *testing.T) {
	now := time.Date(2024, 1, 1, 12, 0, 0, 0, time.Local)

	tests := []struct {
		name     string
		schedule string
		lastRun  int64
		now      time.Time
		due      bool
	}{
		{"hourly due", "hourly", now.Add(-90 * time.Minute).Unix(), now.Add(-30 * time.Minute), true},
		{"hourly not due", "hourly", now.Add(-30 * time.Minute).Unix(), now.Add(-10 * time.Minute), false},
		{"never run waits for first occurrence", "hourly", 0, now.Add(-65 * time.Minute), false},
		{"catch-up after downtime submits once", "hourly", now.Add(-5 * time.Hour).Unix(), now, true},
		{"time schedule due after last run", "12:00", now.Add(-24 * time.Hour).Unix(), now.Add(5 * time.Minute), true},
		{"time schedule not due", "12:00", 0, now.Add(-30 * time.Minute), false},
		{"invalid schedule", "not-a-schedule", 0, now, false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := &Scheduler{}
			_, due := s.shouldRunScheduled(tt.schedule, tt.lastRun, tt.now)
			if due != tt.due {
				t.Fatalf("shouldRunScheduled(%q, %d, %v) = %v, want %v", tt.schedule, tt.lastRun, tt.now, due, tt.due)
			}
		})
	}
}

func TestShouldRetryBackup(t *testing.T) {
	now := time.Now()
	s := &Scheduler{}

	tests := []struct {
		name   string
		backup database.Backup
		retry  bool
	}{
		{
			name: "failed within retry budget",
			backup: database.Backup{
				Retry:         3,
				RetryInterval: 60,
				History: database.JobHistory{
					LastRunEndtime: now.Add(-2 * time.Hour).Unix(),
					LastRunStatus:  database.JobStatusFailed,
					RetryCount:     1,
				},
			},
			retry: true,
		},
		{
			name: "retry budget exhausted",
			backup: database.Backup{
				Retry:         3,
				RetryInterval: 60,
				History: database.JobHistory{
					LastRunEndtime: now.Add(-2 * time.Hour).Unix(),
					LastRunStatus:  database.JobStatusFailed,
					RetryCount:     3,
				},
			},
			retry: false,
		},
		{
			name: "interval not elapsed",
			backup: database.Backup{
				Retry:         3,
				RetryInterval: 120,
				History: database.JobHistory{
					LastRunEndtime: now.Add(-30 * time.Minute).Unix(),
					LastRunStatus:  database.JobStatusFailed,
				},
			},
			retry: false,
		},
		{
			name: "success not retryable",
			backup: database.Backup{
				Retry:         3,
				RetryInterval: 60,
				History: database.JobHistory{
					LastRunEndtime: now.Add(-2 * time.Hour).Unix(),
					LastRunStatus:  database.JobStatusSuccess,
				},
			},
			retry: false,
		},
		{
			name: "canceled not retryable",
			backup: database.Backup{
				Retry:         3,
				RetryInterval: 60,
				History: database.JobHistory{
					LastRunEndtime: now.Add(-2 * time.Hour).Unix(),
					LastRunStatus:  database.JobStatusCanceled,
				},
			},
			retry: false,
		},
		{
			name: "unknown status falls back to state string",
			backup: database.Backup{
				Retry:         3,
				RetryInterval: 60,
				History: database.JobHistory{
					LastRunEndtime: now.Add(-2 * time.Hour).Unix(),
					LastRunStatus:  database.JobStatusUnknown,
					LastRunState:   "backup failed",
				},
			},
			retry: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := s.shouldRetryBackup(tt.backup, now); got != tt.retry {
				t.Fatalf("shouldRetryBackup() = %v, want %v", got, tt.retry)
			}
		})
	}
}

func TestLastRunRetryableLegacyStates(t *testing.T) {
	tests := []struct {
		state string
		want  bool
	}{
		{"backup failed", true},
		{"OK", false},
		{"operation canceled", false},
		{"WARNINGS: 2", false},
		{"", false},
	}

	for _, tt := range tests {
		t.Run(tt.state, func(t *testing.T) {
			if got := lastRunRetryable(database.JobStatusUnknown, tt.state); got != tt.want {
				t.Fatalf("lastRunRetryable(unknown, %q) = %v, want %v", tt.state, got, tt.want)
			}
		})
	}
}
