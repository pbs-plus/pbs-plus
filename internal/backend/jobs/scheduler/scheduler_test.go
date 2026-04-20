package scheduler

import (
	"testing"
	"time"

	"github.com/pbs-plus/pbs-plus/internal/store/database"
)

func TestIsFailedState(t *testing.T) {
	tests := []struct {
		state    string
		expected bool
	}{
		{"OK", false},
		{"", false},
		{"WARNINGS: 3", false},
		{"WARNINGS: 0", false},
		{"WARNINGS: 10", false},
		{"operation canceled", false}, // Manual stop should not retry
		{"TASK ERROR: connection refused", true},
		{"exit status 1", true},
		{"some error message", true},
		{"lost connection with backup agent", true},
		{"cancelled", true}, // British spelling is treated as error (only exact "operation canceled" is excluded)
		// Edge cases: partial prefix matches should not be treated as warnings
		{"WARNING", true},            // No colon-space after WARNING
		{"WARNINGS:3", true},         // Missing space after colon
		{"WARNINGS:something", true}, // Missing space after colon
		{"WARNINGS", true},           // Just "WARNINGS" without count
		{"WARNINGS: ", false},        // Matches pattern, treated as warning
	}

	for _, tt := range tests {
		t.Run(tt.state, func(t *testing.T) {
			result := isFailedState(tt.state)
			if result != tt.expected {
				t.Errorf("isFailedState(%q) = %v, want %v", tt.state, result, tt.expected)
			}
		})
	}
}

func TestJobStatusEnum(t *testing.T) {
	tests := []struct {
		status         database.JobStatus
		shouldRetry    bool
		isSuccess      bool
		isCompleted    bool
		expectedString string
	}{
		{database.JobStatusUnknown, false, false, false, "UNKNOWN"},
		{database.JobStatusSuccess, false, true, true, "OK"},
		{database.JobStatusWarnings, false, true, true, "WARNINGS"},
		{database.JobStatusFailed, true, false, true, "FAILED"},
		{database.JobStatusCanceled, false, false, true, "CANCELED"},
	}

	for _, tt := range tests {
		t.Run(tt.expectedString, func(t *testing.T) {
			if got := tt.status.ShouldRetry(); got != tt.shouldRetry {
				t.Errorf("JobStatus(%s).ShouldRetry() = %v, want %v", tt.expectedString, got, tt.shouldRetry)
			}
			if got := tt.status.IsSuccess(); got != tt.isSuccess {
				t.Errorf("JobStatus(%s).IsSuccess() = %v, want %v", tt.expectedString, got, tt.isSuccess)
			}
			if got := tt.status.IsCompleted(); got != tt.isCompleted {
				t.Errorf("JobStatus(%s).IsCompleted() = %v, want %v", tt.expectedString, got, tt.isCompleted)
			}
			if got := tt.status.String(); got != tt.expectedString {
				t.Errorf("JobStatus(%d).String() = %q, want %q", tt.status, got, tt.expectedString)
			}
		})
	}
}

func TestJobStatusFromString(t *testing.T) {
	tests := []struct {
		input    string
		expected database.JobStatus
	}{
		{"", database.JobStatusUnknown},
		{"OK", database.JobStatusSuccess},
		{"WARNINGS: 5", database.JobStatusWarnings},
		{"WARNINGS: 0", database.JobStatusWarnings},
		{"WARNINGS: ", database.JobStatusWarnings},
		{"operation canceled", database.JobStatusCanceled},
		{"TASK ERROR: something", database.JobStatusFailed},
		{"exit status 1", database.JobStatusFailed},
		{"random error", database.JobStatusFailed},
	}

	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			result := database.JobStatusFromString(tt.input)
			if result != tt.expected {
				t.Errorf("JobStatusFromString(%q) = %v, want %v", tt.input, result, tt.expected)
			}
		})
	}
}

func TestShouldRetryBackup_IntervalNotElapsed(t *testing.T) {
	s := &Scheduler{}

	now := time.Now()
	b := database.Backup{
		Retry:         3,
		RetryInterval: 5,
		History: database.JobHistory{
			LastRunEndtime: now.Unix() - 60, // 1 minute ago; interval is 5 minutes
			LastRunState:   "exit status 1",
			LastRunStatus:  database.JobStatusFailed,
			RetryCount:     1,
		},
	}

	result := s.shouldRetryBackup(b, now)
	if result {
		t.Error("should not retry when interval has not elapsed")
	}
}

func TestShouldRetryBackup_TypedStatus(t *testing.T) {
	s := &Scheduler{}

	now := time.Now()

	tests := []struct {
		name         string
		status       database.JobStatus
		retryCount   int
		shouldRetry  bool
	}{
		{"Failed with retries remaining", database.JobStatusFailed, 1, true},
		{"Failed at retry limit", database.JobStatusFailed, 3, false},
		{"Success should not retry", database.JobStatusSuccess, 0, false},
		{"Warnings should not retry", database.JobStatusWarnings, 0, false},
		{"Canceled should not retry", database.JobStatusCanceled, 0, false},
		{"Unknown falls back to string parsing", database.JobStatusUnknown, 0, true}, // exit status 1 is failed
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			b := database.Backup{
				Retry:         3,
				RetryInterval: 1,
				History: database.JobHistory{
					LastRunEndtime: now.Unix() - 120, // 2 minutes ago
					LastRunState:   "exit status 1",   // used for Unknown fallback
					LastRunStatus:  tt.status,
					RetryCount:     tt.retryCount,
				},
			}

			result := s.shouldRetryBackup(b, now)
			if result != tt.shouldRetry {
				t.Errorf("shouldRetryBackup() = %v, want %v (status=%v, retryCount=%d)",
					result, tt.shouldRetry, tt.status, tt.retryCount)
			}
		})
	}
}

func TestShouldRetryBackup_NoLastEndTime(t *testing.T) {
	s := &Scheduler{}

	now := time.Now()
	b := database.Backup{
		Retry:         3,
		RetryInterval: 1,
		History: database.JobHistory{
			LastRunEndtime: 0, // No end time
			LastRunState:   "exit status 1",
			LastRunStatus:  database.JobStatusFailed,
			RetryCount:     1,
		},
	}

	result := s.shouldRetryBackup(b, now)
	if result {
		t.Error("should not retry when LastRunEndtime is 0")
	}
}

func TestShouldRetryBackup_PersistentRetryCount(t *testing.T) {
	s := &Scheduler{}

	now := time.Now()

	// Test that RetryCount persists across "restarts" (we just read it from the job history)
	b := database.Backup{
		Retry:         3,
		RetryInterval: 1,
		History: database.JobHistory{
			LastRunEndtime: now.Unix() - 120,
			LastRunState:   "exit status 1",
			LastRunStatus:  database.JobStatusFailed,
			RetryCount:     2, // Already had 2 retries
		},
	}

	// Should allow retry since 2 < 3
	if !s.shouldRetryBackup(b, now) {
		t.Error("should retry when RetryCount (2) < Retry limit (3)")
	}

	// Should not allow retry when at limit
	b.History.RetryCount = 3
	if s.shouldRetryBackup(b, now) {
		t.Error("should not retry when RetryCount (3) >= Retry limit (3)")
	}
}

func TestShouldRetryRestore_TypedStatus(t *testing.T) {
	s := &Scheduler{}

	now := time.Now()

	tests := []struct {
		name        string
		status      database.JobStatus
		retryCount  int
		shouldRetry bool
	}{
		{"Failed with retries remaining", database.JobStatusFailed, 1, true},
		{"Failed at retry limit", database.JobStatusFailed, 3, false},
		{"Success should not retry", database.JobStatusSuccess, 0, false},
		{"Warnings should not retry", database.JobStatusWarnings, 0, false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r := database.Restore{
				Retry:         3,
				RetryInterval: 1,
				History: database.JobHistory{
					LastRunEndtime: now.Unix() - 120,
					LastRunState:   "exit status 1",
					LastRunStatus:  tt.status,
					RetryCount:     tt.retryCount,
				},
			}

			result := s.shouldRetryRestore(r, now)
			if result != tt.shouldRetry {
				t.Errorf("shouldRetryRestore() = %v, want %v (status=%v, retryCount=%d)",
					result, tt.shouldRetry, tt.status, tt.retryCount)
			}
		})
	}
}

func TestIsFailedState_EdgeCases(t *testing.T) {
	// Ensure that states that look like warnings but don't match exactly
	// are treated as failures.
	tests := []struct {
		state    string
		expected bool
		desc     string
	}{
		{"", false, "empty state is not a failure"},
		{"OK", false, "OK is not a failure"},
		{"WARNINGS: 3", false, "warnings with count is not a failure"},
		{"WARNINGS: 0", false, "warnings with zero count is not a failure"},
		{"operation canceled", false, "manual stop/cancel is not a failure"},
		{"WARNING", true, "WARNING without S is a failure"},
		{"WARNINGS", true, "WARNINGS without count is a failure"},
		{"WARNINGS:3", true, "WARNINGS without space is a failure"},
		{"WARNINGS: ", false, "WARNINGS with empty count is not a failure (still matches pattern)"},
		{"TASK ERROR: something", true, "task error is a failure"},
		{"exit status 1", true, "exit status is a failure"},
		{"connection refused", true, "connection error is a failure"},
	}

	for _, tt := range tests {
		t.Run(tt.desc, func(t *testing.T) {
			result := isFailedState(tt.state)
			if result != tt.expected {
				t.Errorf("isFailedState(%q) = %v, want %v (%s)", tt.state, result, tt.expected, tt.desc)
			}
		})
	}
}