package coredb

import (
	"database/sql/driver"
	"fmt"
	"strconv"
	"strings"
)

// It provides type-safe status checking without string parsing.
type JobStatus int

const (
	JobStatusUnknown JobStatus = iota
	JobStatusSuccess
	JobStatusWarnings
	JobStatusFailed
	JobStatusCanceled // 4 - manually canceled (non-retryable)
)

// JobStatusFromString parses a legacy string status into a typed JobStatus.
// This is used for backward compatibility when reading old records.
func JobStatusFromString(state string) JobStatus {
	switch state {
	case "OK":
		return JobStatusSuccess
	case "operation canceled":
		return JobStatusCanceled
	case "":
		return JobStatusUnknown
	default:
		// Check for warnings pattern - must start with "WARNINGS: "
		if strings.HasPrefix(state, "WARNINGS: ") {
			return JobStatusWarnings
		}
		return JobStatusFailed
	}
}

func (js JobStatus) String() string {
	switch js {
	case JobStatusSuccess:
		return "OK"
	case JobStatusWarnings:
		return "WARNINGS"
	case JobStatusFailed:
		return "FAILED"
	case JobStatusCanceled:
		return "CANCELED"
	default:
		return "UNKNOWN"
	}
}

func (js JobStatus) ShouldRetry() bool {
	return js == JobStatusFailed
}

// IsCompleted returns true if the job has finished (success, warnings, failed, or canceled).

// IsCompleted returns true if the job has finished (success, warnings, failed, or canceled).
func (js JobStatus) IsCompleted() bool {
	return js == JobStatusSuccess || js == JobStatusWarnings ||
		js == JobStatusFailed || js == JobStatusCanceled
}

func (js JobStatus) IsSuccess() bool {
	return js == JobStatusSuccess || js == JobStatusWarnings
}

func (js JobStatus) Value() (driver.Value, error) {
	return int64(js), nil
}

func (js *JobStatus) Scan(value any) error {
	switch v := value.(type) {
	case int64:
		*js = JobStatus(v)
	case int:
		*js = JobStatus(v)
	case []byte:
		i, err := strconv.Atoi(string(v))
		if err != nil {
			return fmt.Errorf("cannot scan %v into JobStatus: %w", value, err)
		}
		*js = JobStatus(i)
	case string:
		i, err := strconv.Atoi(v)
		if err != nil {
			return fmt.Errorf("cannot scan %v into JobStatus: %w", value, err)
		}
		*js = JobStatus(i)
	case nil:
		*js = JobStatusUnknown
	default:
		return fmt.Errorf("cannot scan %T into JobStatus", value)
	}
	return nil
}

// JobStatusFromString parses a legacy string status into a typed JobStatus.
// This is used for backward compatibility when reading old records.

type FilesystemAccess string

const (
	TargetTypeFilesystem TargetType = "filesystem"
	TargetTypeS3         TargetType = "s3"
	TargetTypePostgreSQL TargetType = "postgresql"
	TargetTypeMySQL      TargetType = "mysql"

	FilesystemAccessLocal FilesystemAccess = "local"
	FilesystemAccessAgent FilesystemAccess = "agent"
)
