package database

import (
	"database/sql/driver"
	"fmt"
	"strconv"
	"strings"
)

// JobStatus is a typed enum for job completion status.
// It provides type-safe status checking without string parsing.
type JobStatus int

const (
	JobStatusUnknown  JobStatus = iota // 0 - not run or uninitialized
	JobStatusSuccess                   // 1 - completed successfully
	JobStatusWarnings                  // 2 - completed with warnings (success)
	JobStatusFailed                    // 3 - failed (retryable)
	JobStatusCanceled                  // 4 - manually canceled (non-retryable)
)

// String returns the human-readable name of the status.
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

// ShouldRetry returns true if the job should be retried based on this status.
func (js JobStatus) ShouldRetry() bool {
	return js == JobStatusFailed
}

// IsCompleted returns true if the job has finished (success, warnings, failed, or canceled).
func (js JobStatus) IsCompleted() bool {
	return js == JobStatusSuccess || js == JobStatusWarnings ||
		js == JobStatusFailed || js == JobStatusCanceled
}

// IsSuccess returns true if the job completed successfully (with or without warnings).
func (js JobStatus) IsSuccess() bool {
	return js == JobStatusSuccess || js == JobStatusWarnings
}

// Value implements the driver.Valuer interface for database storage.
func (js JobStatus) Value() (driver.Value, error) {
	return int64(js), nil
}

// Scan implements the sql.Scanner interface for database retrieval.
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
		// Anything else is a failure
		return JobStatusFailed
	}
}

type Backup struct {
	ID               string      `json:"id"`
	Store            string      `json:"store"`
	SourceMode       string      `json:"sourcemode"`
	ReadMode         string      `json:"readmode"`
	Mode             string      `json:"mode"`
	Target           Target      `json:"target"`
	IncludeXattr     bool        `json:"include-xattr"`
	LegacyXattr      bool        `json:"legacy-xattr"`
	Subpath          string      `json:"subpath"`
	Schedule         string      `json:"schedule"`
	Comment          string      `json:"comment"`
	NotificationMode string      `json:"notification-mode"`
	PreScript        string      `json:"pre_script"`
	PostScript       string      `json:"post_script"`
	Namespace        string      `json:"ns"`
	NextRun          int64       `json:"next-run"`
	Retry            int         `json:"retry"`
	RetryInterval    int         `json:"retry-interval"`
	MaxDirEntries    int         `json:"max-dir-entries"`
	CurrentPID       int         `json:"current_pid"`
	Exclusions       []Exclusion `json:"exclusions"`
	RawExclusions    string      `json:"rawexclusions"`
	UPIDs            []Tasks     `json:"upids"`
	CurrentStats     JobStats    `json:"current-stats"`
	History          JobHistory  `json:"history"`
}

type Tasks struct {
	UPID    string `json:"upid"`
	Endtime int64  `json:"endtime"`
	Status  string `json:"status"`
}

type Exclusion struct {
	Path    string `json:"path"`
	Comment string `json:"comment"`
	JobID   string `json:"job_id"`
}

type Restore struct {
	ID            string     `json:"id"`
	Store         string     `json:"store"`
	Snapshot      string     `json:"snapshot"`
	Namespace     string     `json:"ns"`
	Mode          int        `json:"mode"`
	SrcPath       string     `json:"src-path"`
	DestTarget    Target     `json:"dest-target"`
	DestSubpath   string     `json:"dest-subpath"`
	PreScript     string     `json:"pre_script"`
	PostScript    string     `json:"post_script"`
	Comment       string     `json:"comment"`
	Retry         int        `json:"retry"`
	RetryInterval int        `json:"retry-interval"`
	CurrentPID    int        `json:"current_pid"`
	ExpectedSize  int        `json:"expected_size,omitempty"`
	UPIDs         []string   `json:"upids"`
	CurrentStats  JobStats   `json:"current-stats"`
	History       JobHistory `json:"history"`
}

type Script struct {
	Path        string `json:"path"`
	Description string `json:"description"`
	JobCount    int    `json:"job_count"`
	TargetCount int    `json:"target_count"`
	Script      string `json:"script"`
}

type JobStats struct {
	CurrentFileCount   int `json:"current_file_count,omitempty"`
	CurrentFolderCount int `json:"current_folder_count,omitempty"`
	CurrentFilesSpeed  int `json:"current_files_speed,omitempty"`
	CurrentBytesSpeed  int `json:"current_bytes_speed,omitempty"`
	CurrentBytesTotal  int `json:"current_bytes_total,omitempty"`
	StatCacheHits      int `json:"stat_cache_hits,omitempty"`
}

type JobHistory struct {
	LastRunUpid           string    `json:"last-run-upid"`
	LastRunStarttime      int64     `json:"last-run-starttime"`
	LastRunState          string    `json:"last-run-state"`  // human-readable message (legacy, for display)
	LastRunStatus         JobStatus `json:"last-run-status"` // typed status for logic ★ NEW
	LastRunEndtime        int64     `json:"last-run-endtime"`
	LastSuccessfulEndtime int64     `json:"last-successful-endtime"`
	LastSuccessfulUpid    string    `json:"last-successful-upid"`
	RetryCount            int       `json:"retry-count"` // persistent retry counter ★ NEW
	LatestSnapshotSize    int       `json:"latest_snapshot_size,omitempty"`
	Duration              int64     `json:"duration"`
}

type Target struct {
	Name             string     `json:"name"`
	Type             TargetType `json:"target_type"`
	Path             string     `json:"path"`
	AgentHost        AgentHost  `json:"agent_host"`
	VolumeID         string     `json:"volume_id,omitempty"`
	MountScript      string     `json:"mount_script"`
	AgentVersion     string     `json:"agent_version"`
	ConnectionStatus bool       `json:"connection_status"`
	JobCount         int        `json:"job_count"`
	VolumeType       string     `json:"volume_type"`
	VolumeName       string     `json:"volume_name"`
	VolumeFS         string     `json:"volume_fs"`
	VolumeTotalBytes int        `json:"volume_total_bytes,omitempty"`
	VolumeUsedBytes  int        `json:"volume_used_bytes,omitempty"`
	VolumeFreeBytes  int        `json:"volume_free_bytes,omitempty"`
	VolumeTotal      string     `json:"volume_total"`
	VolumeUsed       string     `json:"volume_used"`
	VolumeFree       string     `json:"volume_free"`
	S3Info           *S3Url     `json:"s3_info"`
}

type AgentHost struct {
	Name            string `json:"name"`
	IP              string `json:"ip"`
	Auth            string `json:"-"`
	TokenUsed       string `json:"-"`
	OperatingSystem string `json:"os"`
}

type TargetType string

const (
	TargetTypeLocal TargetType = "local"
	TargetTypeAgent TargetType = "agent"
	TargetTypeS3    TargetType = "s3"
)

type AgentToken struct {
	Token      string `json:"token"`
	Duration   string `json:"duration"`
	Comment    string `json:"comment"`
	CreatedAt  int    `json:"created_at"`
	Revoked    bool   `json:"revoked"`
	WinInstall string `json:"win_install"`
}

// VerificationJob represents a data verification job configuration.
type VerificationJob struct {
	ID                  string          `json:"id"`
	BackupJobID         string          `json:"backup_job_id"`
	Store               string          `json:"store"`
	Namespace           string          `json:"ns"`
	Mode                string          `json:"mode"`
	Schedule            string          `json:"schedule"`
	Comment             string          `json:"comment"`
	SpotConfig          SpotCheckConfig `json:"spot_config"`
	NextRun             int64           `json:"next-run"`
	Retry               int             `json:"retry"`
	RetryInterval       int             `json:"retry-interval"`
	History             JobHistory      `json:"history"`
	RunOnBackupComplete bool            `json:"run_on_backup_complete"`
	PendingSince        int64           `json:"pending_since"`
	CreatedAt           int64           `json:"created_at"`
}

// SpotCheckConfig holds configuration for random spot check mode.
type SpotCheckConfig struct {
	SampleCount      int               `json:"sample_count"`
	SamplingStrategy string            `json:"sampling_strategy"` // random, systematic, stratified
	UseLatest        bool              `json:"use_latest"`
	DateFrom         string            `json:"date_from"` // RFC3339 or empty
	DateTo           string            `json:"date_to"`   // RFC3339 or empty
	Filters          []SpotCheckFilter `json:"filters"`
	MaxFileSize      int64             `json:"max_file_size"`  // skip files larger than this (0 = no limit)
	FailThreshold    int               `json:"fail_threshold"` // stop after N failures (0 = no limit)
}

// SpotCheckFilter defines a filter for selecting files in spot checks.
type SpotCheckFilter struct {
	PathPattern string `json:"path_pattern"` // path prefix or glob
	MinSize     int64  `json:"min_size"`     // minimum file size in bytes (0 = no min)
	MaxSize     int64  `json:"max_size"`     // maximum file size in bytes (0 = no max)
}

// VerificationResult stores the outcome of a single verification run.
type VerificationResult struct {
	ID                int                      `json:"id"`
	VerificationJobID string                   `json:"verification_job_id"`
	UPID              string                   `json:"upid"`
	Snapshot          string                   `json:"snapshot"`
	SnapshotTime      int64                    `json:"snapshot_time"`
	TotalPopulation   int                      `json:"total_population"` // total eligible files in archive
	TotalFiles        int                      `json:"total_files"`      // files actually sampled
	VerifiedFiles     int                      `json:"verified_files"`
	FailedFiles       int                      `json:"failed_files"`
	SkippedFiles      int                      `json:"skipped_files"`
	Status            string                   `json:"status"` // pending, running, completed, failed
	StartedAt         int64                    `json:"started_at"`
	CompletedAt       int64                    `json:"completed_at"`
	Details           []VerificationFileResult `json:"details"`
}

// VerificationFileResult stores per-file verification outcome.
type VerificationFileResult struct {
	Path    string `json:"path"`
	Size    int64  `json:"size"`
	Status  string `json:"status"`  // ok, failed, skipped, error
	Message string `json:"message"` // human-readable detail
}
