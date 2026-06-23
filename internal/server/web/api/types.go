package api

import (
	"github.com/pbs-plus/pbs-plus/internal/server/database"
)

type FlatBackup struct {
	ID               string `json:"id"`
	Store            string `json:"store"`
	Mode             string `json:"mode"`
	SourceMode       string `json:"sourcemode"`
	ReadMode         string `json:"readmode"`
	Subpath          string `json:"subpath"`
	Namespace        string `json:"ns"`
	Schedule         string `json:"schedule"`
	Comment          string `json:"comment"`
	NotificationMode string `json:"notification-mode"`
	PreScript        string `json:"pre_script"`
	PostScript       string `json:"post_script"`
	NextRun          int64  `json:"next-run"`
	Retry            int    `json:"retry"`
	RetryInterval    int    `json:"retry-interval"`
	MaxDirEntries    int    `json:"max-dir-entries"`
	RawExclusions    string `json:"rawexclusions"`
	IncludeXattr     bool   `json:"include-xattr"`
	LegacyXattr      bool   `json:"legacy-xattr"`

	Target       string `json:"target"`
	ExpectedSize int    `json:"expected_size,omitempty"`

	LastRunUpid           string `json:"last-run-upid"`
	LastRunState          string `json:"last-run-state"`
	LastRunEndtime        int64  `json:"last-run-endtime"`
	LastSuccessfulEndtime int64  `json:"last-successful-endtime"`
	LastSuccessfulUpid    string `json:"last-successful-upid"`
	Duration              int64  `json:"duration"`

	CurrentFileCount   int `json:"current_file_count,omitempty"`
	CurrentFolderCount int `json:"current_folder_count,omitempty"`
	CurrentFilesSpeed  int `json:"current_files_speed,omitempty"`
	CurrentBytesSpeed  int `json:"current_bytes_speed,omitempty"`
	CurrentBytesTotal  int `json:"current_bytes_total,omitempty"`

	TargetSizeHuman      string           `json:"target_size_human"`
	ReadSpeedHuman       string           `json:"read_speed_human"`
	ReadTotalHuman       string           `json:"read_total_human"`
	ProcessingSpeedHuman string           `json:"processing_speed_human"`
	StatusParsed         ParsedTaskStatus `json:"status_parsed"`

	// Computed server-side from alert settings
	Stale bool `json:"stale"`
}

type FlatRestore struct {
	ID               string `json:"id"`
	Store            string `json:"store"`
	Namespace        string `json:"ns"`
	Snapshot         string `json:"snapshot"`
	SnapshotHuman    string `json:"snapshot_human"`
	SrcPath          string `json:"src-path"`
	DestSubpath      string `json:"dest-subpath"`
	PreScript        string `json:"pre_script"`
	PostScript       string `json:"post_script"`
	Comment          string `json:"comment"`
	NotificationMode string `json:"notification-mode"`
	Retry            int    `json:"retry"`
	RetryInterval    int    `json:"retry-interval"`
	ExpectedSize     int    `json:"expected_size,omitempty"`

	DestTarget string `json:"dest-target"`

	LastRunUpid           string `json:"last-run-upid"`
	LastRunState          string `json:"last-run-state"`
	LastRunEndtime        int64  `json:"last-run-endtime"`
	LastSuccessfulEndtime int64  `json:"last-successful-endtime"`
	LastSuccessfulUpid    string `json:"last-successful-upid"`
	Duration              int64  `json:"duration"`

	CurrentFileCount   int `json:"current_file_count,omitempty"`
	CurrentFolderCount int `json:"current_folder_count,omitempty"`
	CurrentFilesSpeed  int `json:"current_files_speed,omitempty"`
	CurrentBytesSpeed  int `json:"current_bytes_speed,omitempty"`
	CurrentBytesTotal  int `json:"current_bytes_total,omitempty"`

	TargetSizeHuman      string           `json:"target_size_human"`
	ReadSpeedHuman       string           `json:"read_speed_human"`
	ReadTotalHuman       string           `json:"read_total_human"`
	ProcessingSpeedHuman string           `json:"processing_speed_human"`
	StatusParsed         ParsedTaskStatus `json:"status_parsed"`
}

type FlatVerificationJob struct {
	ID                  string              `json:"id"`
	BackupJobID         string              `json:"backup_job_id"`
	Store               string              `json:"store"`
	Namespace           string              `json:"ns"`
	Mode                string              `json:"mode"`
	Schedule            string              `json:"schedule"`
	Comment             string              `json:"comment"`
	NotificationMode    string              `json:"notification-mode"`
	SpotConfig          SpotCheckConfigJSON `json:"spot_config"`
	NextRun             int64               `json:"next-run"`
	Retry               int                 `json:"retry"`
	RetryInterval       int                 `json:"retry-interval"`
	TargetMode          string              `json:"target_mode"`
	Recursive           bool                `json:"recursive"`
	RunOnBackupComplete bool                `json:"run_on_backup_complete"`
	CreatedAt           int64               `json:"created_at"`

	LastRunUpid           string `json:"last-run-upid"`
	LastRunState          string `json:"last-run-state"`
	LastRunStarttime      int64  `json:"last-run-starttime"`
	LastRunEndtime        int64  `json:"last-run-endtime"`
	LastSuccessfulEndtime int64  `json:"last-successful-endtime"`
	LastSuccessfulUpid    string `json:"last-successful-upid"`
	Duration              int64  `json:"duration"`

	StatusParsed ParsedTaskStatus `json:"status_parsed"`
}

type SpotCheckConfigJSON struct {
	SampleCount        int                   `json:"sample_count"`
	SampleCountPercent float64               `json:"sample_count_percent"`
	SamplingStrategy   string                `json:"sampling_strategy"`
	UseLatest          bool                  `json:"use_latest"`
	DateFrom           string                `json:"date_from"`
	DateTo             string                `json:"date_to"`
	Filters            []SpotCheckFilterJSON `json:"filters"`
	FailThreshold      int                   `json:"fail_threshold"`
}

type SpotCheckFilterJSON struct {
	PathPattern string `json:"path_pattern"`
	MinSize     int64  `json:"min_size"`
	MaxSize     int64  `json:"max_size"`
}

type VerificationAggregate struct {
	TotalJobs    int     `json:"total_jobs"`
	TotalRuns    int     `json:"total_runs"`
	TotalFiles   int     `json:"total_files"`
	TotalFailed  int     `json:"total_failed"`
	TotalSkipped int     `json:"total_skipped"`
	PassRate     float64 `json:"pass_rate"`
	CleanRuns    int     `json:"clean_runs"`
	FailedRuns   int     `json:"failed_runs"`
	Last30Days   int     `json:"last_30_days"`
	Confidence   float64 `json:"confidence"`
}

type FlatVerificationResult struct {
	ID                int                          `json:"id"`
	VerificationJobID string                       `json:"verification_job_id"`
	UPID              string                       `json:"upid"`
	Snapshot          string                       `json:"snapshot"`
	SnapshotHuman     string                       `json:"snapshot_human"`
	SnapshotTime      int64                        `json:"snapshot_time"`
	TotalPopulation   int                          `json:"total_population"`
	TotalFiles        int                          `json:"total_files"`
	VerifiedFiles     int                          `json:"verified_files"`
	FailedFiles       int                          `json:"failed_files"`
	SkippedFiles      int                          `json:"skipped_files"`
	Status            string                       `json:"status"`
	StartedAt         int64                        `json:"started_at"`
	CompletedAt       int64                        `json:"completed_at"`
	DurationHuman     string                       `json:"duration_human"`
	PassRate          float64                      `json:"pass_rate"`
	Confidence        ConfidenceInfo               `json:"confidence"`
	StatusBadge       string                       `json:"status_badge"`
	Details           []FlatVerificationFileResult `json:"details"`
}

type FlatVerificationFileResult struct {
	Path        string `json:"path"`
	Size        int64  `json:"size"`
	SizeHuman   string `json:"size_human"`
	Status      string `json:"status"`
	StatusHuman string `json:"status_human"`
	Message     string `json:"message"`
}

type TargetTreeNode struct {
	Text      string           `json:"text"`
	IconCls   string           `json:"iconCls,omitempty"`
	Expanded  bool             `json:"expanded"`
	IsGroup   bool             `json:"isGroup"`
	GroupType string           `json:"groupType,omitempty"`
	Leaf      bool             `json:"leaf"`
	Children  []TargetTreeNode `json:"children,omitempty"`

	Name             string `json:"name,omitempty"`
	Path             string `json:"path,omitempty"`
	TargetType       string `json:"target_type,omitempty"`
	MountScript      string `json:"mount_script,omitempty"`
	VolumeID         string `json:"volume_id,omitempty"`
	JobCount         int    `json:"job_count,omitempty"`
	AgentVersion     string `json:"agent_version,omitempty"`
	ConnectionStatus bool   `json:"connection_status,omitempty"`
	VolumeType       string `json:"volume_type,omitempty"`
	VolumeName       string `json:"volume_name,omitempty"`
	VolumeFS         string `json:"volume_fs,omitempty"`
	VolumeTotalBytes int    `json:"volume_total_bytes,omitempty"`
	VolumeUsedBytes  int    `json:"volume_used_bytes,omitempty"`
	VolumeFreeBytes  int    `json:"volume_free_bytes,omitempty"`
	VolumeTotalHuman string `json:"volume_total,omitempty"`
	VolumeUsedHuman  string `json:"volume_used,omitempty"`
	VolumeFreeHuman  string `json:"volume_free,omitempty"`
	AgentHostname    string `json:"agent_hostname,omitempty"`
	OS               string `json:"os,omitempty"`
	IP               string `json:"ip,omitempty"`
}

type TargetsTreeResponse struct {
	Data   []TargetTreeNode `json:"data"`
	Digest string           `json:"digest"`
}

type BackupConfigResponse struct {
	Errors  map[string]string `json:"errors"`
	Message string            `json:"message"`
	Data    any               `json:"data"`
	Status  int               `json:"status"`
	Success bool              `json:"success"`
}

type BackupUPIDsResponse struct {
	Errors  map[string]string `json:"errors"`
	Message string            `json:"message"`
	Data    []database.Tasks  `json:"data"`
	Status  int               `json:"status"`
	Success bool              `json:"success"`
}

type BackupRunResponse struct {
	Errors  map[string]string `json:"errors"`
	Message string            `json:"message"`
	Data    string            `json:"data"`
	Status  int               `json:"status"`
	Success bool              `json:"success"`
}

type RestoreConfigResponse struct {
	Errors  map[string]string `json:"errors"`
	Message string            `json:"message"`
	Data    any               `json:"data"`
	Status  int               `json:"status"`
	Success bool              `json:"success"`
}

type RestoreRunResponse struct {
	Errors  map[string]string `json:"errors"`
	Message string            `json:"message"`
	Data    string            `json:"data"`
	Status  int               `json:"status"`
	Success bool              `json:"success"`
}

type TargetsResponse struct {
	Data    []database.Target `json:"data"`
	Digest  string            `json:"digest"`
	Success bool              `json:"success"`
}

type TargetConfigResponse struct {
	Errors  map[string]string `json:"errors"`
	Message string            `json:"message"`
	Data    database.Target   `json:"data"`
	Status  int               `json:"status"`
	Success bool              `json:"success"`
}

type AgentConfigResponse struct {
	Errors  map[string]string  `json:"errors"`
	Message string             `json:"message"`
	Data    database.AgentHost `json:"data"`
	Status  int                `json:"status"`
	Success bool               `json:"success"`
}

type TokensResponse struct {
	Data   []database.AgentToken `json:"data"`
	Digest string                `json:"digest"`
}

type TokenConfigResponse struct {
	Errors  map[string]string   `json:"errors"`
	Message string              `json:"message"`
	Data    database.AgentToken `json:"data"`
	Status  int                 `json:"status"`
	Success bool                `json:"success"`
}

type ExclusionsResponse struct {
	Data   []database.Exclusion `json:"data"`
	Digest string               `json:"digest"`
}

type ExclusionConfigResponse struct {
	Errors  map[string]string   `json:"errors"`
	Message string              `json:"message"`
	Data    *database.Exclusion `json:"data"`
	Status  int                 `json:"status"`
	Success bool                `json:"success"`
}

type VersionResponse struct {
	Version string `json:"version"`
}

type ScriptConfig struct {
	AgentUrl       string
	ServerUrl      string
	BootstrapToken string
}

type ScriptsResponse struct {
	Data   []database.Script `json:"data"`
	Digest string            `json:"digest"`
}

type ScriptConfigResponse struct {
	Errors  map[string]string `json:"errors"`
	Message string            `json:"message"`
	Data    database.Script   `json:"data"`
	Status  int               `json:"status"`
	Success bool              `json:"success"`
}

type MtfJobConfigResponse struct {
	Errors  map[string]string `json:"errors"`
	Message string            `json:"message"`
	Data    any               `json:"data"`
	Status  int               `json:"status"`
	Success bool              `json:"success"`
}

type MtfJobRunResponse struct {
	Errors  map[string]string `json:"errors"`
	Message string            `json:"message"`
	Data    string            `json:"data"`
	Status  int               `json:"status"`
	Success bool              `json:"success"`
}

type MtfInventoryResponse struct {
	Data    any    `json:"data"`
	Digest  string `json:"digest"`
	Success bool   `json:"success"`
}

type MtfMappingConfigResponse struct {
	Errors  map[string]string `json:"errors"`
	Message string            `json:"message"`
	Data    any               `json:"data"`
	Status  int               `json:"status"`
	Success bool              `json:"success"`
}
