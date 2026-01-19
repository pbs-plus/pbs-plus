package database

import (
	s3url "github.com/pbs-plus/pbs-plus/internal/backend/vfs/s3/url"
)

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
	JobId   string `json:"job_id"`
}

type Restore struct {
	ID            string     `json:"id"`
	Store         string     `json:"store"`
	Snapshot      string     `json:"snapshot"`
	Namespace     string     `json:"ns"`
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
	LastRunUpid           string `json:"last-run-upid"`
	LastRunState          string `json:"last-run-state"`
	LastRunEndtime        int64  `json:"last-run-endtime"`
	LastSuccessfulEndtime int64  `json:"last-successful-endtime"`
	LastSuccessfulUpid    string `json:"last-successful-upid"`
	LatestSnapshotSize    int    `json:"latest_snapshot_size,omitempty"`
	Duration              int64  `json:"duration"`
}

type Target struct {
	Name             string       `json:"name"`
	Type             TargetType   `json:"target_type"`
	Path             string       `json:"path"`
	AgentHost        AgentHost    `json:"agent_host,omitempty"`
	VolumeID         string       `json:"volume_id,omitempty"`
	MountScript      string       `json:"mount_script"`
	AgentVersion     string       `json:"agent_version"`
	ConnectionStatus bool         `json:"connection_status"`
	JobCount         int          `json:"job_count"`
	VolumeType       string       `json:"volume_type"`
	VolumeName       string       `json:"volume_name"`
	VolumeFS         string       `json:"volume_fs"`
	VolumeTotalBytes int          `json:"volume_total_bytes,omitempty"`
	VolumeUsedBytes  int          `json:"volume_used_bytes,omitempty"`
	VolumeFreeBytes  int          `json:"volume_free_bytes,omitempty"`
	VolumeTotal      string       `json:"volume_total"`
	VolumeUsed       string       `json:"volume_used"`
	VolumeFree       string       `json:"volume_free"`
	S3Info           *s3url.S3Url `json:"s3_info"`
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
