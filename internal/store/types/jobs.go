package types

type Job struct {
	ID                    string      `json:"id"`
	Store                 string      `config:"type=string,required" json:"store"`
	SourceMode            string      `config:"key=source_mode,type=string" json:"sourcemode"`
	ReadMode              string      `json:"readmode"`
	Mode                  string      `config:"type=string" json:"mode"`
	Target                string      `config:"type=string,required" json:"target"`
	TargetPath            string      `config:"type=string,required" json:"target-path"`
	Subpath               string      `config:"type=string" json:"subpath"`
	Schedule              string      `config:"type=string" json:"schedule"`
	Comment               string      `config:"type=string" json:"comment"`
	NotificationMode      string      `config:"key=notification_mode,type=string" json:"notification-mode"`
	PreScript             string      `json:"pre_script"`
	PostScript            string      `json:"post_script"`
	TargetMountScript     string      `json:"mount_script"`
	Namespace             string      `config:"type=string" json:"ns"`
	NextRun               int64       `json:"next-run"`
	Retry                 int         `config:"type=int" json:"retry"`
	RetryInterval         int         `config:"type=int" json:"retry-interval"`
	MaxDirEntries         int         `json:"max-dir-entries"`
	CurrentFileCount      int         `json:"current_file_count,omitempty"`
	CurrentFolderCount    int         `json:"current_folder_count,omitempty"`
	CurrentFilesSpeed     int         `json:"current_files_speed,omitempty"`
	CurrentBytesSpeed     int         `json:"current_bytes_speed,omitempty"`
	CurrentBytesTotal     int         `json:"current_bytes_total,omitempty"`
	StatCacheHits         int         `json:"stat_cache_hits,omitempty"`
	CurrentPID            int         `config:"key=current_pid,type=int" json:"current_pid"`
	LastRunUpid           string      `config:"key=last_run_upid,type=string" json:"last-run-upid"`
	LastRunState          string      `json:"last-run-state"`
	LastRunEndtime        int64       `json:"last-run-endtime"`
	LastSuccessfulEndtime int64       `json:"last-successful-endtime"`
	LastSuccessfulUpid    string      `config:"key=last_successful_upid,type=string" json:"last-successful-upid"`
	LatestSnapshotSize    int         `json:"latest_snapshot_size,omitempty"`
	Duration              int64       `json:"duration"`
	Exclusions            []Exclusion `json:"exclusions"`
	RawExclusions         string      `json:"rawexclusions"`
	ExpectedSize          int         `json:"expected_size,omitempty"`
	UPIDs                 []string    `json:"upids"`
}
