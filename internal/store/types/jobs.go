package types

type Job struct {
	ID                    string      `json:"id"`
	Store                 string      `json:"store"`
	SourceMode            string      `json:"sourcemode"`
	ReadMode              string      `json:"readmode"`
	Mode                  string      `json:"mode"`
	Target                string      `json:"target"`
	TargetPath            string      `json:"target-path"`
	IncludeXattr          bool        `json:"include-xattr"`
	LegacyXattr           bool        `json:"legacy-xattr"`
	Subpath               string      `json:"subpath"`
	Schedule              string      `json:"schedule"`
	Comment               string      `json:"comment"`
	NotificationMode      string      `json:"notification-mode"`
	PreScript             string      `json:"pre_script"`
	PostScript            string      `json:"post_script"`
	TargetMountScript     string      `json:"mount_script"`
	Namespace             string      `json:"ns"`
	NextRun               int64       `json:"next-run"`
	Retry                 int         `json:"retry"`
	RetryInterval         int         `json:"retry-interval"`
	MaxDirEntries         int         `json:"max-dir-entries"`
	CurrentFileCount      int         `json:"current_file_count,omitempty"`
	CurrentFolderCount    int         `json:"current_folder_count,omitempty"`
	CurrentFilesSpeed     int         `json:"current_files_speed,omitempty"`
	CurrentBytesSpeed     int         `json:"current_bytes_speed,omitempty"`
	CurrentBytesTotal     int         `json:"current_bytes_total,omitempty"`
	StatCacheHits         int         `json:"stat_cache_hits,omitempty"`
	CurrentPID            int         `json:"current_pid"`
	LastRunUpid           string      `json:"last-run-upid"`
	LastRunState          string      `json:"last-run-state"`
	LastRunEndtime        int64       `json:"last-run-endtime"`
	LastSuccessfulEndtime int64       `json:"last-successful-endtime"`
	LastSuccessfulUpid    string      `json:"last-successful-upid"`
	LatestSnapshotSize    int         `json:"latest_snapshot_size,omitempty"`
	Duration              int64       `json:"duration"`
	Exclusions            []Exclusion `json:"exclusions"`
	RawExclusions         string      `json:"rawexclusions"`
	ExpectedSize          int         `json:"expected_size,omitempty"`
	UPIDs                 []string    `json:"upids"`
}
