package types

type Restore struct {
	ID                    string     `json:"id"`
	Store                 string     `json:"store"`
	Snapshot              string     `json:"snapshot"`
	Namespace             string     `json:"ns"`
	SrcPath               string     `json:"src-path"`
	DestTarget            TargetName `json:"dest-target"`
	DestTargetPath        TargetPath `json:"dest-target-path"`
	DestPath              string     `json:"dest-path"`
	Comment               string     `json:"comment"`
	Retry                 int        `json:"retry"`
	RetryInterval         int        `json:"retry-interval"`
	CurrentFileCount      int        `json:"current_file_count,omitempty"`
	CurrentFolderCount    int        `json:"current_folder_count,omitempty"`
	CurrentFilesSpeed     int        `json:"current_files_speed,omitempty"`
	CurrentBytesSpeed     int        `json:"current_bytes_speed,omitempty"`
	CurrentBytesTotal     int        `json:"current_bytes_total,omitempty"`
	StatCacheHits         int        `json:"stat_cache_hits,omitempty"`
	CurrentPID            int        `json:"current_pid"`
	LastRunUpid           string     `json:"last-run-upid"`
	LastRunState          string     `json:"last-run-state"`
	LastRunEndtime        int64      `json:"last-run-endtime"`
	LastSuccessfulEndtime int64      `json:"last-successful-endtime"`
	LastSuccessfulUpid    string     `json:"last-successful-upid"`
	Duration              int64      `json:"duration"`
	ExpectedSize          int        `json:"expected_size,omitempty"`
	UPIDs                 []string   `json:"upids"`
}
