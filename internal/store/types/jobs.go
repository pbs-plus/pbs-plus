package types

type Job struct {
	ID                    string      `json:"id"`
	Store                 string      `config:"type=string,required" json:"store"`
	Target                string      `config:"type=string,required" json:"target"`
	Subpath               string      `config:"type=string" json:"subpath"`
	Schedule              string      `config:"type=string" json:"schedule"`
	Comment               string      `config:"type=string" json:"comment"`
	NotificationMode      string      `config:"key=notification_mode,type=string" json:"notification-mode"`
	Namespace             string      `config:"type=string" json:"ns"`
	NextRun               *int64      `json:"next-run"`
	Retry                 int         `config:"type=int" json:"retry"`
	CurrentFileCount      string      `json:"current_file_count"`
	CurrentFolderCount    string      `json:"current_folder_count"`
	CurrentFilesSpeed     string      `json:"current_files_speed"`
	CurrentBytesSpeed     string      `json:"current_bytes_speed"`
	CurrentBytesTotal     string      `json:"current_bytes_total"`
	CurrentPID            int         `config:"key=current_pid,type=int" json:"current_pid"`
	LastRunUpid           string      `config:"key=last_run_upid,type=string" json:"last-run-upid"`
	LastRunState          *string     `json:"last-run-state"`
	LastRunEndtime        *int64      `json:"last-run-endtime"`
	LastSuccessfulEndtime *int64      `json:"last-successful-endtime"`
	LastSuccessfulUpid    string      `config:"key=last_successful_upid,type=string" json:"last-successful-upid"`
	LastRunPlusError      string      `config:"key=last_plus_error,type=string" json:"last-plus-error"`
	LastRunPlusTime       int         `config:"key=last_plus_time,type=int" json:"last-plus-time"`
	Duration              *int64      `json:"duration"`
	Exclusions            []Exclusion `json:"exclusions"`
	RawExclusions         string      `json:"rawexclusions"`
	ExpectedSize          string      `json:"expected_size"`
	UPIDs                 []string    `json:"upids"`
}
