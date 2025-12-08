package types

type Target struct {
	Name             string `json:"name"`
	Path             string `json:"path"`
	IsAgent          bool   `json:"is_agent"`
	IsS3             bool   `json:"is_s3"`
	MountScript      string `json:"mount-script"`
	AgentVersion     string `json:"agent_version"`
	ConnectionStatus bool   `json:"connection_status"`
	Auth             string `json:"auth"`
	JobCount         int    `json:"job_count"`
	TokenUsed        string `json:"token_used"`
	DriveType        string `json:"drive_type"`
	DriveName        string `json:"drive_name"`
	DriveFS          string `json:"drive_fs"`
	DriveTotalBytes  int    `json:"drive_total_bytes,omitempty"`
	DriveUsedBytes   int    `json:"drive_used_bytes,omitempty"`
	DriveFreeBytes   int    `json:"drive_free_bytes,omitempty"`
	DriveTotal       string `json:"drive_total"`
	DriveUsed        string `json:"drive_used"`
	DriveFree        string `json:"drive_free"`
	OperatingSystem  string `json:"os"`
}
