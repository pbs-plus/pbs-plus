package types

type Target struct {
	Name             string `json:"name"`
	Path             string `config:"type=string,required" json:"path"`
	IsAgent          bool   `json:"is_agent"`
	IsS3             bool   `json:"is_s3"`
	MountScript      string `json:"mount-script"`
	AgentVersion     string `json:"agent_version"`
	ConnectionStatus bool   `json:"connection_status"`
	Auth             string `config:"type=string" json:"auth"`
	JobCount         int    `json:"job_count"`
	TokenUsed        string `config:"key=token_used,type=string" json:"token_used"`
	DriveType        string `config:"key=drive_type,type=string" json:"drive_type"`
	DriveName        string `config:"key=drive_name,type=string" json:"drive_name"`
	DriveFS          string `config:"key=drive_fs,type=string" json:"drive_fs"`
	DriveTotalBytes  int    `config:"key=drive_total_bytes,type=int" json:"drive_total_bytes,omitempty"`
	DriveUsedBytes   int    `config:"key=drive_used_bytes,type=int" json:"drive_used_bytes,omitempty"`
	DriveFreeBytes   int    `config:"key=drive_free_bytes,type=int" json:"drive_free_bytes,omitempty"`
	DriveTotal       string `config:"key=drive_total,type=string" json:"drive_total"`
	DriveUsed        string `config:"key=drive_used,type=string" json:"drive_used"`
	DriveFree        string `config:"key=drive_free,type=string" json:"drive_free"`
	OperatingSystem  string `json:"os"`
}
