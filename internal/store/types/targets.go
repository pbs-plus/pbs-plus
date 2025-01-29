package types

type Target struct {
	Name             string `json:"name"`
	Path             string `config:"type=string,required" json:"path"`
	IsAgent          bool   `json:"is_agent"`
	AgentVersion     string `json:"agent_version"`
	ConnectionStatus bool   `json:"connection_status"`
	Auth             string `config:"type=string" json:"auth"`
	TokenUsed        string `config:"type=string" json:"token_used"`
	DriveType        string `config:"type=string" json:"drive_type"`
	DriveName        string `config:"type=string" json:"drive_name"`
	DriveFS          string `config:"type=string" json:"drive_fs"`
	DriveTotalBytes  int    `config:"type=int" json:"drive_total_bytes"`
	DriveUsedBytes   int    `config:"type=int" json:"drive_used_bytes"`
	DriveFreeBytes   int    `config:"type=int" json:"drive_free_bytes"`
	DriveTotal       string `config:"type=string" json:"drive_total"`
	DriveUsed        string `config:"type=string" json:"drive_used"`
	DriveFree        string `config:"type=string" json:"drive_free"`
}
