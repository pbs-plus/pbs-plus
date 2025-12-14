package utils

type DriveInfo struct {
	Letter     string `json:"letter"`
	Type       string `json:"type"`
	VolumeName string `json:"volume_name"`
	FileSystem string `json:"filesystem"`
	TotalBytes uint64 `json:"total_bytes"`
	UsedBytes  uint64 `json:"used_bytes"`
	FreeBytes  uint64 `json:"free_bytes"`
	Total      string `json:"total"`
	Used       string `json:"used"`
	Free       string `json:"free"`
}
