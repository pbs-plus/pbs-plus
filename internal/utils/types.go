package utils

import "time"

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

type FSStat struct {
	TotalSize      int64         `json:"total_size"`
	FreeSize       int64         `json:"free_size"`
	AvailableSize  int64         `json:"available_size"`
	TotalFiles     int           `json:"total_files"`
	FreeFiles      int           `json:"free_files"`
	AvailableFiles int           `json:"available_files"`
	CacheHint      time.Duration `json:"cache_hint"`
}
