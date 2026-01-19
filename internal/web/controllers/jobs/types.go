//go:build linux

package jobs

import "github.com/pbs-plus/pbs-plus/internal/store/database"

type BackupsResponse struct {
	Data   []database.Backup `json:"data"`
	Digest string            `json:"digest"`
}

type BackupConfigResponse struct {
	Errors  map[string]string `json:"errors"`
	Message string            `json:"message"`
	Data    database.Backup   `json:"data"`
	Status  int               `json:"status"`
	Success bool              `json:"success"`
}

type BackupUPIDsResponse struct {
	Errors  map[string]string `json:"errors"`
	Message string            `json:"message"`
	Data    []string          `json:"data"`
	Status  int               `json:"status"`
	Success bool              `json:"success"`
}

type BackupRunResponse struct {
	Errors  map[string]string `json:"errors"`
	Message string            `json:"message"`
	Data    string            `json:"data"`
	Status  int               `json:"status"`
	Success bool              `json:"success"`
}

type RestoresResponse struct {
	Data   []database.Restore `json:"data"`
	Digest string             `json:"digest"`
}

type RestoreConfigResponse struct {
	Errors  map[string]string `json:"errors"`
	Message string            `json:"message"`
	Data    database.Restore  `json:"data"`
	Status  int               `json:"status"`
	Success bool              `json:"success"`
}

type RestoreRunResponse struct {
	Errors  map[string]string `json:"errors"`
	Message string            `json:"message"`
	Data    string            `json:"data"`
	Status  int               `json:"status"`
	Success bool              `json:"success"`
}
