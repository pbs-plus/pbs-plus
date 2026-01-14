//go:build linux

package jobs

import (
	"github.com/pbs-plus/pbs-plus/internal/store/types"
)

type BackupsResponse struct {
	Data   []types.Backup `json:"data"`
	Digest string         `json:"digest"`
}

type BackupConfigResponse struct {
	Errors  map[string]string `json:"errors"`
	Message string            `json:"message"`
	Data    types.Backup      `json:"data"`
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
	Data   []types.Restore `json:"data"`
	Digest string          `json:"digest"`
}

type RestoreConfigResponse struct {
	Errors  map[string]string `json:"errors"`
	Message string            `json:"message"`
	Data    types.Restore     `json:"data"`
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
