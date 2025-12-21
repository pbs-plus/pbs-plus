//go:build linux

package jobs

import (
	"github.com/pbs-plus/pbs-plus/internal/store/types"
)

type JobsResponse struct {
	Data   []types.Job `json:"data"`
	Digest string      `json:"digest"`
}

type JobConfigResponse struct {
	Errors  map[string]string `json:"errors"`
	Message string            `json:"message"`
	Data    types.Job         `json:"data"`
	Status  int               `json:"status"`
	Success bool              `json:"success"`
}

type JobRunResponse struct {
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
