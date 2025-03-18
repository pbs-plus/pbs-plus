//go:build linux

package jobs

import (
	"github.com/sonroyaalmerol/pbs-plus/internal/store/types"
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
