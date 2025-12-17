//go:build linux

package targets

import (
	"github.com/pbs-plus/pbs-plus/internal/store/types"
)

type TargetsResponse struct {
	Data   []types.Target `json:"data"`
	Digest string         `json:"digest"`
}

type TargetConfigResponse struct {
	Errors  map[string]string `json:"errors"`
	Message string            `json:"message"`
	Data    types.Target      `json:"data"`
	Status  int               `json:"status"`
	Success bool              `json:"success"`
}
