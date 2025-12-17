//go:build linux

package scripts

import (
	"github.com/pbs-plus/pbs-plus/internal/store/types"
)

type ScriptsResponse struct {
	Data   []types.Script `json:"data"`
	Digest string         `json:"digest"`
}

type ScriptConfigResponse struct {
	Errors  map[string]string `json:"errors"`
	Message string            `json:"message"`
	Data    types.Script      `json:"data"`
	Status  int               `json:"status"`
	Success bool              `json:"success"`
}
