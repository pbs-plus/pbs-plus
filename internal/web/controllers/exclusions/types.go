//go:build linux

package exclusions

import (
	"github.com/pbs-plus/pbs-plus/internal/store/types"
)

type ExclusionsResponse struct {
	Data   []types.Exclusion `json:"data"`
	Digest string            `json:"digest"`
}

type ExclusionConfigResponse struct {
	Errors  map[string]string `json:"errors"`
	Message string            `json:"message"`
	Data    *types.Exclusion  `json:"data"`
	Status  int               `json:"status"`
	Success bool              `json:"success"`
}
