//go:build linux

package exclusions

import "github.com/sonroyaalmerol/pbs-plus/internal/store"

type ExclusionsResponse struct {
	Data   []store.Exclusion `json:"data"`
	Digest string            `json:"digest"`
}

type ExclusionConfigResponse struct {
	Errors  map[string]string `json:"errors"`
	Message string            `json:"message"`
	Data    *store.Exclusion  `json:"data"`
	Status  int               `json:"status"`
	Success bool              `json:"success"`
}
