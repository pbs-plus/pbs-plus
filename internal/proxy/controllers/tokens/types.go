//go:build linux

package tokens

import "github.com/sonroyaalmerol/pbs-plus/internal/store"

type TokensResponse struct {
	Data   []store.AgentToken `json:"data"`
	Digest string             `json:"digest"`
}

type TokenConfigResponse struct {
	Errors  map[string]string `json:"errors"`
	Message string            `json:"message"`
	Data    *store.AgentToken `json:"data"`
	Status  int               `json:"status"`
	Success bool              `json:"success"`
}
