//go:build linux

package tokens

import "github.com/pbs-plus/pbs-plus/internal/store/database"

type TokensResponse struct {
	Data   []database.AgentToken `json:"data"`
	Digest string                `json:"digest"`
}

type TokenConfigResponse struct {
	Errors  map[string]string   `json:"errors"`
	Message string              `json:"message"`
	Data    database.AgentToken `json:"data"`
	Status  int                 `json:"status"`
	Success bool                `json:"success"`
}
