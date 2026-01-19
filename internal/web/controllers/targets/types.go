//go:build linux

package targets

import "github.com/pbs-plus/pbs-plus/internal/store/database"

type TargetsResponse struct {
	Data   []database.Target `json:"data"`
	Digest string            `json:"digest"`
}

type TargetConfigResponse struct {
	Errors  map[string]string `json:"errors"`
	Message string            `json:"message"`
	Data    database.Target   `json:"data"`
	Status  int               `json:"status"`
	Success bool              `json:"success"`
}

type AgentConfigResponse struct {
	Errors  map[string]string  `json:"errors"`
	Message string             `json:"message"`
	Data    database.AgentHost `json:"data"`
	Status  int                `json:"status"`
	Success bool               `json:"success"`
}
