package targets

import "github.com/sonroyaalmerol/pbs-d2d-backup/store"

type TargetsResponse struct {
	Data   []store.Target `json:"data"`
	Digest string         `json:"digest"`
}

type TargetConfigResponse struct {
	Errors  map[string]string `json:"errors"`
	Message string            `json:"message"`
	Data    *store.Target     `json:"data"`
	Status  int               `json:"status"`
	Success bool              `json:"success"`
}
