//go:build linux

package partial_files

import "github.com/sonroyaalmerol/pbs-plus/internal/store"

type PartialFilesResponse struct {
	Data   []store.PartialFile `json:"data"`
	Digest string              `json:"digest"`
}

type PartialFileConfigResponse struct {
	Errors  map[string]string  `json:"errors"`
	Message string             `json:"message"`
	Data    *store.PartialFile `json:"data"`
	Status  int                `json:"status"`
	Success bool               `json:"success"`
}
