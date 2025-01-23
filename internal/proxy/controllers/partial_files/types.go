//go:build linux

package partial_files

import (
	"github.com/sonroyaalmerol/pbs-plus/internal/store/types"
)

type PartialFilesResponse struct {
	Data   []types.PartialFile `json:"data"`
	Digest string              `json:"digest"`
}

type PartialFileConfigResponse struct {
	Errors  map[string]string  `json:"errors"`
	Message string             `json:"message"`
	Data    *types.PartialFile `json:"data"`
	Status  int                `json:"status"`
	Success bool               `json:"success"`
}
