//go:build linux

package restore

import "github.com/pbs-plus/pbs-plus/internal/agent/agentfs/types"

func formatBytes(b int64) string {
	return types.HumanizeBytes(uint64(b))
}
