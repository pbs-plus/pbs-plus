//go:build linux

package restore

import (
	"fmt"
	"time"

	"github.com/pbs-plus/pbs-plus/internal/agent/agentfs/types"
)

func formatBytes(b int64) string {
	return types.HumanizeBytes(uint64(b))
}

func formatDuration(d time.Duration) string {
	if d < time.Minute {
		return fmt.Sprintf("%.1fs", d.Seconds())
	}
	m := int(d.Minutes())
	s := int(d.Seconds()) - m*60
	if m < 60 {
		return fmt.Sprintf("%dm %ds", m, s)
	}
	h := m / 60
	m = m % 60
	return fmt.Sprintf("%dh %dm %ds", h, m, s)
}

func formatSpeed(bytesPerSec float64) string {
	return fmt.Sprintf("%s/s", types.HumanizeBytes(uint64(bytesPerSec)))
}
