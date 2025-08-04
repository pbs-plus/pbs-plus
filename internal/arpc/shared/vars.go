package shared

import (
	"runtime"

	"github.com/pbs-plus/pbs-plus/internal/syslog"
	"github.com/pbs-plus/pbs-plus/internal/utils"
)

var MaxReceiveBuffer = 209715200
var MaxStreamBuffer = 4194304

func init() {
	// Get system memory
	sysMem, err := getSysMem()
	if err != nil {
		syslog.L.Error(err).WithMessage("failed to get system memory for arpc optimization").Write()
		return
	}

	// Calculate available memory for our application
	// Use conservative approach: 60% of available system memory
	availableForApp := sysMem.Available * 60 / 100

	// Reserve memory for other parts of the application (30% of available)
	reservedMemory := availableForApp * 30 / 100
	smuxMemoryBudget := availableForApp - reservedMemory

	// Calculate per-stream buffer size
	minStreamBuffer := 1 * 1024 * 1024 // 1MB
	maxStreamBuffer := 8 * 1024 * 1024 // 8MB

	perStreamBuffer := int(smuxMemoryBudget / uint64(utils.MAX_CONCURRENT_CLIENTS))
	if perStreamBuffer < minStreamBuffer {
		perStreamBuffer = minStreamBuffer
	}
	if perStreamBuffer > maxStreamBuffer {
		perStreamBuffer = maxStreamBuffer
	}

	// Calculate session buffer (should accommodate all streams)
	sessionBuffer := perStreamBuffer * utils.MAX_CONCURRENT_CLIENTS

	// Add 20% headroom for session buffer
	sessionBuffer = sessionBuffer * 120 / 100

	// Ensure we don't exceed reasonable limits
	maxSessionBuffer := int(smuxMemoryBudget * 80 / 100)
	if sessionBuffer > maxSessionBuffer {
		sessionBuffer = maxSessionBuffer
		// Recalculate per-stream buffer
		perStreamBuffer = sessionBuffer / utils.MAX_CONCURRENT_CLIENTS / 2
	}

	MaxReceiveBuffer = maxSessionBuffer
	MaxStreamBuffer = perStreamBuffer
}

type sysMem struct {
	Total     uint64 // Total system memory in bytes
	Available uint64 // Available memory in bytes
	Free      uint64 // Free memory in bytes
}

func getSysMem() (*sysMem, error) {
	var memStats runtime.MemStats
	runtime.ReadMemStats(&memStats)

	estimated := memStats.Sys * 4 // Conservative estimate

	return &sysMem{
		Total:     estimated,
		Available: estimated / 2, // Very conservative
		Free:      estimated / 4,
	}, nil
}
