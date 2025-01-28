package websockets

import (
	"math"
	"math/rand/v2"
	"time"
)

const (
	baseRetryInterval = 500 * time.Millisecond
	maxRetryInterval  = 30 * time.Second
)

func calculateBackoff(attempt int) time.Duration {
	if attempt <= 0 {
		return baseRetryInterval
	}

	// Calculate exponential backoff
	backoff := float64(baseRetryInterval) * math.Pow(2, float64(attempt))

	// Add jitter (±20%)
	jitter := (rand.Float64()*0.4 - 0.2) * backoff

	// Apply jitter and ensure we don't exceed maxRetryInterval
	duration := time.Duration(backoff + jitter)
	if duration > maxRetryInterval {
		return maxRetryInterval
	}
	return duration
}
