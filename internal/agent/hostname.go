package agent

import (
	"hash/fnv"
	"os"
	"strconv"
	"time"

	"github.com/pbs-plus/pbs-plus/internal/agent/agentfs/types"
	"golang.org/x/exp/rand"
)

// GetAgentHostname returns the agent hostname from the PBS_PLUS_HOSTNAME
// environment variable, falling back to os.Hostname().
func GetAgentHostname() (string, error) {
	host := os.Getenv("PBS_PLUS_HOSTNAME")
	if host == "" {
		return os.Hostname()
	}

	if err := types.ValidateHostname(host); err != nil {
		return "", err
	}

	return host, nil
}

// HashHostname returns an FNV-1a 32-bit hash of the given host string.
func hashHostname(host string) uint32 {
	h := fnv.New32a()
	_, _ = h.Write([]byte(host))
	return h.Sum32()
}

// randomHostname generates a pseudo-random hostname for fallback purposes.
func randomHostname() string {
	return time.Now().Format("20060102150405") + strconv.Itoa(rand.Intn(1000))
}

// ComputeDelay returns the agent's polling interval. It reads
// PBS_PLUS_UPDATE_INTERVAL_MINUTES for a fixed value, or derives a
// deterministic 1-2 hour jitter based on the hostname.
func ComputeDelay() time.Duration {
	fixedDelayEnv := os.Getenv("PBS_PLUS_UPDATE_INTERVAL_MINUTES")
	fixedDelay := -1

	if fixedDelayEnv != "" {
		var err error
		fixedDelay, err = strconv.Atoi(fixedDelayEnv)
		if err != nil {
			fixedDelay = -1
		}
	}

	if fixedDelay != -1 {
		return time.Duration(fixedDelay) * time.Minute
	}

	const baseSeconds = 3600       // 1 hour
	const extraRangeSeconds = 3600 // up to an extra 1 hour

	hostname, err := GetAgentHostname()
	if err != nil {
		hostname = randomHostname()
	}

	hashVal := hashHostname(hostname)
	extraSeconds := int(hashVal % uint32(extraRangeSeconds))
	return time.Duration(baseSeconds+extraSeconds) * time.Second
}
