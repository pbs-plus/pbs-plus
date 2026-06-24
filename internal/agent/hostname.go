package agent

import (
	"hash/fnv"
	"os"
	"strconv"
	"time"

	"github.com/pbs-plus/pbs-plus/internal/crypto"
	"github.com/pbs-plus/pbs-plus/internal/host"
)

func hashHostname(host string) uint32 {
	h := fnv.New32a()
	h.Write([]byte(host))
	return h.Sum32()
}

func randomHostname() string {
	n, err := crypto.SecureRandomInt(1000)
	if err != nil {
		n = 0
	}
	return time.Now().Format("20060102150405") + strconv.Itoa(int(n))
}

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

	const baseSeconds = 3600
	const extraRangeSeconds = 3600

	hostname, err := host.AgentHostname()
	if err != nil {
		hostname = randomHostname()
	}

	hashVal := hashHostname(hostname)
	extraSeconds := int(hashVal % uint32(extraRangeSeconds))
	return time.Duration(baseSeconds+extraSeconds) * time.Second
}
