package agent

import (
	"fmt"
	"hash/fnv"
	"os"
	"strconv"
	"time"

	"golang.org/x/exp/rand"
)

// ValidateHostname checks that host conforms to RFC 1123 DNS label rules.
func ValidateHostname(host string) error {
	if len(host) == 0 || len(host) > 253 {
		return fmt.Errorf("invalid hostname: %s", host)
	}
	labelLen := 0
	labelStart := 0
	for i := 0; i <= len(host); i++ {
		var c byte
		if i < len(host) {
			c = host[i]
		}
		if i == len(host) || c == '.' {
			if labelLen == 0 || host[labelStart] == '-' || host[i-1] == '-' {
				return fmt.Errorf("invalid hostname: %s", host)
			}
			labelLen = 0
			labelStart = i + 1
			continue
		}
		if c > 0x7F {
			return fmt.Errorf("invalid hostname: %s", host)
		}
		if (c >= 'A' && c <= 'Z') || (c >= 'a' && c <= 'z') || (c >= '0' && c <= '9') || c == '-' {
			labelLen++
			if labelLen > 63 {
				return fmt.Errorf("invalid hostname: %s", host)
			}
		} else {
			return fmt.Errorf("invalid hostname: %s", host)
		}
	}

	return nil
}

// GetAgentHostname returns the agent hostname from the PBS_PLUS_HOSTNAME
// environment variable, falling back to os.Hostname().
func GetAgentHostname() (string, error) {
	host := os.Getenv("PBS_PLUS_HOSTNAME")
	if host == "" {
		return os.Hostname()
	}

	if err := ValidateHostname(host); err != nil {
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
