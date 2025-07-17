package utils

import (
	"hash/fnv"
	"os"
	"strconv"
	"time"

	"golang.org/x/exp/rand"
)

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

	const baseSeconds = 3600 // 1 hour
	// Adjust extra seconds range if desired.
	const extraRangeSeconds = 3600 // up to an extra 1 hour, so total range is 1-2 hours

	hostname, err := os.Hostname()
	if err != nil {
		// Fall back to a random hostname if we can't get one.
		hostname = RandomHostname()
	}

	hashVal := HashHostname(hostname)
	// Using the modulus to get a value between 0 and extraRangeSeconds-1.
	extraSeconds := int(hashVal % uint32(extraRangeSeconds))
	return time.Duration(baseSeconds+extraSeconds) * time.Second
}

func HashHostname(host string) uint32 {
	h := fnv.New32a()
	_, _ = h.Write([]byte(host))
	return h.Sum32()
}

// randomHostname generates a pseudo-random string to use as a fallback hostname.
func RandomHostname() string {
	return time.Now().Format("20060102150405") + strconv.Itoa(rand.Intn(1000))
}
