package utils

import (
	"fmt"
	"os"
)

func GetAgentHostname() (string, error) {
	host := os.Getenv("AGENT_HOSTNAME")
	if host == "" {
		return os.Hostname()
	}
	if len(host) == 0 || len(host) > 253 {
		return "", fmt.Errorf("invalid hostname: %s", host)
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
				return "", fmt.Errorf("invalid hostname: %s", host)
			}
			labelLen = 0
			labelStart = i + 1
			continue
		}
		if c > 0x7F {
			return "", fmt.Errorf("invalid hostname: %s", host)
		}
		if (c >= 'A' && c <= 'Z') || (c >= 'a' && c <= 'z') || (c >= '0' && c <= '9') || c == '-' {
			labelLen++
			if labelLen > 63 {
				return "", fmt.Errorf("invalid hostname: %s", host)
			}
		} else {
			return "", fmt.Errorf("invalid hostname: %s", host)
		}
	}
	return host, nil
}
