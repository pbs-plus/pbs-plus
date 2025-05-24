package utils

import (
	"net"
	"strings"
)

func ValidateTargetPath(path string) bool {
	if strings.HasPrefix(path, "agent://") {
		trimmed := strings.TrimPrefix(path, "agent://")

		parts := strings.Split(trimmed, "/")
		if len(parts) != 2 {
			return false
		}

		ip, _ := parts[0], parts[1]

		if net.ParseIP(ip) == nil {
			return false
		}

		return true
	}

	return strings.HasPrefix(path, "/")
}
