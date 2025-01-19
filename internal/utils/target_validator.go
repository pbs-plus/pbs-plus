package utils

import (
	"net"
	"regexp"
	"strings"
)

func ValidateTargetPath(path string) bool {
	if strings.HasPrefix(path, "agent://") {
		trimmed := strings.TrimPrefix(path, "agent://")

		parts := strings.Split(trimmed, "/")
		if len(parts) != 2 {
			return false
		}

		ip, driveLetter := parts[0], parts[1]

		if net.ParseIP(ip) == nil {
			return false
		}

		driveLetterPattern := regexp.MustCompile(`^[a-zA-Z]$`)
		return driveLetterPattern.MatchString(driveLetter)
	}

	return strings.HasPrefix(path, "/")
}
