package utils

import (
	"bytes"
	"fmt"
	"os/exec"
	"strings"
)

// ValidateOnCalendar checks if a string is a valid systemd timer OnCalendar value
func ValidateOnCalendar(value string) error {
	if value == "" {
		return fmt.Errorf("calendar specification cannot be empty")
	}

	cmd := exec.Command("/usr/bin/systemd-analyze", "calendar", value)
	var stderr bytes.Buffer
	cmd.Stderr = &stderr

	if err := cmd.Run(); err != nil {
		if _, ok := err.(*exec.ExitError); ok {
			return fmt.Errorf("invalid calendar specification: %s", strings.TrimSpace(stderr.String()))
		}
		return fmt.Errorf("failed to execute systemd-analyze: %v", err)
	}

	return nil
}
