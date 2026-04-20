package utils

import (
	"fmt"

	"github.com/pbs-plus/pbs-plus/internal/calendarevent"
)

// ValidateOnCalendar checks if a string is a valid calendar event specification
func ValidateOnCalendar(value string) error {
	if value == "" {
		return fmt.Errorf("calendar specification cannot be empty")
	}

	_, err := calendarevent.Parse(value)
	if err != nil {
		return fmt.Errorf("invalid calendar specification: %v", err)
	}

	return nil
}
