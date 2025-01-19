package utils

import (
	"fmt"
	"regexp"
	"strings"
)

// ValidateOnCalendar checks if a string is a valid systemd timer OnCalendar value
func ValidateOnCalendar(value string) error {
	// Handle empty value
	if value == "" {
		return fmt.Errorf("OnCalendar value cannot be empty")
	}

	// Special keywords
	specialKeywords := map[string]bool{
		"minutely":     true,
		"hourly":       true,
		"daily":        true,
		"monthly":      true,
		"weekly":       true,
		"yearly":       true,
		"quarterly":    true,
		"semiannually": true,
		"annually":     true,
	}

	// Check for special keywords
	if specialKeywords[strings.ToLower(value)] {
		return nil
	}

	// Regular expressions for different parts of the calendar specification
	weekdaySpec := `(mon|tue|wed|thu|fri|sat|sun)(?:\.\.(mon|tue|wed|thu|fri|sat|sun))?`
	dateSpec := `(?:\d+|(?:\*(?:-\*)?(?:-\*)?))`
	timeSpec := `([0-1][0-9]|2[0-3]):([0-5][0-9])(?::([0-5][0-9]))?`

	// Combined pattern for the entire calendar event
	// Format: [DayOfWeek] Date Time
	pattern := fmt.Sprintf(`^(?:(%s)\s+)?%s\s+%s$`,
		weekdaySpec,
		dateSpec,
		timeSpec,
	)

	re := regexp.MustCompile(strings.ToLower(pattern))
	if !re.MatchString(strings.ToLower(value)) {
		return fmt.Errorf("invalid calendar event format")
	}

	return nil
}
