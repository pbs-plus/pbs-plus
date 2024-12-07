package utils

import (
	"regexp"
	"strings"
)

// Converts a glob pattern to a regex pattern
func GlobToRegex(pattern string) string {
	// Handle leading / or \ for current directory matches
	if strings.HasPrefix(pattern, "/") || strings.HasPrefix(pattern, "\\") {
		pattern = "." + pattern
	} else {
		pattern = "**/" + pattern
	}

	// Escape characters and replace glob wildcards
	regexPattern := regexp.QuoteMeta(pattern)
	regexPattern = strings.ReplaceAll(regexPattern, `\*\*`, `.*`)    // Double star for any depth
	regexPattern = strings.ReplaceAll(regexPattern, `\*`, `[^/\\]*`) // Single star for single level
	regexPattern = strings.ReplaceAll(regexPattern, `\?`, `.`)       // ? for any single character
	regexPattern = strings.ReplaceAll(regexPattern, `\[`, `[`)       // Keep character sets
	regexPattern = strings.ReplaceAll(regexPattern, `\]`, `]`)
	regexPattern = strings.ReplaceAll(regexPattern, `\!`, `!`) // Keep negation sets

	// Replace forward and backward slashes with a platform-agnostic pattern
	regexPattern = strings.ReplaceAll(regexPattern, `/`, `[\\\\/]`)  // Match / or \
	regexPattern = strings.ReplaceAll(regexPattern, `\\`, `[\\\\/]`) // Match / or \

	// Handle trailing slashes for directories
	if strings.HasSuffix(pattern, "/") || strings.HasSuffix(pattern, "\\") {
		regexPattern += `(?:[\\/]|$)` // Match only directories
	} else {
		regexPattern += `(?:$|[\\/]$)` // Match files and subdirectories
	}

	return regexPattern
}

func IsValidPattern(pattern string) bool {
	isValid := true

	// Attempt to convert the glob pattern to regex
	defer func() {
		// Recover in case of panic during regex compilation
		if r := recover(); r != nil {
			isValid = false
		}
	}()

	// Convert to regex and attempt compilation
	regex := GlobToRegex(pattern)
	_, err := regexp.Compile(regex)
	if err != nil {
		isValid = false
	}

	return isValid
}
