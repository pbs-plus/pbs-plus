package utils

import (
	"fmt"
	"regexp"
	"strings"
)

// GlobToRegex converts a glob pattern to a regex-compatible string.
func GlobToRegex(glob string) (string, error) {
	// Check for negation at the start
	negate := false
	if strings.HasPrefix(glob, "!") {
		negate = true
		glob = glob[1:]
	}

	glob = strings.TrimSuffix(glob, "/")
	glob = strings.TrimSuffix(glob, "\\")

	var regex strings.Builder
	regex.WriteString(".*") // Match any path leading to the pattern

	for i := 0; i < len(glob); i++ {
		c := glob[i]
		switch c {
		case '*':
			if i+1 < len(glob) && glob[i+1] == '*' {
				// Handle "**" for zero or more directories/files
				regex.WriteString(".*")
				i++ // Skip next '*'
			} else {
				// Handle "*" for zero or more characters except '/'
				regex.WriteString("[^/\\\\]*")
			}
		case '.':
			// Escape '.' in regex
			regex.WriteString("\\.")
		case '/':
			// Match both '/' and '\'
			regex.WriteString("[\\\\/]")
		case '\\':
			// Match literal '\' if escaped
			regex.WriteString("\\\\")
		case '[':
			// Preserve character classes
			endIdx := strings.IndexByte(glob[i:], ']')
			if endIdx == -1 {
				return "", fmt.Errorf("invalid glob pattern, unmatched '['")
			}
			class := glob[i : i+endIdx+1]
			if negate {
				// Add negation directly inside the character class
				regex.WriteString("[^" + class[1:])
			} else {
				regex.WriteString(class)
			}
			i += endIdx
		default:
			// Escape all other special regex characters
			if strings.ContainsRune(`+()|^$!`, rune(c)) {
				regex.WriteString("\\")
			}
			regex.WriteByte(c)
		}
	}

	// Add optional end match
	regex.WriteString("(?:$|[\\\\/])")

	// Return the final regex
	return regex.String(), nil
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
	regex, err := GlobToRegex(pattern)
	if err != nil {
		return false
	}

	_, err = regexp.Compile(regex)
	if err != nil {
		isValid = false
	}

	return isValid
}
