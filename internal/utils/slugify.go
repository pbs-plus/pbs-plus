package utils

import "strings"

func Slugify(input string) string {
	// Convert the input string into a slice of runes for proper Unicode handling.
	runes := []rune(input)
	var hasHyphen, hasSpace bool

	// Check for the existence of a space and a hyphen.
	for _, r := range runes {
		if r == '-' {
			hasHyphen = true
		} else if r == ' ' {
			hasSpace = true
		}
		// Break early if both are found
		if hasHyphen && hasSpace {
			break
		}
	}

	// If there are no spaces, return the input as is.
	if !hasSpace {
		return input
	}

	// If there are spaces but no hyphens, replace all spaces with hyphens.
	if !hasHyphen {
		for i, r := range runes {
			if r == ' ' {
				runes[i] = '-'
			}
		}
		return string(runes)
	}

	// In case the string contains both spaces and hyphens,
	// preserve any space that is adjacent to a hyphen.
	var builder strings.Builder
	builder.Grow(len(runes)) // Optional: allocate enough capacity

	for i, r := range runes {
		if r == ' ' {
			prevIsHyphen := i > 0 && runes[i-1] == '-'
			nextIsHyphen := i < len(runes)-1 && runes[i+1] == '-'
			if prevIsHyphen || nextIsHyphen {
				builder.WriteRune(r) // Preserve the space
			} else {
				builder.WriteRune('-') // Replace space with hyphen
			}
		} else {
			builder.WriteRune(r)
		}
	}

	return builder.String()
}
