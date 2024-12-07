package utils

import "regexp"

func IsValidNamespace(namespace string) bool {
	// Define the regex pattern based on the provided pattern.
	const pattern = `^(?:(?:(?:[A-Za-z0-9_][A-Za-z0-9._\-]*)/){0,7}(?:[A-Za-z0-9_][A-Za-z0-9._\-]*))?$`

	// Compile the regular expression.
	re := regexp.MustCompile(pattern)

	// Check if the string matches the regex.
	return re.MatchString(namespace)
}
