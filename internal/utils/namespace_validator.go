package utils

import "regexp"

func IsValidNamespace(namespace string) bool {
	const pattern = `^(?:(?:(?:[A-Za-z0-9_][A-Za-z0-9._\-]*)/){0,7}(?:[A-Za-z0-9_][A-Za-z0-9._\-]*))?$`
	re := regexp.MustCompile(pattern)
	return re.MatchString(namespace)
}

func IsValidID(id string) bool {
	const pattern = `^(?:[A-Za-z0-9_][A-Za-z0-9._\-]*)$`
	re := regexp.MustCompile(pattern)
	return re.MatchString(id)
}
