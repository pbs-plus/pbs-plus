package pathjoin

import (
	"os"
	"strings"
)

func Join(paths ...string) string {
	if len(paths) == 0 {
		return ""
	}

	separator := byte(os.PathSeparator)
	var result strings.Builder

	for _, path := range paths {
		if path == "" {
			continue
		}

		if separator == '\\' && strings.ContainsRune(path, '/') {
			path = strings.ReplaceAll(path, "/", "\\")
		} else if separator == '/' && strings.ContainsRune(path, '\\') {
			path = strings.ReplaceAll(path, "\\", "/")
		}

		if result.Len() > 0 {
			lastChar := result.String()[result.Len()-1]
			firstChar := path[0]
			if lastChar != separator && firstChar != separator {
				result.WriteByte(separator)
			} else if lastChar == separator && firstChar == separator {
				path = path[1:]
			}
		}

		result.WriteString(path)
	}

	return result.String()
}
