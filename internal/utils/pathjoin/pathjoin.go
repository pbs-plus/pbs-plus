package pathjoin

import (
	"os"
	"strings"
)

func Join(paths ...string) string {
	if len(paths) == 0 {
		return ""
	}

	separator := string(os.PathSeparator)
	var result strings.Builder

	for i, path := range paths {
		if i > 0 && !strings.HasSuffix(result.String(), separator) && !strings.HasPrefix(path, separator) {
			result.WriteString(separator)
		}
		result.WriteString(path)
	}

	return result.String()
}
