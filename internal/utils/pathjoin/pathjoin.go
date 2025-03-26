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
	invalidSeperator := "/"
	if separator == "/" {
		invalidSeperator = "\\"
	}

	var result strings.Builder

	for _, path := range paths {
		subpaths := strings.Split(path, invalidSeperator)
		for i, subpath := range subpaths {
			if i > 0 && !strings.HasSuffix(result.String(), separator) && !strings.HasPrefix(subpath, separator) {
				result.WriteString(separator)
			}
			result.WriteString(subpath)
		}
	}

	return result.String()
}
