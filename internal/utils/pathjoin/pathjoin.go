package pathjoin

import (
	"os"
	"unsafe"
)

func Join(paths ...string) string {
	if len(paths) == 0 {
		return ""
	}

	separator := byte(os.PathSeparator)
	var totalLength int
	for _, path := range paths {
		totalLength += len(path)
	}
	result := make([]byte, 0, totalLength+len(paths)-1)

	for _, path := range paths {
		if path == "" {
			continue // Skip empty paths
		}

		pathBytes := []byte(path)

		// Replace all '/' and '\' with the platform-specific separator
		for j := 0; j < len(pathBytes); j++ {
			if pathBytes[j] == '/' || pathBytes[j] == '\\' {
				pathBytes[j] = separator
			}
		}

		start, end := 0, len(pathBytes)
		for start < end && pathBytes[start] == separator {
			start++
		}
		for end > start && pathBytes[end-1] == separator {
			end--
		}

		if len(result) > 0 && result[len(result)-1] != separator {
			result = append(result, separator)
		}

		result = append(result, pathBytes[start:end]...)
	}

	if len(result) == 0 {
		return ""
	}

	return unsafe.String(unsafe.SliceData(result), len(result))
}
