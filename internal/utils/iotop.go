package utils

import (
	"bufio"
	"fmt"
	"os"
	"strconv"
	"strings"
)

func GetProcIO(pid int) (readBytes int64, writeBytes int64, err error) {
	filePath := fmt.Sprintf("/proc/%d/io", pid)
	f, err := os.Open(filePath)
	if err != nil {
		return 0, 0, err
	}
	defer f.Close()

	scanner := bufio.NewScanner(f)
	for scanner.Scan() {
		line := scanner.Text()

		// Example lines:
		// read_bytes: 123456
		// write_bytes: 654321
		if strings.HasPrefix(line, "read_bytes:") {
			parts := strings.Fields(line)
			if len(parts) == 2 {
				readBytes, err = strconv.ParseInt(parts[1], 10, 64)
				if err != nil {
					return 0, 0, err
				}
			}
		} else if strings.HasPrefix(line, "write_bytes:") {
			parts := strings.Fields(line)
			if len(parts) == 2 {
				writeBytes, err = strconv.ParseInt(parts[1], 10, 64)
				if err != nil {
					return 0, 0, err
				}
			}
		}
	}

	if err = scanner.Err(); err != nil {
		return 0, 0, err
	}
	return readBytes, writeBytes, nil
}

// humanReadableBytes formats the given number of bytes into a human-readable string.
func HumanReadableBytes(bytes int64) string {
	const (
		KB = 1024
		MB = KB * 1024
		GB = MB * 1024
	)

	switch {
	case bytes >= GB:
		return fmt.Sprintf("%.2f GB/s", float64(bytes)/float64(GB))
	case bytes >= MB:
		return fmt.Sprintf("%.2f MB/s", float64(bytes)/float64(MB))
	case bytes >= KB:
		return fmt.Sprintf("%.2f KB/s", float64(bytes)/float64(KB))
	default:
		return fmt.Sprintf("%d B/s", bytes)
	}
}
