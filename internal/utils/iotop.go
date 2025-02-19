package utils

import (
	"bufio"
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"
)

var previousRead = NewLRUCache(256)
var previousWrite = NewLRUCache(256)
var previousTime = NewLRUCache(256)

func GetProcIO(pid int) (read, write int64, readSpeed, writeSpeed float64, err error) {
	filePath := fmt.Sprintf("/proc/%d/io", pid)
	f, err := os.Open(filePath)
	if err != nil {
		return 0, 0, 0, 0, err
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
				read, err = strconv.ParseInt(parts[1], 10, 64)
				if err != nil {
					return 0, 0, 0, 0, err
				}
			}
		} else if strings.HasPrefix(line, "write_bytes:") {
			parts := strings.Fields(line)
			if len(parts) == 2 {
				write, err = strconv.ParseInt(parts[1], 10, 64)
				if err != nil {
					return 0, 0, 0, 0, err
				}
			}
		}
	}

	if err = scanner.Err(); err != nil {
		return 0, 0, 0, 0, err
	}

	pidString := fmt.Sprintf("%d", pid)

	lastTime, ok := previousTime.Get(pidString)
	if !ok {
		lastTime = time.Now()
	}

	initialRead, ok := previousRead.Get(pidString)
	if !ok {
		initialRead = int64(0)
	}

	initialWrite, ok := previousWrite.Get(pidString)
	if !ok {
		initialWrite = int64(0)
	}

	timeSince := time.Since(lastTime.(time.Time)).Seconds()
	if timeSince == 0 {
		timeSince = float64(1)
	}

	rateFactor := 1.0 / timeSince
	readRate := float64(read-initialRead.(int64)) * rateFactor
	writeRate := float64(write-initialWrite.(int64)) * rateFactor

	previousRead.Set(pidString, read)
	previousWrite.Set(pidString, write)
	previousTime.Set(pidString, time.Now())

	return read, write, readRate, writeRate, nil
}

// humanReadableBytes formats the given number of bytes into a human-readable string.
func HumanReadableBytes(bytes int64) string {
	const (
		KB = 1024
		MB = KB * 1024
		GB = MB * 1024
		TB = GB * 1024
	)

	switch {
	case bytes >= TB:
		return fmt.Sprintf("%.2f TB", float64(bytes)/float64(TB))
	case bytes >= GB:
		return fmt.Sprintf("%.2f GB", float64(bytes)/float64(GB))
	case bytes >= MB:
		return fmt.Sprintf("%.2f MB", float64(bytes)/float64(MB))
	case bytes >= KB:
		return fmt.Sprintf("%.2f KB", float64(bytes)/float64(KB))
	default:
		return fmt.Sprintf("%d B", bytes)
	}
}

func HumanReadableSpeed(speed float64) string {
	const (
		KB = 1024.0
		MB = KB * 1024
		GB = MB * 1024
	)
	switch {
	case speed >= GB:
		return fmt.Sprintf("%.2f GB/s", speed/GB)
	case speed >= MB:
		return fmt.Sprintf("%.2f MB/s", speed/MB)
	case speed >= KB:
		return fmt.Sprintf("%.2f KB/s", speed/KB)
	default:
		return fmt.Sprintf("%.2f B/s", speed)
	}
}
