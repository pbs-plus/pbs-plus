//go:build linux

package api

import (
	"bufio"
	"os"
	"strings"
)

func IsMounted(path string) bool {
	mountInfoFile, err := os.Open("/proc/self/mountinfo")
	if err != nil {
		return false
	}
	defer mountInfoFile.Close()

	scanner := bufio.NewScanner(mountInfoFile)
	for scanner.Scan() {
		fields := strings.Fields(scanner.Text())
		if len(fields) >= 5 && fields[4] == path {
			return true
		}
	}

	return false
}
