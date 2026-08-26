//go:build linux

package mountapi

import (
	"bufio"
	"os"
	"strings"

	"github.com/pbs-plus/pbs-plus/internal/log"
)

func IsMounted(path string) bool {
	mountInfoFile, err := os.Open("/proc/self/mountinfo")
	if err != nil {
		return false
	}
	defer func() {
		if err := mountInfoFile.Close(); err != nil {
			log.Error(err, "")
		}
	}()

	scanner := bufio.NewScanner(mountInfoFile)
	for scanner.Scan() {
		fields := strings.Fields(scanner.Text())
		if len(fields) >= 5 && fields[4] == path {
			return true
		}
	}

	return false
}
