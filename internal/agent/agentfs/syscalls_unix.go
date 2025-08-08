//go:build unix

package agentfs

import (
	"math"
	"os/exec"
	"strconv"
	"strings"
	"syscall"

	"github.com/pbs-plus/pbs-plus/internal/agent/agentfs/types"
	"github.com/pbs-plus/pbs-plus/internal/utils"
)

func GetAllocGranularity() int {
	// On Linux, the allocation granularity is typically the page size
	pageSize := syscall.Getpagesize()
	return pageSize
}

// getPosixACL uses the "getfacl" command to obtain the ACL for path.
// It returns a slice of PosixACLEntry. It ignores comment lines.
func getPosixACL(path string) ([]types.PosixACL, error) {
	out, err := exec.Command("getfacl", "-p", "-c", path).Output()
	if err != nil {
		return nil, err
	}
	lines := strings.Split(string(out), "\n")
	var entries []types.PosixACL
	for _, line := range lines {
		line = strings.TrimSpace(line)
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		// Expected entry format: tag:qualifier:perms, e.g.,
		// "user:1001:rwx" or "group::r-x"
		parts := strings.Split(line, ":")
		if len(parts) < 3 {
			continue
		}
		tag := parts[0]
		qualifier := parts[1]
		permsStr := parts[2]
		var id int32 = -1
		if qualifier != "" {
			if uid, err := strconv.ParseInt(qualifier, 10, 32); err == nil {
				if uid >= math.MinInt32 && uid <= math.MaxInt32 {
					id = int32(uid)
				}
			}
		}
		var perms uint8 = 0
		if strings.Contains(permsStr, "r") {
			perms |= 4
		}
		if strings.Contains(permsStr, "w") {
			perms |= 2
		}
		if strings.Contains(permsStr, "x") {
			perms |= 1
		}
		entry := types.PosixACL{
			Tag:   utils.StringToBytes(tag),
			ID:    id,
			Perms: perms,
		}
		entries = append(entries, entry)
	}
	return entries, nil
}
