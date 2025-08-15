//go:build unix

package agentfs

import (
	"math"
	"os/exec"
	"runtime"
	"strconv"
	"strings"
	"syscall"

	"github.com/pbs-plus/pbs-plus/internal/agent/agentfs/types"
)

func getAllocGranularity() int {
	// On Linux, the allocation granularity is typically the page size
	pageSize := syscall.Getpagesize()
	return pageSize
}

func getPosixACL(path string) ([]types.PosixACL, error) {
	var cmd *exec.Cmd

	switch runtime.GOOS {
	case "linux":
		cmd = exec.Command("getfacl", "-p", "-c", path)
		return parsePosixACL(cmd)
	case "freebsd":
		cmd = exec.Command("getfacl", "-q", path)
		return parseNFSv4ACL(cmd)
	default:
		cmd = exec.Command("getfacl", path)
		return parsePosixACL(cmd)
	}
}

func parsePosixACL(cmd *exec.Cmd) ([]types.PosixACL, error) {
	out, err := cmd.Output()
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
			Tag:   tag,
			ID:    id,
			Perms: perms,
		}
		entries = append(entries, entry)
	}
	return entries, nil
}

func parseNFSv4ACL(cmd *exec.Cmd) ([]types.PosixACL, error) {
	out, err := cmd.Output()
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

		// NFSv4 format: who:permissions:flags:type
		parts := strings.Split(line, ":")
		if len(parts) < 4 {
			continue
		}

		who := parts[0]      // "owner@", "group@", "everyone@"
		permsStr := parts[1] // "rw-p--aARWcCos"
		// flags := parts[2]      // "-------" (inheritance flags)
		aclType := parts[3] // "allow" or "deny"

		// Skip deny entries for simplicity
		if aclType != "allow" {
			continue
		}

		// Convert NFSv4 "who" to POSIX-like tag
		var tag string
		var id int32 = -1

		switch who {
		case "owner@":
			tag = "user"
		case "group@":
			tag = "group"
		case "everyone@":
			tag = "other"
		default:
			// Handle specific user/group (if any)
			tag = "user"
		}

		// Convert NFSv4 permissions to simple rwx
		var perms uint8 = 0
		if strings.Contains(permsStr, "r") {
			perms |= 4 // read
		}
		if strings.Contains(permsStr, "w") || strings.Contains(permsStr, "W") {
			perms |= 2 // write
		}
		if strings.Contains(permsStr, "x") {
			perms |= 1 // execute
		}

		entry := types.PosixACL{
			Tag:   tag,
			ID:    id,
			Perms: perms,
		}
		entries = append(entries, entry)
	}
	return entries, nil
}
