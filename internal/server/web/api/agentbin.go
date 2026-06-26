//go:build linux

package api

import (
	"embed"
	"strings"

	"github.com/pbs-plus/pbs-plus/internal/log"
)

//go:embed agentbin/VERSION agentbin/checksums.txt
var agentBinFS embed.FS

// embeddedChecksums maps artifact filenames to their expected sha256 hashes.
// Populated at release build time from the GitHub release checksums.
// For local/dev builds the map is empty — downloads are served without
// verification (with a warning logged).
var embeddedChecksums map[string]string

func init() {
	embeddedChecksums = make(map[string]string)
	data, err := agentBinFS.ReadFile("agentbin/checksums.txt")
	if err != nil {
		log.Error(err, "failed to read embedded checksums")
		return
	}
	for line := range strings.SplitSeq(string(data), "\n") {
		line = strings.TrimSpace(line)
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		parts := strings.SplitN(line, "=", 2)
		if len(parts) != 2 {
			continue
		}
		filename := strings.TrimSpace(parts[0])
		checksum := strings.ToLower(strings.TrimSpace(parts[1]))
		if filename != "" && checksum != "" {
			embeddedChecksums[filename] = checksum
		}
	}
}

func embeddedVersion() string {
	data, err := agentBinFS.ReadFile("agentbin/VERSION")
	if err != nil {
		return ""
	}
	v := strings.TrimSpace(string(data))
	if v == "" || v == "dev" {
		return ""
	}
	return v
}

// embeddedChecksum returns the expected sha256 for the given artifact
// filename and whether a checksum is known for it.
func embeddedChecksum(filename string) (string, bool) {
	c, ok := embeddedChecksums[filename]
	return c, ok
}
