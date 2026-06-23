package tape

import (
	"os"
	"strings"
)

// ResolveDevice converts a SCSI generic device path (-sg) to the
// corresponding non-rewind tape device (-nst). udev by-id paths ending
// in -sg point to SCSI generic devices; the matching tape device has the
// same name with -nst.
func ResolveDevice(path string) string {
	if path == "" {
		return path
	}
	if before, ok := strings.CutSuffix(path, "-sg"); ok {
		nstPath := before + "-nst"
		if _, err := os.Stat(nstPath); err == nil {
			return nstPath
		}
	}
	return path
}
