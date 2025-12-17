//go:build !linux && !windows

package updater

import (
	"fmt"

	"github.com/pbs-plus/pbs-plus/internal/syslog"
)

func restartCallback(_ Config) {
	// No OS-specific cleanup on generic platforms.
	syslog.L.Error(fmt.Errorf("no supported supervisors detected")).WithMessage("manual service restart required for update").Write()
}
