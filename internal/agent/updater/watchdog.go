package updater

import (
	"os"

	"github.com/pbs-plus/pbs-plus/internal/agent/binswap"
	"github.com/pbs-plus/pbs-plus/internal/log"
)

func CheckPendingOnBoot() bool {
	mgr, err := binswap.NewFromExecutable()
	if err != nil {
		return false
	}
	pending, rollback := mgr.CheckPending()
	if rollback {
		log.Info("updater: rolled back to previous binary, exiting for service manager restart")
		os.Exit(1)
	}
	if pending {
		mgr.Prune()
	}
	return pending
}

func CommitUpdate() {
	mgr, err := binswap.NewFromExecutable()
	if err != nil {
		return
	}
	mgr.Commit()
	log.Info("updater: update committed (agent healthy)")
}
