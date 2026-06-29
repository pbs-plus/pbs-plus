package updater

import (
	"encoding/json"
	"os"
	"time"

	"github.com/pbs-plus/pbs-plus/internal/log"
)

const (
	pendingSuffix  = ".update-pending"
	previousSuffix = ".previous"
	stagedSuffix   = ".staged"
	badSuffix      = ".bad"

	maxBootAttempts = 3

	CommitGrace = 60 * time.Second
)

type pendingUpdate struct {
	Version      string    `json:"version"`
	PreviousPath string    `json:"previous_path"`
	Attempts     int       `json:"attempts"`
	StartedAt    time.Time `json:"started_at"`
}

func pendingPath(exePath string) string    { return exePath + pendingSuffix }
func previousPathOf(exePath string) string { return exePath + previousSuffix }
func stagedPathOf(exePath string) string   { return exePath + stagedSuffix }

func readPending(exePath string) (*pendingUpdate, bool) {
	data, err := os.ReadFile(pendingPath(exePath))
	if err != nil {
		return nil, false
	}
	var p pendingUpdate
	if err := json.Unmarshal(data, &p); err != nil {
		log.Error(err, "updater: pending marker is corrupt, removing")
		_ = os.Remove(pendingPath(exePath))
		return nil, false
	}
	return &p, true
}

func writePending(exePath string, p *pendingUpdate) error {
	data, err := json.Marshal(p)
	if err != nil {
		return err
	}
	return atomicWriteFile(pendingPath(exePath), data, 0o644)
}

func removePending(exePath string) {
	if err := os.Remove(pendingPath(exePath)); err != nil && !os.IsNotExist(err) {
		log.Error(err, "updater: failed to remove pending marker")
	}
}

func pruneStaleArtifacts() {
	exePath, err := os.Executable()
	if err != nil {
		return
	}
	for _, suffix := range []string{stagedSuffix, ".swapping", badSuffix} {
		if err := os.Remove(exePath + suffix); err != nil && !os.IsNotExist(err) {
			log.Error(err, "updater: failed to prune stale artifact", "suffix", suffix)
		}
	}
}

func CheckPendingOnBoot() bool {
	exePath, err := os.Executable()
	if err != nil {
		return false
	}
	pending, rollback := evaluatePending(exePath)
	if rollback {
		log.Info("updater: rolled back to previous binary, exiting for service manager restart")
		os.Exit(1)
	}
	return pending
}

func evaluatePending(exePath string) (pending bool, rollback bool) {
	p, ok := readPending(exePath)
	if !ok {
		return false, false
	}

	p.Attempts++
	if err := writePending(exePath, p); err != nil {
		log.Error(err, "updater: failed to persist boot-attempt counter")
	}

	log.Info("updater: uncommitted update in flight",
		"version", p.Version, "attempts", p.Attempts, "max", maxBootAttempts)

	if p.Attempts > maxBootAttempts {
		log.Warn("updater: update failed to stabilize, rolling back",
			"version", p.Version, "attempts", p.Attempts)
		if rerr := rollbackToPrevious(exePath, p.PreviousPath); rerr != nil {
			log.Error(rerr, "updater: automatic rollback failed; clearing marker to avoid a permanent loop")
			removePending(exePath)
			return false, false
		}
		removePending(exePath)
		return false, true
	}

	return true, false
}

func CommitUpdate() {
	exePath, err := os.Executable()
	if err != nil {
		return
	}
	if _, ok := readPending(exePath); !ok {
		return
	}
	removePending(exePath)
	log.Info("updater: update committed (agent healthy)")
}

func rollbackToPrevious(exePath, prevPath string) error {
	if prevPath == "" {
		prevPath = previousPathOf(exePath)
	}
	if _, err := os.Stat(prevPath); err != nil {
		return err
	}

	badPath := exePath + badSuffix
	_ = os.Remove(badPath)

	if err := os.Rename(exePath, badPath); err != nil {
		return err
	}
	if err := os.Rename(prevPath, exePath); err != nil {
		_ = os.Rename(badPath, exePath)
		return err
	}

	_ = os.Remove(badPath)
	return nil
}
