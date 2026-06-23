//go:build linux

package tasks

import (
	"bufio"
	"fmt"
	"os"
	"strings"
	"syscall"

	"github.com/pbs-plus/pbs-plus/internal/conf"
)

// AddActive registers upid in the active tasks file (conf.ActiveLogsPath).
// It is idempotent: registering an already-present upid is a no-op.
func AddActive(upid string) error {
	return modifyActiveFile(upid, true)
}

// RemoveActive unregisters upid from the active tasks file.
// It is idempotent: removing an absent upid is a no-op.
func RemoveActive(upid string) error {
	return modifyActiveFile(upid, false)
}

// modifyActiveFile adds or removes a upid line from the active tasks file
// under an exclusive flock. The file stores one UPID per line.
func modifyActiveFile(target string, add bool) error {
	f, err := os.OpenFile(conf.ActiveLogsPath, os.O_RDWR|os.O_CREATE, 0644)
	if err != nil {
		if !add && os.IsNotExist(err) {
			return nil
		}
		return fmt.Errorf("open active tasks: %w", err)
	}
	defer func() { _ = f.Close() }()

	if err := syscall.Flock(int(f.Fd()), syscall.LOCK_EX); err != nil {
		return fmt.Errorf("lock active tasks: %w", err)
	}
	defer func() { _ = syscall.Flock(int(f.Fd()), syscall.LOCK_UN) }()

	var lines []string
	found := false
	scanner := bufio.NewScanner(f)
	for scanner.Scan() {
		line := scanner.Text()
		if strings.TrimSpace(line) == target {
			found = true
			if !add {
				continue
			}
		}
		lines = append(lines, line)
	}
	if add && !found {
		lines = append(lines, target)
	}
	if !add && !found {
		return nil
	}

	if err := f.Truncate(0); err != nil {
		return err
	}
	if _, err := f.Seek(0, 0); err != nil {
		return err
	}
	w := bufio.NewWriter(f)
	for _, line := range lines {
		if _, err := w.WriteString(line + "\n"); err != nil {
			return err
		}
	}
	if err := w.Flush(); err != nil {
		return err
	}
	return f.Sync()
}
