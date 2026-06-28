//go:build linux

package tasklog

import (
	"bufio"
	"fmt"
	"os"
	"strings"
	"syscall"

	"github.com/pbs-plus/pbs-plus/internal/conf"
	"log/slog"
)

func AddActive(upid string) error {
	return modifyActiveFile(upid, true)
}

func RemoveActive(upid string) error {
	return modifyActiveFile(upid, false)
}

func IsActive(upid string) bool {
	f, err := os.Open(conf.ActiveLogsPath)
	if err != nil {
		return false
	}
	defer func() {
		if cerr := f.Close(); cerr != nil {
			slog.Error(cerr.Error())
		}
	}()

	scanner := bufio.NewScanner(f)
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line == "" {
			continue
		}
		parts := strings.Fields(line)
		if parts[0] == upid {
			return true
		}
	}
	return false
}

func CleanupActiveTasks() error {
	targetNode := "pbsplus"

	f, err := os.OpenFile(conf.ActiveLogsPath, os.O_RDWR|os.O_CREATE, 0660)
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return fmt.Errorf("tasklog: open active tasks: %w", err)
	}
	defer func() {
		if cerr := f.Close(); cerr != nil {
			slog.Error(cerr.Error())
		}
	}()

	if err := syscall.Flock(int(f.Fd()), syscall.LOCK_EX); err != nil {
		return fmt.Errorf("tasklog: lock active tasks: %w", err)
	}
	defer func() {
		if err := syscall.Flock(int(f.Fd()), syscall.LOCK_UN); err != nil {
			slog.Error(err.Error())
		}
	}()

	var lines []string
	scanner := bufio.NewScanner(f)
	for scanner.Scan() {
		line := scanner.Text()
		parts := strings.SplitN(line, ":", 4)
		if len(parts) >= 2 && parts[1] == targetNode {
			continue
		}
		lines = append(lines, line)
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

func modifyActiveFile(target string, add bool) error {
	f, err := os.OpenFile(conf.ActiveLogsPath, os.O_RDWR|os.O_CREATE, 0660)
	if err != nil {
		if !add && os.IsNotExist(err) {
			return nil
		}
		return fmt.Errorf("tasklog: open active tasks: %w", err)
	}
	defer func() {
		if cerr := f.Close(); cerr != nil {
			slog.Error(cerr.Error())
		}
	}()

	if err := syscall.Flock(int(f.Fd()), syscall.LOCK_EX); err != nil {
		return fmt.Errorf("tasklog: lock active tasks: %w", err)
	}
	defer func() {
		if err := syscall.Flock(int(f.Fd()), syscall.LOCK_UN); err != nil {
			slog.Error(err.Error())
		}
	}()

	var lines []string
	found := false
	scanner := bufio.NewScanner(f)
	for scanner.Scan() {
		line := scanner.Text()
		fields := strings.Fields(line)
		if len(fields) > 0 && fields[0] == target {
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
