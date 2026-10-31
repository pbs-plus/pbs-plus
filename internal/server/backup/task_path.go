//go:build linux

package backup

import (
	"bufio"
	"io"
	"os"
	"strings"

	"github.com/pbs-plus/pbs-plus/internal/proxmox"
	"github.com/pbs-plus/pbs-plus/internal/proxmox/tasklog"
)

// taskReasonTailBytes bounds the tail read because a task log can hold millions of file lines.
const taskReasonTailBytes = 8 * 1024

func getTaskLogPath(upid string) string {
	path, err := tasklog.UPIDLogPath(upid)
	if err != nil {
		return ""
	}
	return path
}

func taskFailureReason(upid string) (reason string) {
	path := getTaskLogPath(upid)
	if path == "" {
		return ""
	}
	file, err := os.Open(path)
	if err != nil {
		return ""
	}
	defer func() {
		if cerr := file.Close(); cerr != nil {
			reason = ""
		}
	}()
	info, err := file.Stat()
	if err != nil {
		return ""
	}
	if offset := info.Size() - taskReasonTailBytes; offset > 0 {
		if _, err := file.Seek(offset, io.SeekStart); err != nil {
			return ""
		}
	}
	last := ""
	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		if line := strings.TrimSpace(scanner.Text()); line != "" {
			last = line
		}
	}
	if err := scanner.Err(); err != nil {
		return ""
	}
	after, ok := strings.CutPrefix(proxmox.TrimLogLineTimestamp(last), "TASK ERROR: ")
	if !ok {
		return ""
	}
	return after
}
