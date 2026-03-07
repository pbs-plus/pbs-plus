package backup

import (
	"bytes"
	"io"
	"os"
	"os/exec"
	"time"

	"github.com/fsnotify/fsnotify"
	"github.com/pbs-plus/pbs-plus/internal/syslog"
)

var connectionFailedPattern = []byte("connection failed")

func monitorPBSClientLogs(filePath string, cmd *exec.Cmd, done <-chan struct{}) {
	watcher, err := fsnotify.NewWatcher()
	if err != nil {
		syslog.L.Error(err).WithMessage("failed to create watcher").Write()
		return
	}
	defer watcher.Close()

	file, err := os.Open(filePath)
	if err != nil {
		syslog.L.Error(err).WithMessage("failed to open file").Write()
		return
	}
	defer file.Close()

	offset, err := file.Seek(0, io.SeekEnd)
	if err != nil {
		syslog.L.Error(err).WithMessage("failed to seek file").Write()
		return
	}

	if err := watcher.Add(filePath); err != nil {
		syslog.L.Error(err).WithMessage("failed to add file to watcher").Write()
		return
	}

	buf := make([]byte, 32*1024)

	var debounceTimer *time.Timer
	defer func() {
		if debounceTimer != nil {
			debounceTimer.Stop()
		}
	}()

	resetDebounce := func() <-chan time.Time {
		if debounceTimer != nil {
			debounceTimer.Stop()
			select {
			case <-debounceTimer.C:
			default:
			}
		}
		debounceTimer = time.NewTimer(100 * time.Millisecond)
		return debounceTimer.C
	}

	var debounceC <-chan time.Time

	for {
		select {
		case event, ok := <-watcher.Events:
			if !ok {
				_, _ = processFileBuffer(file, offset, buf, cmd)
				return
			}
			if event.Op&fsnotify.Write == fsnotify.Write {
				debounceC = resetDebounce()
			}

		case err, ok := <-watcher.Errors:
			if !ok {
				_, _ = processFileBuffer(file, offset, buf, cmd)
				return
			}
			syslog.L.Error(err).WithMessage("watcher error").Write()

		case <-debounceC:
			debounceC = nil
			newOffset, errored := processFileBuffer(file, offset, buf, cmd)
			offset = newOffset
			if errored {
				return
			}

		case <-done:
			_, _ = processFileBuffer(file, offset, buf, cmd)
			return
		}
	}
}

func processFileBuffer(
	file *os.File,
	offset int64,
	buf []byte,
	cmd *exec.Cmd,
) (int64, bool) {
	currentPos, err := file.Seek(offset, io.SeekStart)
	if err != nil {
		syslog.L.Error(err).WithMessage("seek error").Write()
		return offset, false
	}

	n, err := file.Read(buf)
	if err != nil && err != io.EOF {
		syslog.L.Error(err).WithMessage("read error").Write()
		return currentPos, false
	}

	if n == 0 {
		return currentPos, false
	}

	if bytes.Contains(buf[:n], connectionFailedPattern) {
		if cmd.Process != nil {
			_ = cmd.Process.Kill()
		}
		return currentPos + int64(n), true
	}

	return currentPos + int64(n), false
}
