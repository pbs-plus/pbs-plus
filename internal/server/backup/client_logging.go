package backup

import (
	"bytes"
	"context"
	"io"
	"os"
	"os/exec"
	"time"

	"github.com/fsnotify/fsnotify"
	"github.com/pbs-plus/pbs-plus/internal/log"
)

var connectionFailedPattern = []byte("connection failed")

func monitorPBSClientLogs(ctx context.Context, filePath string, cmd *exec.Cmd) {
	watcher, err := fsnotify.NewWatcher()
	if err != nil {
		log.Error(err, "failed to create watcher")
		return
	}
	defer func() {
		if err := watcher.Close(); err != nil {
			log.Error(err, "")
		}
	}()

	file, err := os.Open(filePath)
	if err != nil {
		log.Error(err, "failed to open file")
		return
	}
	defer func() {
		if err := file.Close(); err != nil {
			log.Error(err, "")
		}
	}()

	offset, err := file.Seek(0, io.SeekEnd)
	if err != nil {
		log.Error(err, "failed to seek file")
		return
	}

	if err := watcher.Add(filePath); err != nil {
		log.Error(err, "failed to add file to watcher")
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
			log.Error(err, "watcher error")

		case <-debounceC:
			debounceC = nil
			newOffset, errored := processFileBuffer(file, offset, buf, cmd)
			offset = newOffset
			if errored {
				return
			}

		case <-ctx.Done():
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
		log.Error(err, "seek error")
		return offset, false
	}

	n, err := file.Read(buf)
	if err != nil && err != io.EOF {
		log.Error(err, "read error")
		return currentPos, false
	}

	if n == 0 {
		return currentPos, false
	}

	if bytes.Contains(buf[:n], connectionFailedPattern) {
		if cmd.Process != nil {
			if err := cmd.Process.Kill(); err != nil {
				log.Error(err, "")
			}
		}
		return currentPos + int64(n), true
	}

	return currentPos + int64(n), false
}
