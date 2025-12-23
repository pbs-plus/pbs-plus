package backup

import (
	"bytes"
	"io"
	"os"
	"os/exec"
	"sync/atomic"
	"time"
	"unsafe"

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

	var debounceTimerPtr unsafe.Pointer
	resetDebounce := func() <-chan time.Time {
		newTimer := time.NewTimer(100 * time.Millisecond)
		oldTimer := (*time.Timer)(atomic.SwapPointer(&debounceTimerPtr, unsafe.Pointer(newTimer)))
		if oldTimer != nil {
			oldTimer.Stop()
			select {
			case <-oldTimer.C:
			default:
			}
		}
		return newTimer.C
	}

	buf := make([]byte, 32*1024) // 32KB buffer

	terminateCh := make(chan struct{})

	go func() {
		<-done
		close(terminateCh)
	}()

	var debounceC <-chan time.Time
	writeEventPending := false

	for {
		if writeEventPending && debounceC == nil {
			debounceC = resetDebounce()
			writeEventPending = false
		}

		select {
		case event, ok := <-watcher.Events:
			if !ok {
				return
			}

			if event.Op&fsnotify.Write == fsnotify.Write {
				writeEventPending = true
			}

		case <-debounceC:
			newOffset, errored := processFileBuffer(file, offset, buf, cmd)
			if errored {
				return
			}
			offset = newOffset
			debounceC = nil

		case err, ok := <-watcher.Errors:
			if !ok {
				return
			}
			syslog.L.Error(err).WithMessage("watcher error").Write()

		case <-terminateCh:
			_, _ = processFileBuffer(file, offset, buf, cmd)
			return
		}
	}
}

func processFileBuffer(file *os.File, offset int64, buf []byte, cmd *exec.Cmd) (int64, bool) {
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
