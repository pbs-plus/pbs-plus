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
	"github.com/sonroyaalmerol/pbs-plus/internal/syslog"
)

// Pre-compile the error pattern for faster matching
var connectionFailedPattern = []byte("connection failed")

func monitorPBSClientLogs(filePath string, cmd *exec.Cmd, done <-chan struct{}) {
	// Create a new watcher with a specific buffer size to handle high event volume
	watcher, err := fsnotify.NewWatcher()
	if err != nil {
		syslog.L.Error(err).WithMessage("failed to create watcher").Write()
		return
	}
	defer watcher.Close()

	// Open the file for reading
	file, err := os.Open(filePath)
	if err != nil {
		syslog.L.Error(err).WithMessage("failed to open file").Write()
		return
	}
	defer file.Close()

	// Start reading at the file's end
	offset, err := file.Seek(0, io.SeekEnd)
	if err != nil {
		syslog.L.Error(err).WithMessage("failed to seek file").Write()
		return
	}

	// Add the file to the watcher
	if err := watcher.Add(filePath); err != nil {
		syslog.L.Error(err).WithMessage("failed to add file to watcher").Write()
		return
	}

	// Debounce timer setup - use atomic operations for thread safety
	var debounceTimerPtr unsafe.Pointer
	resetDebounce := func() <-chan time.Time {
		// Create a new timer
		newTimer := time.NewTimer(100 * time.Millisecond)
		// Atomically swap in the new timer
		oldTimer := (*time.Timer)(atomic.SwapPointer(&debounceTimerPtr, unsafe.Pointer(newTimer)))
		// Stop and clean up the old timer if it exists
		if oldTimer != nil {
			oldTimer.Stop()
			// Drain the channel if needed
			select {
			case <-oldTimer.C:
			default:
			}
		}
		return newTimer.C
	}

	// Pre-allocate a buffer for reading
	buf := make([]byte, 32*1024) // 32KB buffer

	// Use a channel for termination signaling
	terminateCh := make(chan struct{})

	// Trigger a final process when done signal is received
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

			// We only care about write events
			if event.Op&fsnotify.Write == fsnotify.Write {
				writeEventPending = true
			}

		case <-debounceC:
			// Process the file after the debounce period
			newOffset, errored := processFileBuffer(file, offset, buf, cmd)
			if errored {
				// If an error was found, we can exit immediately
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
			// Do a final check when terminating
			_, _ = processFileBuffer(file, offset, buf, cmd)
			return
		}
	}
}

// processFileBuffer reads and processes new file content using a pre-allocated buffer
func processFileBuffer(file *os.File, offset int64, buf []byte, cmd *exec.Cmd) (int64, bool) {
	// Seek to the last read position
	currentPos, err := file.Seek(offset, io.SeekStart)
	if err != nil {
		syslog.L.Error(err).WithMessage("seek error").Write()
		return offset, false
	}

	// Read new content directly into the buffer
	n, err := file.Read(buf)
	if err != nil && err != io.EOF {
		syslog.L.Error(err).WithMessage("read error").Write()
		return currentPos, false
	}

	// If no new content, return current position
	if n == 0 {
		return currentPos, false
	}

	// Check if the error pattern is in the new content
	if bytes.Contains(buf[:n], connectionFailedPattern) {
		if cmd.Process != nil {
			_ = cmd.Process.Kill()
		}
		return currentPos + int64(n), true
	}

	// Return the new offset
	return currentPos + int64(n), false
}
