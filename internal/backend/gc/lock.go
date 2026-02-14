//go:build linux

package gc

import (
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"

	"golang.org/x/sys/unix"
)

func AcquireProxmoxSharedLock(datastorePath string) (*os.File, error) {
	lockPath := filepath.Join(datastorePath, ".lock")
	f, err := os.OpenFile(lockPath, os.O_RDWR, 0644)
	if err != nil {
		return nil, err
	}

	// PBS ProcessLocker uses:
	// Byte 0: Exclusive lock (GC)
	// Bytes 1-2048: Shared locks (Backup/Sync)
	lk := &unix.Flock_t{
		Type:   unix.F_RDLCK, // Shared
		Whence: io.SeekStart,
		Start:  0,
		Len:    1,
	}

	// F_SETLK (non-blocking). If GC is running elsewhere, this fails.
	err = unix.FcntlFlock(f.Fd(), unix.F_SETLK, lk)
	if err != nil {
		f.Close()
		return nil, fmt.Errorf("failed to acquire shared datastore lock (GC might be running): %w", err)
	}

	return f, nil
}

// GetOldestWriter mimics Rust's ProcessLocker::oldest_shared_lock.
// It returns the Unix epoch of the start time of the oldest process holding a shared lock.
func GetOldestWriter(datastorePath string) (int64, error) {
	lockPath := filepath.Join(datastorePath, ".lock")
	f, err := os.Open(lockPath)
	if err != nil {
		return 0, err
	}
	defer f.Close()

	var oldestStartTime int64 = 0

	// PBS shared locks are in the range 1..2048
	for i := int64(1); i <= 2048; i++ {
		// Prepare the fcntl query
		lk := unix.Flock_t{
			Type:   unix.F_WRLCK, // Query for who would block a Write Lock
			Whence: io.SeekStart,
			Start:  i,
			Len:    1,
		}

		// F_GETLK returns the PID of the process holding a conflicting lock
		if err := unix.FcntlFlock(f.Fd(), unix.F_GETLK, &lk); err != nil {
			continue
		}

		// Type becomes F_UNLCK if no lock is found
		if lk.Type == unix.F_UNLCK || lk.Pid == 0 {
			continue
		}

		// Get the start time of this PID
		startTime, err := getProcessStartTime(int(lk.Pid))
		if err != nil {
			continue // Process might have exited
		}

		if oldestStartTime == 0 || startTime < oldestStartTime {
			oldestStartTime = startTime
		}
	}

	return oldestStartTime, nil
}

// getProcessStartTime reads /proc/<pid>/stat to get the process start time in Unix epoch.
func getProcessStartTime(pid int) (int64, error) {
	data, err := os.ReadFile(fmt.Sprintf("/proc/%d/stat", pid))
	if err != nil {
		return 0, err
	}

	// The stat format is: pid (comm) state ppid pgrp session tty_nr tpgid flags ...
	// The starttime is the 22nd field.
	fields := strings.Fields(string(data))
	if len(fields) < 22 {
		return 0, fmt.Errorf("invalid stat format")
	}

	// Field 21 (index 21) is start_time in clock ticks since boot
	var ticks int64
	fmt.Sscanf(fields[21], "%d", &ticks)

	// To convert ticks to Unix epoch:
	// 1. Get boot time from /proc/stat (btime)
	// 2. Add (ticks / USER_HZ)
	btime, err := getBootTime()
	if err != nil {
		return 0, err
	}

	// USER_HZ is almost always 100 on Linux
	return btime + (ticks / 100), nil
}

func getBootTime() (int64, error) {
	data, err := os.ReadFile("/proc/stat")
	if err != nil {
		return 0, err
	}
	for _, line := range strings.Split(string(data), "\n") {
		if strings.HasPrefix(line, "btime ") {
			var btime int64
			fmt.Sscanf(line, "btime %d", &btime)
			return btime, nil
		}
	}
	return 0, fmt.Errorf("btime not found")
}
