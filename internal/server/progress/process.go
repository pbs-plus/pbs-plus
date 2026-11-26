//go:build linux

package progress

import (
	"context"
	"fmt"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"
)

type processIO struct {
	read  uint64
	write uint64
}

func readProcessIO(pid int) (processIO, error) {
	data, err := os.ReadFile(fmt.Sprintf("/proc/%d/io", pid))
	if err != nil {
		return processIO{}, err
	}
	return parseProcessIO(string(data))
}

func parseProcessIO(data string) (processIO, error) {
	var result processIO
	found := 0
	for line := range strings.SplitSeq(data, "\n") {
		fields := strings.Fields(line)
		if len(fields) != 2 || (fields[0] != "rchar:" && fields[0] != "wchar:") {
			continue
		}
		value, err := strconv.ParseUint(fields[1], 10, 64)
		if err != nil {
			return processIO{}, err
		}
		if fields[0] == "rchar:" {
			result.read = value
		} else {
			result.write = value
		}
		found++
	}
	if found != 2 {
		return processIO{}, fmt.Errorf("process read/write counters are unavailable")
	}
	return result, nil
}

// Watch reports measured process I/O every 15 seconds; its stop function waits for logging to finish.
func Watch(ctx context.Context, pid int, phase string, write func(string)) func() {
	return watch(ctx, pid, phase, write, 15*time.Second)
}

func watch(ctx context.Context, pid int, phase string, write func(string), interval time.Duration) func() {
	if write == nil {
		return func() {}
	}
	stop := make(chan struct{})
	done := make(chan struct{})
	start := time.Now()
	previous, previousErr := readProcessIO(pid)
	lastSample := start
	lastIO := start
	if pid > 0 {
		write(phase + "; rates below measure process read/write calls (files and sockets), not mailbox payload or network throughput")
	} else {
		write(phase)
	}
	go func() {
		defer close(done)
		ticker := time.NewTicker(interval)
		defer ticker.Stop()
		for {
			select {
			case <-stop:
				return
			case <-ctx.Done():
				return
			case now := <-ticker.C:
				elapsed := now.Sub(start).Round(time.Second)
				if pid == 0 {
					write(fmt.Sprintf("%s; elapsed %s; still waiting for this stage to finish", phase, elapsed))
					continue
				}
				current, err := readProcessIO(pid)
				if err != nil || previousErr != nil {
					write(fmt.Sprintf("%s; elapsed %s; process I/O rates unavailable", phase, elapsed))
				} else {
					if current != previous {
						lastIO = now
					}
					write(processLine(phase, elapsed, current, previous, now.Sub(lastSample), now.Sub(lastIO)))
				}
				previous, previousErr, lastSample = current, err, now
			}
		}
	}()
	var once sync.Once
	return func() {
		once.Do(func() { close(stop) })
		<-done
	}
}

func processLine(phase string, elapsed time.Duration, current, previous processIO, interval, idle time.Duration) string {
	rate := func(current, previous uint64) float64 {
		if current < previous || interval <= 0 {
			return 0
		}
		return float64(current-previous) / interval.Seconds() / (1024 * 1024)
	}
	line := fmt.Sprintf("%s; elapsed %s; process read %.2f MiB (%.2f MiB/s), write %.2f MiB (%.2f MiB/s)",
		phase, elapsed, float64(current.read)/(1024*1024), rate(current.read, previous.read),
		float64(current.write)/(1024*1024), rate(current.write, previous.write))
	if idle > 0 {
		line += fmt.Sprintf("; no process read/write activity observed for %s", idle.Round(time.Second))
	}
	return line
}
