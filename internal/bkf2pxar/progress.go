package bkf2pxar

import (
	"context"
	"fmt"
	"io"
	"sync/atomic"
	"time"
)

// progress is the live migration counter. Its fields are atomic so a reporter
// goroutine can read them concurrently with the single-threaded conversion
// loop writing them in processEntry. It is the single source of truth for
// files/dirs/bytes during a run; [Stats] is populated from it at the end.
type progress struct {
	startTime time.Time
	files     atomic.Int64
	dirs      atomic.Int64
	bytes     atomic.Int64
}

// newProgress starts a fresh counter anchored at now.
func newProgress() *progress {
	return &progress{startTime: time.Now()}
}

// snapshot loads the current counters into a plain-int triple for Stats.
func (p *progress) snapshot() (files, dirs int, bytes int64) {
	return int(p.files.Load()), int(p.dirs.Load()), p.bytes.Load()
}

// report launches a goroutine that prints live throughput to w every interval
// until the returned stop function is called. It is the native-rate counter:
// it shows the actual sustained MB/s of the read pipeline (tape -> pxar -> PBS)
// so a streaming run surfaces the drive's native speed.
//
// The instantaneous rate is measured over each interval; the average rate is
// measured since start. Bytes are SI (1 MB = 1e6 B) to match tape native-rate
// specifications (LTO-6 = 160 MB/s, LTO-8 = 360 MB/s compressed / native).
func (p *progress) report(ctx context.Context, w io.Writer, interval time.Duration) (stop func()) {
	ticker := time.NewTicker(interval)
	done := make(chan struct{})
	go func() {
		var lastBytes int64
		lastTime := time.Now()
		for {
			select {
			case <-done:
				ticker.Stop()
				return
			case <-ctx.Done():
				ticker.Stop()
				return
			case now := <-ticker.C:
				cur := p.bytes.Load()
				dt := now.Sub(lastTime).Seconds()
				var inst float64
				if dt > 0 {
					inst = float64(cur-lastBytes) / 1e6 / dt
				}
				lastBytes = cur
				lastTime = now
				// Suppress the line when nothing moved: the first interval(s)
				// are spent rewinding, validating, and buffering the first
				// file — printing 0.0 MB/s is misleading.
				if cur == 0 || inst == 0 {
					continue
				}
				elapsed := now.Sub(p.startTime).Seconds()
				var avg float64
				if elapsed > 0 {
					avg = float64(cur) / 1e6 / elapsed
				}
				_, _ = fmt.Fprintf(w, "progress: %d files, %.1f MB | inst %.1f MB/s | avg %.1f MB/s | %s\n",
					p.files.Load(), float64(cur)/1e6, inst, avg, now.Sub(p.startTime).Round(time.Second))
			}
		}
	}()
	return func() { close(done) }
}
