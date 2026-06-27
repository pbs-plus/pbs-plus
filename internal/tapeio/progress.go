package tapeio

import (
	"context"
	"fmt"
	"io"
	"sync/atomic"
	"time"

	"github.com/pbs-plus/pbs-plus/internal/log"
)

// progress is the live migration counter. Fields are atomic so the reporter
// goroutine can read them concurrently with the conversion loop.
type progress struct {
	startTime time.Time
	files     atomic.Int64
	dirs      atomic.Int64
	bytes     atomic.Int64
}

func newProgress() *progress {
	return &progress{startTime: time.Now()}
}

func (p *progress) snapshot() (files, dirs int, bytes int64) {
	return int(p.files.Load()), int(p.dirs.Load()), p.bytes.Load()
}

// report launches a goroutine that prints live throughput to w every interval
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
				// Suppress the line when nothing moved (first intervals spend time
				if cur == 0 || inst == 0 {
					continue
				}
				elapsed := now.Sub(p.startTime).Seconds()
				var avg float64
				if elapsed > 0 {
					avg = float64(cur) / 1e6 / elapsed
				}
				if _, err := fmt.Fprintf(w, "progress: %d files, %.1f MB | inst %.1f MB/s | avg %.1f MB/s | %s\n",
					p.files.Load(), float64(cur)/1e6, inst, avg, now.Sub(p.startTime).Round(time.Second)); err != nil {
					log.Error(err, "")
				}
			}
		}
	}()
	return func() { close(done) }
}
