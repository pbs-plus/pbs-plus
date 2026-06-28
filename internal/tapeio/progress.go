package tapeio

import (
	"context"
	"fmt"
	"io"
	"sync/atomic"
	"time"

	"github.com/pbs-plus/pbs-plus/internal/log"
)

type progress struct {
	startTime time.Time
	procStart atomic.Int64
	procEnd   atomic.Int64

	files         atomic.Int64
	dirs          atomic.Int64
	bytes         atomic.Int64
	tapeBytes     atomic.Int64
	tapePhysBytes atomic.Int64
}

func newProgress() *progress {
	return &progress{startTime: time.Now()}
}

func (p *progress) snapshot() (files, dirs int, bytes int64) {
	return int(p.files.Load()), int(p.dirs.Load()), p.bytes.Load()
}

func (p *progress) markProcessing() {
	p.procStart.CompareAndSwap(0, time.Now().UnixNano())
}

func (p *progress) markProcessingDone() {
	p.procEnd.Store(time.Now().UnixNano())
}

func (p *progress) procElapsed() float64 {
	start := p.procStart.Load()
	if start == 0 {
		return 0
	}
	end := p.procEnd.Load()
	if end == 0 {
		end = time.Now().UnixNano()
	}
	return float64(end-start) / 1e9
}

func (p *progress) summary(w io.Writer) {
	elapsed := p.procElapsed()
	cur := p.bytes.Load()
	curTape := p.tapeBytes.Load()
	curPhys := p.tapePhysBytes.Load()
	files := p.files.Load()
	dirs := p.dirs.Load()

	if elapsed <= 0 || cur == 0 {
		return
	}
	avg := float64(cur) / 1e6 / elapsed
	tapeAvg := float64(curTape) / 1e6 / elapsed
	physAvg := float64(curPhys) / 1e6 / elapsed
	fmt.Fprintf(w, "\nprocessing summary: %d files, %d dirs, %.1f MB in %s\n", files, dirs, float64(cur)/1e6, time.Duration(elapsed*1e9).Round(time.Second))
	fmt.Fprintf(w, "  average: ingest %.1f MB/s | tape %.1f MB/s | phys %.1f MB/s\n", avg, tapeAvg, physAvg)
}

func (p *progress) report(ctx context.Context, w io.Writer, interval time.Duration) (stop func()) {
	return p.reportWith(ctx, w, interval, nil)
}

func (p *progress) reportWith(ctx context.Context, w io.Writer, interval time.Duration, cb func(Progress)) (stop func()) {
	ticker := time.NewTicker(interval)
	done := make(chan struct{})
	go func() {
		var lastBytes, lastTape, lastPhys int64
		var lastFiles int64
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
				curTape := p.tapeBytes.Load()
				curPhys := p.tapePhysBytes.Load()
				curFiles := p.files.Load()
				dt := now.Sub(lastTime).Seconds()
				var inst, tapeInst, physInst float64
				var filesInst float64
				if dt > 0 {
					inst = float64(cur-lastBytes) / 1e6 / dt
					tapeInst = float64(curTape-lastTape) / 1e6 / dt
					physInst = float64(curPhys-lastPhys) / 1e6 / dt
					filesInst = float64(curFiles-lastFiles) / dt
				}
				lastBytes = cur
				lastTape = curTape
				lastPhys = curPhys
				lastFiles = curFiles
				lastTime = now
				procElapsed := p.procElapsed()
				var avg, tapeAvg, physAvg float64
				var filesAvg float64
				if procElapsed > 0 {
					avg = float64(cur) / 1e6 / procElapsed
					tapeAvg = float64(curTape) / 1e6 / procElapsed
					physAvg = float64(curPhys) / 1e6 / procElapsed
					filesAvg = float64(curFiles) / procElapsed
				}
				if _, err := fmt.Fprintf(w, "progress: %d files, %.1f MB | phys %.1f/%.1f MB/s | tape %.1f/%.1f MB/s | ingest %.1f/%.1f MB/s | %s\n",
					p.files.Load(), float64(cur)/1e6,
					physInst, physAvg, tapeInst, tapeAvg, inst, avg,
					now.Sub(p.startTime).Round(time.Second)); err != nil {
					log.Error(err, "")
				}
				if cb != nil {
					cb(Progress{
						Files:      p.files.Load(),
						Dirs:       p.dirs.Load(),
						Bytes:      cur,
						PhysInst:   physInst,
						PhysAvg:    physAvg,
						TapeInst:   tapeInst,
						TapeAvg:    tapeAvg,
						IngestInst: inst,
						IngestAvg:  avg,
						FilesInst:  filesInst,
						FilesAvg:   filesAvg,
					})
				}
			}
		}
	}()
	return func() {
		close(done)
		p.summary(w)
	}
}
