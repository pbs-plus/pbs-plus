package tapeio

import (
	"context"
	"runtime"
	"testing"
	"time"

	mtf "github.com/pbs-plus/go-mtf"
)

func TestRunPipelineNoGoroutineLeak(t *testing.T) {
	before := runtime.NumGoroutine()

	const iterations = 100
	for range iterations {
		c := &converter{
			cfg:         Config{SpoolCapBytes: 1 << 20},
			ctx:         context.Background(),
			prog:        newProgress(),
			snapshotIdx: -1,
		}
		r := mtf.NewReader(mtf.NewSliceTape(nil))
		_ = c.processReader(r)
	}

	for range 4 {
		runtime.GC()
		time.Sleep(20 * time.Millisecond)
	}

	after := runtime.NumGoroutine()
	leaked := after - before
	if leaked > iterations/10 {
		t.Fatalf("goroutine leak: %d goroutines still alive after %d processReader calls (before=%d after=%d)",
			leaked, iterations, before, after)
	}
}
