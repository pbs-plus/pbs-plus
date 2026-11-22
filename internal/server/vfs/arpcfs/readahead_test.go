//go:build linux

package arpcfs

import (
	"bytes"
	"context"
	"errors"
	"math/rand/v2"
	"runtime"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"github.com/hanwen/go-fuse/v2/fuse"
	"github.com/pbs-plus/pbs-plus/internal/agent/agentfs/fswire"
	"github.com/pbs-plus/pbs-plus/internal/server/vfs"
	"go.uber.org/goleak"
)

// kernelReadSize is the largest FUSE read seen on production mounts (max_read=131072).
const kernelReadSize = 128 << 10

type fakeAgent struct {
	data     []byte
	latency  time.Duration
	failAt   int64
	hangFrom int64
	unhang   chan struct{}

	calls       atomic.Int64
	inflight    atomic.Int64
	maxInflight atomic.Int64
	closed      atomic.Bool
	afterClose  atomic.Int64
}

func (a *fakeAgent) readAt(ctx context.Context, _ fswire.FileHandleID, p []byte, off int64) (int, error) {
	if a.closed.Load() {
		a.afterClose.Add(1)
		return 0, errors.New("read after close")
	}
	a.calls.Add(1)
	cur := a.inflight.Add(1)
	defer a.inflight.Add(-1)
	if a.hangFrom > 0 && off >= a.hangFrom {
		select {
		case <-ctx.Done():
			return 0, ctx.Err()
		case <-a.unhang:
		}
	}
	for {
		m := a.maxInflight.Load()
		if cur <= m || a.maxInflight.CompareAndSwap(m, cur) {
			break
		}
	}
	time.Sleep(a.latency)
	if a.failAt > 0 && off <= a.failAt && a.failAt < off+int64(len(p)) {
		return 0, syscall.EIO
	}
	if off >= int64(len(a.data)) {
		return 0, nil
	}
	return copy(p, a.data[off:]), nil
}

func (a *fakeAgent) close(context.Context, fswire.FileHandleID) error {
	a.closed.Store(true)
	return nil
}

func newTestHandle(t *testing.T, a *fakeAgent) *FileHandle {
	t.Helper()
	fs := &ARPCFS{VFSBase: vfs.InjectBase(vfs.VFSBase{Ctx: t.Context()})}
	fs.remoteReadAt = a.readAt
	fs.remoteClose = a.close
	return newFileHandle(fs, &ARPCFile{fs: fs, name: "/file", handleID: 1})
}

func testData(size int) []byte {
	b := make([]byte, size)
	r := rand.New(rand.NewPCG(1, uint64(size)))
	for i := range b {
		b[i] = byte(r.Uint32())
	}
	return b
}

type readResult struct {
	data  []byte
	errno syscall.Errno
}

func readVia(t *testing.T, fh *FileHandle, off int64, size int) readResult {
	t.Helper()
	res, errno := fh.Read(t.Context(), make([]byte, size), off)
	if errno != 0 {
		return readResult{errno: errno}
	}
	buf, status := res.Bytes(make([]byte, size))
	if status != fuse.OK {
		t.Fatalf("read result status %v", status)
	}
	return readResult{data: bytes.Clone(buf)}
}

// readSequential reads like the kernel does for a backup: one kernelReadSize request at a time.
func readSequential(t *testing.T, fh *FileHandle, size int) []byte {
	t.Helper()
	var out []byte
	for off := int64(0); ; {
		r := readVia(t, fh, off, kernelReadSize)
		if r.errno != 0 {
			t.Fatalf("read at %d: %v", off, r.errno)
		}
		out = append(out, r.data...)
		off += int64(len(r.data))
		if len(r.data) < kernelReadSize {
			return out
		}
	}
}

// Without readahead every kernel read is one agent round trip, so a stream
// moves at most kernelReadSize per round trip.
func TestDirectReadsCostOneRoundTripEach(t *testing.T) {
	const size = 8 << 20
	const latency = 2 * time.Millisecond
	a := &fakeAgent{data: testData(size), latency: latency}
	fh := newTestHandle(t, a)
	fh.ra.file = nil

	start := time.Now()
	got := readSequential(t, fh, size)
	elapsed := time.Since(start)
	if !bytes.Equal(got, a.data) {
		t.Fatal("read data does not match the source")
	}
	t.Logf("%d agent calls, %d max in flight, %v elapsed, %.1f MiB/s",
		a.calls.Load(), a.maxInflight.Load(), elapsed, float64(size)/(1<<20)/elapsed.Seconds())
	if c, want := a.calls.Load(), int64(size/kernelReadSize+1); c != want {
		t.Errorf("agent calls = %d, want %d", c, want)
	}
	if m := a.maxInflight.Load(); m != 1 {
		t.Errorf("max agent reads in flight = %d, want 1", m)
	}
	if floor := time.Duration(size/kernelReadSize) * latency; elapsed < floor {
		t.Errorf("elapsed %v is below the serial floor %v", elapsed, floor)
	}
}

func TestSequentialReadsOverlapAgentRoundTrips(t *testing.T) {
	const size = 32 << 20
	const latency = 4 * time.Millisecond
	a := &fakeAgent{data: testData(size), latency: latency}
	fh := newTestHandle(t, a)

	start := time.Now()
	got := readSequential(t, fh, size)
	elapsed := time.Since(start)
	if err := fh.Release(t.Context()); err != 0 {
		t.Fatalf("release: %v", err)
	}

	if !bytes.Equal(got, a.data) {
		t.Fatal("read data does not match the source")
	}
	serial := time.Duration(size/kernelReadSize) * latency
	t.Logf("%d agent calls, %d max in flight, %v elapsed, %.1f MiB/s (serial floor %v)",
		a.calls.Load(), a.maxInflight.Load(), elapsed, float64(size)/(1<<20)/elapsed.Seconds(), serial)
	if c := a.calls.Load(); c > size/kernelReadSize/4 {
		t.Errorf("agent calls = %d, want at most %d: each kernel read still costs its own round trip", c, size/kernelReadSize/4)
	}
	if m := a.maxInflight.Load(); m < 4 {
		t.Errorf("max agent reads in flight = %d, want at least 4: round trips are not overlapped", m)
	}
	if elapsed >= serial {
		t.Errorf("sequential read took %v, want under the serial floor %v", elapsed, serial)
	}
}

func TestReadaheadToleratesKernelReorder(t *testing.T) {
	const size = 32 << 20
	a := &fakeAgent{data: testData(size), latency: 500 * time.Microsecond}
	fh := newTestHandle(t, a)
	r := rand.New(rand.NewPCG(3, 3))
	for off := int64(0); off < size; off += 2 * kernelReadSize {
		pair := [2]int64{off, off + kernelReadSize}
		if r.IntN(2) == 0 {
			pair[0], pair[1] = pair[1], pair[0]
		}
		for _, o := range pair {
			got := readVia(t, fh, o, kernelReadSize)
			if want := expected(a.data, o, kernelReadSize); got.errno != 0 || !bytes.Equal(got.data, want) {
				t.Fatalf("read at %d: %d bytes errno %v, want %d bytes", o, len(got.data), got.errno, len(want))
			}
		}
	}
	if err := fh.Release(t.Context()); err != 0 {
		t.Fatalf("release: %v", err)
	}
	t.Logf("%d agent calls for %d kernel reads", a.calls.Load(), size/kernelReadSize)
	if c := a.calls.Load(); c > size/kernelReadSize/4 {
		t.Errorf("agent calls = %d: reordered pairs keep resetting readahead", c)
	}
}

func TestReadaheadMatchesDirectReads(t *testing.T) {
	const block = readaheadBlock
	sizes := []int{0, 1, kernelReadSize, block - 1, block, block + 1, 3*block + 12345}
	for _, size := range sizes {
		data := testData(size)
		plans := map[string][][2]int64{
			"sequential 4k":   seqPlan(size, 4<<10),
			"sequential 128k": seqPlan(size, kernelReadSize),
			"sequential odd":  seqPlan(size, block+7),
			"random":          randomPlan(size, 64),
			"restart":         append(seqPlan(size, kernelReadSize), seqPlan(size, kernelReadSize)...),
			"backward":        reversePlan(seqPlan(size, kernelReadSize)),
		}
		for name, plan := range plans {
			a := &fakeAgent{data: data}
			fh := newTestHandle(t, a)
			for _, step := range plan {
				off, n := step[0], int(step[1])
				got := readVia(t, fh, off, n)
				want := expected(data, off, n)
				if got.errno != 0 || !bytes.Equal(got.data, want) {
					t.Fatalf("size %d %s: read %d@%d returned %d bytes errno %v, want %d bytes",
						size, name, n, off, len(got.data), got.errno, len(want))
				}
			}
			if err := fh.Release(t.Context()); err != 0 {
				t.Fatalf("release: %v", err)
			}
		}
	}
}

func TestReadaheadConcurrentReaders(t *testing.T) {
	const size = 6<<20 + 333
	a := &fakeAgent{data: testData(size), latency: 200 * time.Microsecond}
	fh := newTestHandle(t, a)
	var wg sync.WaitGroup
	for w := range 4 {
		wg.Go(func() {
			for off := int64(w * kernelReadSize); off < size; off += 4 * kernelReadSize {
				got := readVia(t, fh, off, kernelReadSize)
				if want := expected(a.data, off, kernelReadSize); got.errno != 0 || !bytes.Equal(got.data, want) {
					t.Errorf("read at %d: %d bytes errno %v, want %d bytes", off, len(got.data), got.errno, len(want))
					return
				}
			}
		})
	}
	wg.Wait()
	if err := fh.Release(t.Context()); err != 0 {
		t.Fatalf("release: %v", err)
	}
}

// Readahead must not change which kernel read reports a failed agent read.
func TestReadaheadKeepsReadErrors(t *testing.T) {
	const size = 5 << 20
	failAt := int64(3<<20 + 5000)
	data := testData(size)
	run := func(readahead bool) []readResult {
		a := &fakeAgent{data: data, failAt: failAt}
		fh := newTestHandle(t, a)
		if !readahead {
			fh.ra.file = nil
		}
		var out []readResult
		for off := int64(0); off < size; off += kernelReadSize {
			out = append(out, readVia(t, fh, off, kernelReadSize))
		}
		if err := fh.Release(t.Context()); err != 0 {
			t.Fatalf("release: %v", err)
		}
		return out
	}
	direct, ahead := run(false), run(true)
	if len(direct) != len(ahead) {
		t.Fatalf("%d direct results, %d readahead results", len(direct), len(ahead))
	}
	for i := range direct {
		if direct[i].errno != ahead[i].errno || !bytes.Equal(direct[i].data, ahead[i].data) {
			t.Fatalf("read %d: direct returned %d bytes errno %v, readahead %d bytes errno %v",
				i, len(direct[i].data), direct[i].errno, len(ahead[i].data), ahead[i].errno)
		}
	}
}

func TestNoReadaheadAfterRelease(t *testing.T) {
	defer goleak.VerifyNone(t, goleak.IgnoreCurrent())
	const size = 16 << 20
	a := &fakeAgent{data: testData(size), latency: 3 * time.Millisecond}
	fh := newTestHandle(t, a)
	for off := int64(0); off < 12*kernelReadSize; off += kernelReadSize {
		readVia(t, fh, off, kernelReadSize)
	}
	if err := fh.Release(t.Context()); err != 0 {
		t.Fatalf("release: %v", err)
	}
	if !a.closed.Load() {
		t.Fatal("release did not close the agent handle")
	}
	waitFor(t, func() bool { return a.inflight.Load() == 0 }, "agent reads never drained")
	calls := a.calls.Load()
	time.Sleep(30 * time.Millisecond)
	if c := a.calls.Load(); c != calls {
		t.Fatalf("agent calls grew from %d to %d after release", calls, c)
	}
	if n := a.afterClose.Load(); n > readaheadWindow {
		t.Fatalf("%d agent reads after close, more than one window", n)
	}
}

func TestReleaseDoesNotBlockOnHungAgent(t *testing.T) {
	defer goleak.VerifyNone(t, goleak.IgnoreCurrent())
	old := readaheadTimeout
	readaheadTimeout = 150 * time.Millisecond
	defer func() { readaheadTimeout = old }()

	const size = 8 << 20
	a := &fakeAgent{data: testData(size), hangFrom: readaheadBlock, unhang: make(chan struct{})}
	defer close(a.unhang)
	fh := newTestHandle(t, a)
	for off := int64(0); off < readaheadBlock; off += kernelReadSize {
		if got := readVia(t, fh, off, kernelReadSize); got.errno != 0 {
			t.Fatalf("read at %d: %v", off, got.errno)
		}
	}
	waitFor(t, func() bool { return a.inflight.Load() > 0 }, "no prefetch reached the hung range")

	start := time.Now()
	if err := fh.Release(t.Context()); err != 0 {
		t.Fatalf("release: %v", err)
	}
	if d := time.Since(start); d > 50*time.Millisecond {
		t.Fatalf("release blocked %v on a hung prefetch", d)
	}
	waitFor(t, func() bool { return a.inflight.Load() == 0 }, "hung prefetch outlived its deadline")
	if fh.fs.sessionDown.Load() {
		t.Fatal("a timed-out prefetch marked the session lost")
	}
}

func TestUnreleasedHandleIsCollectable(t *testing.T) {
	const size = 4 << 20
	a := &fakeAgent{data: testData(size)}
	collected := make(chan struct{})
	func() {
		fh := newTestHandle(t, a)
		runtime.AddCleanup(fh, func(ch chan struct{}) { close(ch) }, collected)
		readSequential(t, fh, size)
	}()
	waitFor(t, func() bool { return a.inflight.Load() == 0 }, "agent reads never drained")
	deadline := time.Now().Add(5 * time.Second)
	for {
		runtime.GC()
		select {
		case <-collected:
			return
		default:
		}
		if time.Now().After(deadline) {
			t.Fatal("a file handle with readahead state was never garbage collected")
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func waitFor(t *testing.T, cond func() bool, msg string) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatal(msg)
		}
		time.Sleep(5 * time.Millisecond)
	}
}

func TestReadaheadSmallFilesDoNotOverfetch(t *testing.T) {
	for _, size := range []int{2 * kernelReadSize, readaheadBlock + readaheadBlock/2, 3*readaheadBlock + 1} {
		calls := func(readahead bool) int64 {
			a := &fakeAgent{data: testData(size)}
			fh := newTestHandle(t, a)
			if !readahead {
				fh.ra.file = nil
			}
			if got := readSequential(t, fh, size); !bytes.Equal(got, a.data) {
				t.Fatalf("size %d: read data does not match the source", size)
			}
			if err := fh.Release(t.Context()); err != 0 {
				t.Fatalf("release: %v", err)
			}
			return a.calls.Load()
		}
		direct, ahead := calls(false), calls(true)
		t.Logf("size %d: %d direct calls, %d with readahead", size, direct, ahead)
		if ahead > direct {
			t.Errorf("size %d: readahead made %d agent calls, direct reads need %d", size, ahead, direct)
		}
	}
}

// TestReadaheadReusesBlockBuffers fails if each 1 MiB block is freshly allocated.
func TestReadaheadReusesBlockBuffers(t *testing.T) {
	const size = 64 << 20
	a := &fakeAgent{data: testData(size)}
	fh := newTestHandle(t, a)
	dest := make([]byte, kernelReadSize)
	pass := func() {
		for off := int64(0); off < size; off += kernelReadSize {
			if _, errno := fh.Read(t.Context(), dest, off); errno != 0 {
				t.Fatalf("read at %d: %v", off, errno)
			}
		}
	}
	pass()
	var before, after runtime.MemStats
	runtime.ReadMemStats(&before)
	pass()
	runtime.ReadMemStats(&after)
	if err := fh.Release(t.Context()); err != 0 {
		t.Fatalf("release: %v", err)
	}
	alloc := after.TotalAlloc - before.TotalAlloc
	t.Logf("%d KiB allocated reading %d MiB, %d mallocs", alloc>>10, size>>20, after.Mallocs-before.Mallocs)
	if alloc > size/4 {
		t.Errorf("allocated %d bytes reading %d bytes; block buffers are not reused", alloc, size)
	}
}

func expected(data []byte, off int64, n int) []byte {
	if off >= int64(len(data)) {
		return []byte{}
	}
	return data[off:min(int64(len(data)), off+int64(n))]
}

func seqPlan(size, step int) [][2]int64 {
	var plan [][2]int64
	for off := 0; off <= size; off += step {
		plan = append(plan, [2]int64{int64(off), int64(step)})
	}
	return plan
}

func randomPlan(size, count int) [][2]int64 {
	r := rand.New(rand.NewPCG(7, uint64(size)))
	plan := make([][2]int64, count)
	for i := range plan {
		plan[i] = [2]int64{r.Int64N(int64(size) + 1), int64(1 + r.IntN(2*kernelReadSize))}
	}
	return plan
}

func reversePlan(plan [][2]int64) [][2]int64 {
	out := make([][2]int64, len(plan))
	for i, s := range plan {
		out[len(plan)-1-i] = s
	}
	return out
}
