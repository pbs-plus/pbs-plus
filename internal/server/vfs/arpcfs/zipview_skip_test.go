//go:build linux

package arpcfs

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"sync/atomic"
	"syscall"
	"testing"
)

func countingOverlay(t *testing.T, data []byte, reads *atomic.Int64) *zipOverlay {
	t.Helper()
	ov := testOverlay(t, data)
	src := readAtBytes(data)
	ov.readAt = func(ctx context.Context, p []byte, off int64) (int, error) {
		reads.Add(1)
		return src(ctx, p, off)
	}
	return ov
}

func compressible(size int) []byte {
	var b bytes.Buffer
	for i := 0; b.Len() < size; i++ {
		fmt.Fprintf(&b, "line %d of a fairly repetitive survey record\n", i)
	}
	return b.Bytes()[:size]
}

func TestZipPrecheckRejectsCorruptSmallEntry(t *testing.T) {
	data := buildTestZip(t, map[string]int{"deflate.txt": 4096})
	central := bytes.LastIndex(data, []byte("PK\x01\x02"))
	data[central+16] ^= 0xff

	fs := testFS(testOverlay(t, data))
	f, err := fs.OpenFile(context.Background(), "/data/deflate.txt", 0, 0)
	if err != nil {
		t.Fatal(err)
	}
	err = f.precheck(context.Background())
	if !errors.Is(err, errZipCorrupt) {
		t.Fatalf("precheck = %v, want %v", err, errZipCorrupt)
	}
	if !skippableReadErr(err) {
		t.Fatal("corrupt entry must be skippable")
	}
}

func TestZipPrecheckServesReadsFromRing(t *testing.T) {
	want := compressible(300 << 10)
	var reads atomic.Int64
	fs := testFS(countingOverlay(t, buildZipFiles(t, map[string][]byte{"a.txt": want}), &reads))
	f, err := fs.OpenFile(context.Background(), "/data/a.txt", 0, 0)
	if err != nil {
		t.Fatal(err)
	}
	if err := f.precheck(context.Background()); err != nil {
		t.Fatal(err)
	}
	before := reads.Load()
	got := make([]byte, len(want))
	if n, _ := f.ReadAt(context.Background(), got, 0); n != len(want) || !bytes.Equal(got, want) {
		t.Fatalf("read %d bytes, content match %v", n, bytes.Equal(got, want))
	}
	if after := reads.Load(); after != before {
		t.Fatalf("reads after precheck hit the source %d times", after-before)
	}
	if err := f.Close(context.Background()); err != nil {
		t.Fatal(err)
	}
}

func TestZipPrecheckSkipsLargeEntry(t *testing.T) {
	var reads atomic.Int64
	fs := testFS(countingOverlay(t, buildZipFiles(t, map[string][]byte{"big.txt": compressible(zipRingSize + 1)}), &reads))
	f, err := fs.OpenFile(context.Background(), "/data/big.txt", 0, 0)
	if err != nil {
		t.Fatal(err)
	}
	if err := f.precheck(context.Background()); err != nil {
		t.Fatal(err)
	}
	if n := reads.Load(); n != 0 {
		t.Fatalf("precheck read the source %d times for an entry larger than the ring", n)
	}
}

func TestFileHandleCutsCorruptEntryShort(t *testing.T) {
	content := compressible(3 << 20)
	data := buildZipFiles(t, map[string][]byte{"big.txt": content})
	ov := testOverlay(t, data)
	e := &ov.entries[ov.byName["big.txt"]]
	dataOff, err := ov.dataOffset(context.Background(), e)
	if err != nil {
		t.Fatal(err)
	}
	mid := dataOff + e.compSize/2
	for i := mid; i < mid+64; i++ {
		data[i] ^= 0x5a
	}

	fs := testFS(ov)
	f, err := fs.OpenFile(context.Background(), "/data/big.txt", 0, 0)
	if err != nil {
		t.Fatal(err)
	}
	fh := &FileHandle{fs: fs, file: f}
	buf := make([]byte, 128<<10)
	var off int64
	for off < int64(len(content)) {
		res, errno := fh.Read(context.Background(), buf, off)
		if errno != 0 {
			t.Fatalf("Read at %d: errno %v, want a short read", off, errno)
		}
		n := res.Size()
		off += int64(n)
		if n < len(buf) {
			break
		}
	}
	if off >= int64(len(content)) {
		t.Fatal("corrupt entry was read to the end without a cut")
	}
	if res, errno := fh.Read(context.Background(), buf, off); errno != 0 || res.Size() != 0 {
		t.Fatalf("read past cut: size %d errno %v, want EOF", res.Size(), errno)
	}
	if n := fs.skippedCount.Load(); n != 1 {
		t.Fatalf("skip warnings = %d, want 1", n)
	}
}

func TestFileHandleCountsOnlyForwardBytes(t *testing.T) {
	content := compressible(3 << 20)
	fs := testFS(testOverlay(t, buildZipFiles(t, map[string][]byte{"big.txt": content})))
	f, err := fs.OpenFile(context.Background(), "/data/big.txt", 0, 0)
	if err != nil {
		t.Fatal(err)
	}
	fh := &FileHandle{fs: fs, file: f}
	buf := make([]byte, 64<<10)
	read := func(off int64) {
		if _, errno := fh.Read(context.Background(), buf, off); errno != 0 {
			t.Fatalf("Read at %d: errno %v", off, errno)
		}
	}

	read(0)
	read(64 << 10)
	read(0)
	read(32 << 10)
	if got := fs.ForwardBytes.Load(); got != 128<<10 {
		t.Fatalf("forward bytes after re-reads = %d, want %d", got, 128<<10)
	}
	read(1 << 20)
	if got := fs.ForwardBytes.Load(); got != 1<<20+64<<10 {
		t.Fatalf("forward bytes after skip-ahead = %d, want %d", got, 1<<20+64<<10)
	}
}

func TestSkippableReadErrKeepsTransportFatal(t *testing.T) {
	for _, err := range []error{
		fmt.Errorf("%w: %w", ErrSessionLost, errors.New("pipe closed")),
		context.Canceled,
		fmt.Errorf("read: %w", context.DeadlineExceeded),
	} {
		if skippableReadErr(err) {
			t.Fatalf("%v must not be skippable", err)
		}
	}
	if !skippableReadErr(syscall.EIO) {
		t.Fatal("agent-side EIO must be skippable")
	}
}
