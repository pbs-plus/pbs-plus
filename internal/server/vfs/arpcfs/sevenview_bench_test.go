//go:build linux

package arpcfs

import (
	"context"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
)

// build7z shells out because bodgit/sevenzip is read-only, no writer exists.
func build7z(tb testing.TB, files int, size int, solid bool) []byte {
	tb.Helper()
	bin, err := exec.LookPath("7z")
	if err != nil {
		tb.Skip("7z not installed")
	}
	dir := tb.TempDir()
	src := filepath.Join(dir, "src")
	if err := os.MkdirAll(src, 0o755); err != nil {
		tb.Fatal(err)
	}
	buf := make([]byte, size)
	for i := range buf {
		buf[i] = byte(i*7 + i/251)
	}
	for i := range files {
		name := filepath.Join(src, fmt.Sprintf("f%03d.bin", i))
		if err := os.WriteFile(name, buf, 0o644); err != nil {
			tb.Fatal(err)
		}
	}
	out := filepath.Join(dir, "a.7z")
	args := []string{"a", "-bso0", "-bsp0", "-mx1"}
	if !solid {
		args = append(args, "-ms=off")
	}
	args = append(args, out, src+"/.")
	if err := exec.Command(bin, args...).Run(); err != nil {
		tb.Fatal(err)
	}
	data, err := os.ReadFile(out)
	if err != nil {
		tb.Fatal(err)
	}
	return data
}

func sevenOverlay(tb testing.TB, data []byte) *zipOverlay {
	tb.Helper()
	ov, err := parseArchiveOverlay(readAtBytes(data), int64(len(data)), zipMaxEntries)
	if err != nil {
		tb.Fatal(err)
	}
	return ov
}

func readAll(tb testing.TB, ov *zipOverlay, idx int32, buf []byte) {
	tb.Helper()
	ent := &ov.entries[idx]
	zs := &zipFileState{ov: ov, ent: ent, uncomp: ent.uncompSize}
	defer zs.close()
	for off := int64(0); off < ent.uncompSize; {
		n, err := zs.ReadAt(context.Background(), buf, off)
		off += int64(n)
		if err == io.EOF {
			break
		}
		if err != nil {
			tb.Fatal(err)
		}
	}
}

func benchSeven(b *testing.B, solid, reverse bool) {
	data := build7z(b, 24, 256<<10, solid)
	ov := sevenOverlay(b, data)
	b.SetBytes(int64(len(ov.entries)) * 256 << 10)
	b.ReportAllocs()
	buf := make([]byte, 128<<10)
	for b.Loop() {
		for i := range int32(len(ov.entries)) {
			if reverse {
				i = int32(len(ov.entries)) - 1 - i
			}
			readAll(b, ov, i, buf)
		}
	}
}

func BenchmarkSevenSolidForward(b *testing.B)    { benchSeven(b, true, false) }
func BenchmarkSevenSolidReverse(b *testing.B)    { benchSeven(b, true, true) }
func BenchmarkSevenNonSolidForward(b *testing.B) { benchSeven(b, false, false) }
