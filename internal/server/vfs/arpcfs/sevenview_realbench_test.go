//go:build linux

package arpcfs

import (
	"fmt"
	"math/rand"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
)

// build7zReal uses default compression (-mx5) and mixed compressible data.
func build7zReal(tb testing.TB, files int, size int, solid bool) []byte {
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
	rng := rand.New(rand.NewSource(42))
	for i := range files {
		name := filepath.Join(src, fmt.Sprintf("f%04d.bin", i))
		buf := make([]byte, size)
		for j := range buf {
			if j%2 == 0 {
				buf[j] = byte('a' + (j/251)%26)
			} else {
				buf[j] = byte(rng.Intn(256))
			}
		}
		if err := os.WriteFile(name, buf, 0o644); err != nil {
			tb.Fatal(err)
		}
	}
	out := filepath.Join(dir, "a.7z")
	args := []string{"a", "-bso0", "-bsp0"}
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
	tb.Logf("archive %d files x %d bytes: %d bytes", files, size, len(data))
	return data
}

func benchSevenReal(b *testing.B, solid, reverse bool, files, size int) {
	data := build7zReal(b, files, size, solid)
	ov := sevenOverlay(b, data)
	b.SetBytes(int64(len(ov.entries)) * int64(size))
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

func BenchmarkRealSolidFwd(b *testing.B) { benchSevenReal(b, true, false, 24, 256<<10) }
func BenchmarkRealSolidRev(b *testing.B) { benchSevenReal(b, true, true, 24, 256<<10) }
func BenchmarkRealNonSolidFwd(b *testing.B) {
	benchSevenReal(b, false, false, 24, 256<<10)
}
func BenchmarkRealNonSolidManySmall(b *testing.B) {
	benchSevenReal(b, false, false, 500, 4096)
}
func BenchmarkRealSolidManySmall(b *testing.B) {
	benchSevenReal(b, true, false, 500, 4096)
}
