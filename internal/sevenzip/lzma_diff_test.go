package sevenzip

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"fmt"
	"io"
	"math/rand"
	"os"
	"os/exec"
	"path/filepath"
	"testing"

	"github.com/pbs-plus/pbs-plus/internal/sevenzip/internal/lzma"
	ulzma "github.com/ulikunitz/xz/lzma"
)

func makePayload(kind string, size int, seed int64) []byte {
	rng := rand.New(rand.NewSource(seed))
	buf := make([]byte, size)
	switch kind {
	case "text":
		words := []byte("the quick brown fox jumps over the lazy dog 0123456789\n")
		for i := range buf {
			buf[i] = words[rng.Intn(len(words))]
		}
	case "random":
		rng.Read(buf)
	case "mixed":
		for i := range buf {
			if i%2 == 0 {
				buf[i] = byte('a' + rng.Intn(26))
			} else {
				buf[i] = byte(rng.Intn(256))
			}
		}
	case "runs":
		i := 0
		for i < len(buf) {
			n := 1 + rng.Intn(300)
			b := byte(rng.Intn(256))
			for j := 0; j < n && i < len(buf); j++ {
				buf[i] = b
				i++
			}
		}
	}
	return buf
}

func xzCompress(t *testing.T, payload []byte, opts string) []byte {
	t.Helper()
	dir := t.TempDir()
	src := filepath.Join(dir, "in")
	if err := os.WriteFile(src, payload, 0o644); err != nil {
		t.Fatal(err)
	}
	out := filepath.Join(dir, "out.lzma")
	cmd := exec.Command("xz", "--format=lzma", opts, "-k", "-c", src)
	f, err := os.Create(out)
	if err != nil {
		t.Fatal(err)
	}
	cmd.Stdout = f
	if err := cmd.Run(); err != nil {
		t.Skipf("xz failed: %v", err)
	}
	f.Close()
	data, err := os.ReadFile(out)
	if err != nil {
		t.Fatal(err)
	}
	return data
}

// TestLZMA1Differential decodes the same .lzma stream through the fused core
// and through ulikunitz, requiring byte-identical output.
func TestLZMA1Differential(t *testing.T) {
	cases := []struct {
		kind string
		size int
		opts string
	}{
		{"text", 256 << 10, "-1"},
		{"text", 4 << 20, "-1"},
		{"random", 512 << 10, "-0"},
		{"mixed", 2 << 20, "-6"},
		{"mixed", 256 << 10, "--lzma1=preset=1,lc=0,lp=2,pb=0"},
		{"mixed", 256 << 10, "--lzma1=preset=1,lc=4,lp=0,pb=1"},
		{"runs", 1 << 20, "-2"},
		{"text", 700 << 10, "-9e"},
	}
	for _, tc := range cases {
		t.Run(fmt.Sprintf("%s/%d/%s", tc.kind, tc.size, tc.opts), func(t *testing.T) {
			payload := makePayload(tc.kind, tc.size, 42)
			stream := xzCompress(t, payload, tc.opts)

			ur, err := ulzma.NewReader(bytes.NewReader(stream))
			if err != nil {
				t.Fatal(err)
			}
			want, err := io.ReadAll(ur)
			if err != nil {
				t.Fatalf("ulikunitz decode: %v", err)
			}
			if !bytes.Equal(want, payload) {
				t.Fatal("ulikunitz output differs from payload; fixture invalid")
			}

			props := stream[:5]
			size := binary.LittleEndian.Uint64(stream[5:13])
			fr, err := lzma.NewReader(props, size, []io.ReadCloser{io.NopCloser(bytes.NewReader(stream[13:]))})
			if err != nil {
				t.Fatal(err)
			}
			got, err := io.ReadAll(fr)
			if err != nil {
				t.Fatalf("fused decode: %v", err)
			}
			if !bytes.Equal(got, want) {
				t.Fatalf("fused output differs: got %d bytes sha %x, want %d bytes sha %x",
					len(got), sha256.Sum256(got), len(want), sha256.Sum256(want))
			}
		})
	}
}

// TestSevenZipEndToEnd compares archive extraction against the 7z CLI.
func TestSevenZipEndToEnd(t *testing.T) {
	if _, err := exec.LookPath("7z"); err != nil {
		t.Skip("7z not installed")
	}
	for _, method := range []string{"-m0=lzma2", "-m0=lzma"} {
		for _, tc := range []struct {
			kind string
			size int
		}{
			{"mixed", 3 << 20},
			{"text", 3 << 20},
		} {
			t.Run(fmt.Sprintf("%s/%s/%d", method, tc.kind, tc.size), func(t *testing.T) {
				payload := makePayload(tc.kind, tc.size, 7)
				dir := t.TempDir()
				src := filepath.Join(dir, "f.bin")
				if err := os.WriteFile(src, payload, 0o644); err != nil {
					t.Fatal(err)
				}
				out := filepath.Join(dir, "a.7z")
				cmd := exec.Command("7z", "a", "-bso0", "-bsp0", method, out, src)
				if err := cmd.Run(); err != nil {
					t.Fatal(err)
				}
				data, err := os.ReadFile(out)
				if err != nil {
					t.Fatal(err)
				}
				zr, err := NewReader(bytes.NewReader(data), int64(len(data)))
				if err != nil {
					t.Fatal(err)
				}
				if len(zr.File) != 1 {
					t.Fatalf("expected 1 file, got %d", len(zr.File))
				}
				rc, err := zr.File[0].Open()
				if err != nil {
					t.Fatal(err)
				}
				got, err := io.ReadAll(rc)
				rc.Close()
				if err != nil {
					t.Fatalf("decode: %v", err)
				}
				if !bytes.Equal(got, payload) {
					t.Fatalf("extraction mismatch for %s %s: got %d bytes, want %d",
						method, tc.kind, len(got), len(payload))
				}
			})
		}
	}
}

// TestSevenZipMultiFileSolid exercises folder pooling with many files.
func TestSevenZipMultiFileSolid(t *testing.T) {
	if _, err := exec.LookPath("7z"); err != nil {
		t.Skip("7z not installed")
	}
	dir := t.TempDir()
	src := filepath.Join(dir, "src")
	if err := os.MkdirAll(src, 0o755); err != nil {
		t.Fatal(err)
	}
	var want [][]byte
	for i := range 60 {
		p := makePayload("mixed", 32<<10, int64(i+1))
		want = append(want, p)
		if err := os.WriteFile(filepath.Join(src, fmt.Sprintf("f%03d.bin", i)), p, 0o644); err != nil {
			t.Fatal(err)
		}
	}
	out := filepath.Join(dir, "a.7z")
	if err := exec.Command("7z", "a", "-bso0", "-bsp0", out, src+"/.").Run(); err != nil {
		t.Fatal(err)
	}
	data, err := os.ReadFile(out)
	if err != nil {
		t.Fatal(err)
	}
	zr, err := NewReader(bytes.NewReader(data), int64(len(data)))
	if err != nil {
		t.Fatal(err)
	}
	if len(zr.File) != 60 {
		t.Fatalf("expected 60 files, got %d", len(zr.File))
	}
	for i, f := range zr.File {
		rc, err := f.Open()
		if err != nil {
			t.Fatalf("file %d: %v", i, err)
		}
		got, err := io.ReadAll(rc)
		rc.Close()
		if err != nil {
			t.Fatalf("file %d decode: %v", i, err)
		}
		if !bytes.Equal(got, want[i]) {
			t.Fatalf("file %d mismatch: %d vs %d bytes", i, len(got), len(want[i]))
		}
	}
}
