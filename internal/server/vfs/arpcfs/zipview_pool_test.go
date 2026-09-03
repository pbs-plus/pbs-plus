//go:build linux

package arpcfs

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"sync"
	"testing"
)

func TestZipRingPoolNoCrossTalk(t *testing.T) {
	const files, size = 6, 300 << 10
	want := map[string][]byte{}
	for i := range files {
		b := make([]byte, size)
		for j := range b {
			b[j] = byte(i*31 + j/1024)
		}
		want[fmt.Sprintf("f%d.bin", i)] = b
	}
	ov := testOverlay(t, buildZipFiles(t, want))

	var wg sync.WaitGroup
	for g := range 8 {
		wg.Go(func() {
			for n := range 12 {
				name := fmt.Sprintf("f%d.bin", (g+n)%files)
				idx, ok := ov.byName[name]
				if !ok {
					t.Errorf("missing %s", name)
					return
				}
				ent := &ov.entries[idx]
				zs := &zipFileState{ov: ov, ent: ent, uncomp: ent.uncompSize}
				got := make([]byte, 0, size)
				buf := make([]byte, 64<<10)
				for off := int64(0); off < ent.uncompSize; {
					n, err := zs.ReadAt(context.Background(), buf, off)
					got = append(got, buf[:n]...)
					off += int64(n)
					if err == io.EOF {
						break
					}
					if err != nil {
						t.Errorf("%s: %v", name, err)
						zs.close()
						return
					}
				}
				zs.close()
				if !bytes.Equal(got, want[name]) {
					t.Errorf("%s: content mismatch (%d bytes)", name, len(got))
					return
				}
			}
		})
	}
	wg.Wait()
}
