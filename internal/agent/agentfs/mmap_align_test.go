package agentfs

import "testing"

func TestAlignForMmapProducesMappableOffsets(t *testing.T) {
	const granularity = 65536
	s := &Server{allocGranularity: granularity}

	cases := []struct {
		name   string
		off    int64
		length int
	}{
		{"zero offset", 0, 4096},
		{"unaligned small", 1, 4096},
		{"one byte before boundary", granularity - 1, 4096},
		{"exactly one granule", granularity, 4096},
		{"deep unaligned", 5*granularity + 1234, 1 << 20},
		{"large offset", 12345678901, 65536},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			aligned, diff, size := s.alignForMmap(tc.off, tc.length)

			if aligned%granularity != 0 {
				t.Fatalf("aligned offset %d is not a multiple of %d", aligned, granularity)
			}
			if aligned > tc.off {
				t.Fatalf("aligned offset %d overshot requested %d", aligned, tc.off)
			}
			if got := aligned + int64(diff); got != tc.off {
				t.Fatalf("aligned+diff = %d, want the requested offset %d", got, tc.off)
			}
			if size != tc.length+diff {
				t.Fatalf("size %d does not cover diff %d plus length %d", size, diff, tc.length)
			}
			if diff < 0 || diff+tc.length > size {
				t.Fatalf("reslice [%d:%d] escapes a mapping of size %d", diff, diff+tc.length, size)
			}
		})
	}
}
