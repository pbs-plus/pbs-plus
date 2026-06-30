package tapeio

import (
	"fmt"
	"strings"
	"testing"
)

func relPaths(n int) []string {
	out := make([]string, n)
	for i := range n {
		depth := i % 5
		var b strings.Builder
		for d := range depth {
			if d > 0 {
				b.WriteByte('/')
			}
			fmt.Fprintf(&b, "dir%d", d)
		}
		if depth > 0 {
			b.WriteByte('/')
		}
		fmt.Fprintf(&b, "file_%d.bin", i)
		out[i] = b.String()
	}
	return out
}

func BenchmarkPumpEntryStrings(b *testing.B) {
	paths := relPaths(1000)
	root := "C:"
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		p := paths[i%len(paths)]
		rel := strings.TrimPrefix(strings.TrimPrefix(p, root), "/")
		comps := strings.Split(rel, "/")
		name := sanitizeName(comps[len(comps)-1])
		depth := len(comps) - 1
		_ = name
		_ = depth
	}
}

func BenchmarkPumpEntryStringsNoSplit(b *testing.B) {
	paths := relPaths(1000)
	root := "C:"
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		p := paths[i%len(paths)]
		rel := strings.TrimPrefix(strings.TrimPrefix(p, root), "/")
		name, depth := lastNameSegment(rel)
		name = sanitizeName(name)
		_ = name
		_ = depth
	}
}
