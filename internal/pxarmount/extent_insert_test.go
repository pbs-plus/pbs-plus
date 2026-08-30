package pxarmount

import (
	"math/rand"
	"os"
	"path/filepath"
	"reflect"
	"testing"
)

func TestInsertDataExtent(t *testing.T) {
	tests := []struct {
		name  string
		start []dataExtent
		ins   [][2]uint64
		want  []dataExtent
	}{
		{
			name: "sequential append coalesces",
			ins:  [][2]uint64{{0, 10}, {10, 20}, {20, 30}},
			want: []dataExtent{{0, 30}},
		},
		{
			name: "disjoint stays split and sorted",
			ins:  [][2]uint64{{40, 50}, {0, 10}, {20, 30}},
			want: []dataExtent{{0, 10}, {20, 30}, {40, 50}},
		},
		{
			name: "gap filler merges both neighbours",
			ins:  [][2]uint64{{0, 10}, {20, 30}, {10, 20}},
			want: []dataExtent{{0, 30}},
		},
		{
			name: "overlapping write absorbs several",
			ins:  [][2]uint64{{0, 10}, {20, 30}, {40, 50}, {5, 45}},
			want: []dataExtent{{0, 50}},
		},
		{
			name:  "rewrite inside an existing extent is a no-op",
			start: []dataExtent{{0, 100}},
			ins:   [][2]uint64{{10, 20}},
			want:  []dataExtent{{0, 100}},
		},
		{
			name: "empty range ignored",
			ins:  [][2]uint64{{5, 5}},
			want: nil,
		},
		{
			name: "backwards write before the tail",
			ins:  [][2]uint64{{100, 200}, {0, 10}},
			want: []dataExtent{{0, 10}, {100, 200}},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := append([]dataExtent(nil), tt.start...)
			for _, in := range tt.ins {
				got = insertDataExtent(got, in[0], in[1])
			}
			if len(got) == 0 && len(tt.want) == 0 {
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Fatalf("got %v, want %v", got, tt.want)
			}
		})
	}
}

// Must agree with the sort-and-merge it replaced, or sparse reads pull the wrong layer.
func TestInsertDataExtentMatchesMerge(t *testing.T) {
	rng := rand.New(rand.NewSource(1))
	for range 200 {
		var incremental []dataExtent
		var all []dataExtent
		for range 30 {
			start := uint64(rng.Intn(500))
			end := start + uint64(rng.Intn(50))
			incremental = insertDataExtent(incremental, start, end)
			all = append(all, dataExtent{Start: start, End: end})
		}
		want := mergeDataExtents(nil, all)
		if len(incremental) == 0 && len(want) == 0 {
			continue
		}
		if !reflect.DeepEqual(incremental, want) {
			t.Fatalf("incremental %v != merged %v", incremental, want)
		}
	}
}

func TestRebuildDataExtents(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "sparse.bin")

	const size = 1 << 20
	f, err := os.Create(path)
	if err != nil {
		t.Fatal(err)
	}
	if err := f.Truncate(size); err != nil {
		t.Fatal(err)
	}
	payload := make([]byte, 8192)
	for i := range payload {
		payload[i] = 0xAB
	}
	for _, off := range []int64{0, 512 << 10} {
		if _, err := f.WriteAt(payload, off); err != nil {
			t.Fatal(err)
		}
	}
	if err := f.Sync(); err != nil {
		t.Fatal(err)
	}
	if err := f.Close(); err != nil {
		t.Fatal(err)
	}

	got, err := rebuildDataExtents(path, size)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) == 0 {
		t.Fatal("no extents recovered from a file with written data")
	}
	for _, want := range []dataExtent{{0, 8192}, {512 << 10, (512 << 10) + 8192}} {
		covered := false
		for _, e := range got {
			if e.Start <= want.Start && e.End >= want.End {
				covered = true
				break
			}
		}
		if !covered {
			t.Fatalf("written range %v not covered by %v", want, got)
		}
	}
	for _, e := range got {
		if e.Start >= e.End || e.End > size {
			t.Fatalf("malformed extent %v", e)
		}
	}
}

func TestRebuildDataExtentsFullySparse(t *testing.T) {
	path := filepath.Join(t.TempDir(), "hole.bin")
	f, err := os.Create(path)
	if err != nil {
		t.Fatal(err)
	}
	if err := f.Truncate(1 << 20); err != nil {
		t.Fatal(err)
	}
	if err := f.Close(); err != nil {
		t.Fatal(err)
	}

	got, err := rebuildDataExtents(path, 1<<20)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 0 {
		t.Fatalf("hole-only file reported extents %v", got)
	}
}

func TestPermitAccess(t *testing.T) {
	tests := []struct {
		name     string
		mode     uint32
		owner    uint32
		group    uint32
		isDir    bool
		uid      uint32
		gid      uint32
		mask     uint32
		expected bool
	}{
		{"owner read on 0600", 0o600, 1000, 1000, false, 1000, 1000, 4, true},
		{"other read on 0600", 0o600, 1000, 1000, false, 1001, 1001, 4, false},
		{"other write on 0644", 0o644, 1000, 1000, false, 1001, 1001, 2, false},
		{"group read on 0640", 0o640, 1000, 1000, false, 1001, 1000, 4, true},
		{"group write on 0640", 0o640, 1000, 1000, false, 1001, 1000, 2, false},
		{"owner rw needs both bits", 0o400, 1000, 1000, false, 1000, 1000, 6, false},
		{"existence check always ok", 0o000, 1000, 1000, false, 1001, 1001, 0, true},
		{"root reads a 0000 file", 0o000, 1000, 1000, false, 0, 0, 4, true},
		{"root cannot exec a 0644 file", 0o644, 1000, 1000, false, 0, 0, 1, false},
		{"root execs when any x bit set", 0o644 | 0o010, 1000, 1000, false, 0, 0, 1, true},
		{"root traverses any directory", 0o000, 1000, 1000, true, 0, 0, 1, true},
		{"owner beats a wider group bit", 0o477, 1000, 1000, false, 1000, 1000, 2, false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			re := &ResolvedEntry{Mode: tt.mode, UID: tt.owner, GID: tt.group, IsDir: tt.isDir}
			if got := permitAccess(re, tt.uid, tt.gid, tt.mask); got != tt.expected {
				t.Fatalf("permitAccess = %v, want %v", got, tt.expected)
			}
		})
	}
}
