//go:build linux

package agentfs

import (
	"cmp"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"reflect"
	"slices"
	"testing"

	"github.com/pbs-plus/pbs-plus/internal/agent/agentfs/fswire"
	"go.uber.org/goleak"
	"golang.org/x/sys/unix"
)

func mkFiles(t *testing.T, dir string, n int, size int) []string {
	t.Helper()
	names := make([]string, 0, n)
	payload := make([]byte, size)
	for i := range n {
		name := fmt.Sprintf("file-%05d-%s", i, "padded_name_to_widen_the_dirent")
		if err := os.WriteFile(filepath.Join(dir, name), payload, 0o644); err != nil {
			t.Fatalf("write %s: %v", name, err)
		}
		names = append(names, name)
	}
	return names
}

func readdirAll(t *testing.T, dir string, batch int) []fswire.AgentFileInfo {
	t.Helper()
	f, err := os.Open(dir)
	if err != nil {
		t.Fatalf("open %s: %v", dir, err)
	}
	r, err := NewDirReader(f, dir)
	if err != nil {
		t.Fatalf("NewDirReader: %v", err)
	}
	defer r.Close()

	var all []fswire.AgentFileInfo
	for {
		got, err := r.readdir(batch, 4096)
		if err == io.EOF {
			break
		}
		if err != nil {
			t.Fatalf("readdir: %v", err)
		}
		if len(got) == 0 {
			break
		}
		all = append(all, got...)
	}
	return all
}

func TestParseDirentsMatchesGetdents(t *testing.T) {
	dir := t.TempDir()
	want := map[string]uint64{}

	for _, name := range []string{"plain", "with space", "ünïcødé", ".hidden", "a"} {
		if err := os.WriteFile(filepath.Join(dir, name), nil, 0o644); err != nil {
			t.Fatal(err)
		}
		var st unix.Stat_t
		if err := unix.Lstat(filepath.Join(dir, name), &st); err != nil {
			t.Fatal(err)
		}
		want[name] = st.Ino
	}
	if err := os.Mkdir(filepath.Join(dir, "sub"), 0o755); err != nil {
		t.Fatal(err)
	}
	var subSt unix.Stat_t
	if err := unix.Lstat(filepath.Join(dir, "sub"), &subSt); err != nil {
		t.Fatal(err)
	}
	want["sub"] = subSt.Ino

	fd, err := unix.Open(dir, unix.O_RDONLY|unix.O_CLOEXEC, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer unix.Close(fd)

	buf := make([]byte, 64*1024)
	n, err := unix.Getdents(fd, buf)
	if err != nil {
		t.Fatal(err)
	}

	consumed, ents := parseDirents(buf[:n], 1<<30, nil)
	if consumed != n {
		t.Errorf("consumed %d bytes, getdents returned %d", consumed, n)
	}
	if len(ents) != len(want) {
		t.Fatalf("got %d entries, want %d: %v", len(ents), len(want), ents)
	}

	for _, e := range ents {
		ino, ok := want[e.name]
		if !ok {
			t.Errorf("unexpected entry %q", e.name)
			continue
		}
		if e.ino != ino {
			t.Errorf("%q: d_ino %d, want %d", e.name, e.ino, ino)
		}
		delete(want, e.name)
	}
	for name := range want {
		t.Errorf("missing entry %q", name)
	}
}

func TestParseDirentsRespectsMax(t *testing.T) {
	dir := t.TempDir()
	mkFiles(t, dir, 20, 0)

	fd, err := unix.Open(dir, unix.O_RDONLY|unix.O_CLOEXEC, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer unix.Close(fd)

	buf := make([]byte, 64*1024)
	n, err := unix.Getdents(fd, buf)
	if err != nil {
		t.Fatal(err)
	}

	consumed, ents := parseDirents(buf[:n], 5, nil)
	if len(ents) != 5 {
		t.Fatalf("got %d entries, want 5", len(ents))
	}
	if consumed >= n {
		t.Errorf("consumed %d of %d bytes, want a partial consume", consumed, n)
	}

	_, rest := parseDirents(buf[consumed:n], 1<<30, nil)
	if len(rest) != 15 {
		t.Errorf("resumed parse got %d entries, want 15", len(rest))
	}
}

func TestReaddirMetadataMatchesLstat(t *testing.T) {
	dir := t.TempDir()
	names := mkFiles(t, dir, 300, 1234)

	if err := os.Mkdir(filepath.Join(dir, "subdir"), 0o755); err != nil {
		t.Fatal(err)
	}
	names = append(names, "subdir")
	if err := os.Symlink("file-00000", filepath.Join(dir, "link")); err != nil {
		t.Fatal(err)
	}

	got := readdirAll(t, dir, defaultBatchSize)

	seen := map[string]fswire.AgentFileInfo{}
	for _, info := range got {
		if _, dup := seen[info.Name]; dup {
			t.Fatalf("duplicate entry %q", info.Name)
		}
		seen[info.Name] = info
	}

	if _, ok := seen["link"]; ok {
		t.Error("symlink should be excluded")
	}
	if len(seen) != len(names) {
		t.Fatalf("got %d entries, want %d", len(seen), len(names))
	}

	for _, name := range names {
		info, ok := seen[name]
		if !ok {
			t.Fatalf("missing entry %q", name)
		}
		var st unix.Stat_t
		if err := unix.Lstat(filepath.Join(dir, name), &st); err != nil {
			t.Fatal(err)
		}
		isDir := st.Mode&unix.S_IFMT == unix.S_IFDIR
		if info.IsDir != isDir {
			t.Errorf("%q: IsDir %v, want %v", name, info.IsDir, isDir)
		}
		if !isDir && info.Size != st.Size {
			t.Errorf("%q: Size %d, want %d", name, info.Size, st.Size)
		}
		if info.Mode&0o777 != uint32(st.Mode&0o777) {
			t.Errorf("%q: Mode %o, want %o", name, info.Mode&0o777, st.Mode&0o777)
		}
		if want := st.Mtim.Sec*1e9 + st.Mtim.Nsec; info.ModTime != want {
			t.Errorf("%q: ModTime %d, want %d", name, info.ModTime, want)
		}
		if !isDir {
			if want := uint64((st.Size + 4095) / 4096); info.Blocks < want {
				t.Errorf("%q: Blocks %d, want at least %d", name, info.Blocks, want)
			}
		}
	}
}

func TestReaddirStatsInInodeOrder(t *testing.T) {
	dir := t.TempDir()
	mkFiles(t, dir, 400, 0)

	got := readdirAll(t, dir, defaultBatchSize)
	if len(got) != 400 {
		t.Fatalf("got %d entries, want 400", len(got))
	}

	inos := make([]uint64, len(got))
	for i, info := range got {
		var st unix.Stat_t
		if err := unix.Lstat(filepath.Join(dir, info.Name), &st); err != nil {
			t.Fatal(err)
		}
		inos[i] = st.Ino
	}

	if !slices.IsSorted(inos) {
		t.Errorf("entries were not stat'ed in inode order: %v", inos[:min(len(inos), 16)])
	}

	fd, err := unix.Open(dir, unix.O_RDONLY|unix.O_CLOEXEC, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer unix.Close(fd)
	buf := make([]byte, 1024*1024)
	n, err := unix.Getdents(fd, buf)
	if err != nil {
		t.Fatal(err)
	}
	_, raw := parseDirents(buf[:n], 1<<30, nil)

	inversions := 0
	for i := 1; i < len(raw); i++ {
		if raw[i].ino < raw[i-1].ino {
			inversions++
		}
	}
	t.Logf("getdents order: %d/%d inode inversions before sorting", inversions, len(raw))
}

func TestStatDirentsParallelMatchesSerial(t *testing.T) {
	dir := t.TempDir()
	mkFiles(t, dir, 500, 7)

	fd, err := unix.Open(dir, unix.O_RDONLY|unix.O_CLOEXEC, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer unix.Close(fd)

	buf := make([]byte, 1024*1024)
	n, err := unix.Getdents(fd, buf)
	if err != nil {
		t.Fatal(err)
	}
	_, ents := parseDirents(buf[:n], 1<<30, nil)
	if len(ents) < statParallelThreshold {
		t.Fatalf("need more than %d entries to exercise the pool, got %d", statParallelThreshold, len(ents))
	}

	serial := withStatWorkerLimit(t, 1, func() []fswire.AgentFileInfo {
		out, err := statDirents(nil, fd, ents, 4096)
		if err != nil {
			t.Fatalf("serial statDirents: %v", err)
		}
		return out
	})
	parallel := withStatWorkerLimit(t, 16, func() []fswire.AgentFileInfo {
		out, err := statDirents(nil, fd, ents, 4096)
		if err != nil {
			t.Fatalf("parallel statDirents: %v", err)
		}
		return out
	})

	if len(serial) != len(parallel) {
		t.Fatalf("serial %d entries, parallel %d", len(serial), len(parallel))
	}
	for i := range serial {
		if !reflect.DeepEqual(serial[i], parallel[i]) {
			t.Fatalf("entry %d differs:\n serial   %+v\n parallel %+v", i, serial[i], parallel[i])
		}
	}
	if statWorkers(len(ents)) < 2 {
		t.Fatalf("statWorkers(%d) = %d, expected the parallel path", len(ents), statWorkers(len(ents)))
	}
}

func TestStatDirentsSkipsVanishedEntries(t *testing.T) {
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, "here"), nil, 0o644); err != nil {
		t.Fatal(err)
	}

	fd, err := unix.Open(dir, unix.O_RDONLY|unix.O_CLOEXEC, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer unix.Close(fd)

	ents := []dirent{{ino: 1, name: "gone"}, {ino: 2, name: "here"}}
	for _, limit := range []int{1, 16} {
		got := withStatWorkerLimit(t, limit, func() []fswire.AgentFileInfo {
			out, err := statDirents(nil, fd, ents, 4096)
			if err != nil {
				t.Fatalf("statDirents (workers=%d): %v", limit, err)
			}
			return out
		})
		if len(got) != 1 || got[0].Name != "here" {
			t.Fatalf("workers=%d: got %+v, want only \"here\"", limit, got)
		}
		for i, info := range got[len(got):cap(got)] {
			if !reflect.DeepEqual(info, fswire.AgentFileInfo{}) {
				t.Fatalf("workers=%d: scratch entry %d retains %+v", limit, i, info)
			}
		}
	}
}

func TestStatDirentsDoesNotLeakWorkers(t *testing.T) {
	defer goleak.VerifyNone(t, goleak.IgnoreCurrent())

	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, "here"), nil, 0o644); err != nil {
		t.Fatal(err)
	}
	fd, err := unix.Open(dir, unix.O_RDONLY|unix.O_CLOEXEC, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer unix.Close(fd)

	ents := make([]dirent, 512)
	for i := range ents {
		ents[i] = dirent{ino: uint64(i + 1), name: "here"}
	}

	withStatWorkerLimit(t, 16, func() struct{} {
		if _, err := statDirents(nil, fd, ents, 4096); err != nil {
			t.Fatalf("successful batch: %v", err)
		}

		broken := slices.Clone(ents)
		broken[len(broken)/2].name = "here/child"
		out, err := statDirents(nil, fd, broken, 4096)
		if err == nil {
			t.Fatal("broken batch returned no error")
		}
		for i, info := range out[:cap(out)] {
			if !reflect.DeepEqual(info, fswire.AgentFileInfo{}) {
				t.Fatalf("error scratch entry %d retains %+v", i, info)
			}
		}

		for range 100 {
			if _, err := statDirents(nil, -1, ents, 4096); err == nil {
				t.Fatal("bad descriptor batch returned no error")
			}
		}
		return struct{}{}
	})
}

func TestStatWorkers(t *testing.T) {
	defer func(old int) { statWorkerLimit = old }(statWorkerLimit)
	statWorkerLimit = 16

	for _, tc := range []struct{ n, want int }{
		{0, 1},
		{1, 1},
		{statParallelThreshold - 1, 1},
		{statParallelThreshold, 2},
		{statParallelThreshold * 4, 4},
		{statParallelThreshold * 1000, 16},
	} {
		if got := statWorkers(tc.n); got != tc.want {
			t.Errorf("statWorkers(%d) = %d, want %d", tc.n, got, tc.want)
		}
	}

	statWorkerLimit = 1
	if got := statWorkers(10000); got != 1 {
		t.Errorf("statWorkers with limit 1 = %d, want 1", got)
	}
}

func withStatWorkerLimit[T any](t *testing.T, limit int, fn func() T) T {
	t.Helper()
	old := statWorkerLimit
	statWorkerLimit = limit
	defer func() { statWorkerLimit = old }()
	return fn()
}

func BenchmarkStatDirentsOrder(b *testing.B) {
	dir := b.TempDir()
	for i := range 10000 {
		path := filepath.Join(dir, fmt.Sprintf("file-%05d-padded_name_to_widen_the_dirent", i))
		if err := os.WriteFile(path, nil, 0o644); err != nil {
			b.Fatal(err)
		}
	}

	fd, err := unix.Open(dir, unix.O_RDONLY|unix.O_CLOEXEC, 0)
	if err != nil {
		b.Fatal(err)
	}
	defer unix.Close(fd)

	buf := make([]byte, 4*1024*1024)
	n, err := unix.Getdents(fd, buf)
	if err != nil {
		b.Fatal(err)
	}
	_, raw := parseDirents(buf[:n], 1<<30, nil)
	sorted := slices.Clone(raw)
	slices.SortFunc(sorted, func(a, b dirent) int { return cmp.Compare(a.ino, b.ino) })

	old := statWorkerLimit
	statWorkerLimit = 1
	defer func() { statWorkerLimit = old }()

	for _, tc := range []struct {
		name string
		ents []dirent
	}{{"getdents_order", raw}, {"inode_order", sorted}} {
		b.Run(tc.name, func(b *testing.B) {
			for b.Loop() {
				if _, err := statDirents(nil, fd, tc.ents, 4096); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

func BenchmarkReaddir(b *testing.B) {
	dir := b.TempDir()
	payload := make([]byte, 64)
	for i := range 10000 {
		path := filepath.Join(dir, fmt.Sprintf("file-%05d-padded_name_to_widen_the_dirent", i))
		if err := os.WriteFile(path, payload, 0o644); err != nil {
			b.Fatal(err)
		}
	}

	for _, workers := range []int{1, 2, 4, 8, 16, 32} {
		b.Run(fmt.Sprintf("workers=%d/10000files", workers), func(b *testing.B) {
			old := statWorkerLimit
			statWorkerLimit = workers
			defer func() { statWorkerLimit = old }()

			b.ReportAllocs()
			for b.Loop() {
				f, err := os.Open(dir)
				if err != nil {
					b.Fatal(err)
				}
				r, err := NewDirReader(f, dir)
				if err != nil {
					b.Fatal(err)
				}
				count := 0
				for {
					got, err := r.readdir(defaultBatchSize, 4096)
					if err == io.EOF || len(got) == 0 {
						break
					}
					if err != nil {
						b.Fatal(err)
					}
					count += len(got)
				}
				if count != 10000 {
					b.Fatalf("read %d entries, want 10000", count)
				}
				if err := r.Close(); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}
