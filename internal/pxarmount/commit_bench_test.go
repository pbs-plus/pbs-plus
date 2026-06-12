package pxarmount

import (
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"testing"

	pxar "github.com/pbs-plus/pxar"
	"github.com/pbs-plus/pxar/backupproxy"
	"github.com/pbs-plus/pxar/encoder"
	"github.com/pbs-plus/pxar/format"
	"github.com/pbs-plus/pxar/transfer"
)

type noopWriter struct{}

func (noopWriter) Begin(_ *pxar.Metadata, _ transfer.Options) error { return nil }
func (noopWriter) WriteEntry(_ *pxar.Entry, _ []byte) error         { return nil }
func (noopWriter) WriteEntryRef(_ *pxar.Entry, _ uint64) error      { return nil }
func (noopWriter) WriteEntryReader(_ *pxar.Entry, _ io.Reader, _ uint64) error {
	return nil
}
func (noopWriter) BeginDirectory(_ string, _ *pxar.Metadata) error  { return nil }
func (noopWriter) EndDirectory() error                              { return nil }
func (noopWriter) InjectChunks(_ []backupproxy.KnownChunkRef) error { return nil }
func (noopWriter) Encoder() *encoder.Encoder                        { return nil }
func (noopWriter) Finish() error                                    { return nil }
func (noopWriter) Close() error                                     { return nil }

func makeDirEntries(n int) []dirEntrySlim {
	entries := make([]dirEntrySlim, n)
	for i := range entries {
		isDir := i < n/2
		entries[i] = dirEntrySlim{
			name:          fmt.Sprintf("entry_%04d", i),
			inode:         uint64(i + 2),
			entryStart:    uint64(i * 512),
			contentOffset: uint64(i*4096 + 2048),
			fileSize:      4096,
			mode:          0o644,
			uid:           1000,
			gid:           1000,
			mtimeSecs:     1700000000,
			mtimeNanos:    0,
			isDir:         isDir,
			isSymlink:     false,
			isReg:         !isDir,
		}
	}
	return entries
}

func makeGraphEdges(m int) []GraphEdge {
	edges := make([]GraphEdge, m)
	for i := range edges {
		kind := NodeFile
		if i%5 == 0 {
			kind = NodeDir
		}
		edges[i] = GraphEdge{
			ParentID: 1,
			Name:     fmt.Sprintf("journal_%04d", i),
			ChildID:  int64(100 + i),
		}
		_ = kind
	}
	return edges
}

func makeGraphNodes(edges []GraphEdge, hasDataFraction float64) map[int64]*GraphNode {
	nodes := make(map[int64]*GraphNode, len(edges))
	for i, e := range edges {
		kind := NodeFile
		if i%5 == 0 {
			kind = NodeDir
		}
		hasData := float64(i%100)/100.0 < hasDataFraction
		nodes[e.ChildID] = &GraphNode{
			ID:      e.ChildID,
			Kind:    kind,
			Mode:    0o644,
			UID:     1000,
			GID:     1000,
			Size:    4096,
			MtimeNs: 1700000000e9,
			CtimeNs: 1700000000e9,
			HasData: hasData,
		}
	}
	return nodes
}

func BenchmarkCommitWalkSliceBuilding(b *testing.B) {
	for _, size := range []int{10, 100, 1000} {
		b.Run(fmt.Sprintf("pxar_%d_edges_%d", size, size/5), func(b *testing.B) {
			pxarEntries := makeDirEntries(size)
			edges := makeGraphEdges(size / 5)
			nodes := makeGraphNodes(edges, 0.1)
			whiteouts := make([]string, 0)
			edgeNames := make(map[string]bool, len(edges))
			for _, e := range edges {
				edgeNames[e.Name] = true
			}
			whiteoutSet := make(map[string]bool, len(whiteouts))
			for _, w := range whiteouts {
				whiteoutSet[w] = true
			}

			b.ResetTimer()
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				var pxarDirs, pxarFiles []commitEntry
				for j := range pxarEntries {
					pe := &pxarEntries[j]
					if edgeNames[pe.name] || whiteoutSet[pe.name] {
						continue
					}
					if pe.isDir || pe.isSymlink {
						pxarDirs = append(pxarDirs, commitEntry{name: pe.name, pxarSlim: pe})
					} else {
						pxarFiles = append(pxarFiles, commitEntry{name: pe.name, pxarSlim: pe})
					}
				}
				allEntries := make([]commitEntry, 0, len(pxarDirs)+len(pxarFiles))
				allEntries = append(allEntries, pxarDirs...)
				allEntries = append(allEntries, pxarFiles...)
				for j := range allEntries {
					e := &allEntries[j]
					if e.pxarSlim.isDir || e.pxarSlim.isSymlink {
						e.sortKey = e.pxarSlim.entryStart
					} else {
						e.sortKey = e.pxarSlim.contentOffset
					}
				}
				sort.Slice(allEntries, func(a, bIdx int) bool {
					if allEntries[a].pxarSlim.isDir != allEntries[bIdx].pxarSlim.isDir {
						return allEntries[a].pxarSlim.isDir
					}
					return allEntries[a].sortKey < allEntries[bIdx].sortKey
				})

				var refEntries, newDataEntries []commitEntry
				refEntries = append(refEntries, allEntries...)
				for _, edge := range edges {
					node := nodes[edge.ChildID]
					if node == nil {
						continue
					}
					ce := commitEntry{name: edge.Name, node: node}
					if node.Kind == NodeFile && node.HasData {
						newDataEntries = append(newDataEntries, ce)
					} else {
						refEntries = append(refEntries, ce)
					}
				}

				_ = refEntries
				_ = newDataEntries
			}
		})
	}
}

func BenchmarkCommitWalkSliceBuildingPooled(b *testing.B) {
	for _, size := range []int{10, 100, 1000} {
		b.Run(fmt.Sprintf("pxar_%d_edges_%d", size, size/5), func(b *testing.B) {
			pxarEntries := makeDirEntries(size)
			edges := makeGraphEdges(size / 5)
			nodes := makeGraphNodes(edges, 0.1)
			whiteouts := make([]string, 0)
			edgeNames := make(map[string]bool, len(edges))
			for _, e := range edges {
				edgeNames[e.Name] = true
			}
			whiteoutSet := make(map[string]bool, len(whiteouts))
			for _, w := range whiteouts {
				whiteoutSet[w] = true
			}

			refEntries := make([]commitEntry, 0, 64)
			newDataEntries := make([]commitEntry, 0, 16)
			allEntries := make([]commitEntry, 0, 64)

			b.ResetTimer()
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				refEntries = refEntries[:0]
				newDataEntries = newDataEntries[:0]
				allEntries = allEntries[:0]

				for j := range pxarEntries {
					pe := &pxarEntries[j]
					if edgeNames[pe.name] || whiteoutSet[pe.name] {
						continue
					}
					if pe.isDir || pe.isSymlink {
						allEntries = append(allEntries, commitEntry{name: pe.name, pxarSlim: pe})
					}
				}
				for j := range pxarEntries {
					pe := &pxarEntries[j]
					if edgeNames[pe.name] || whiteoutSet[pe.name] {
						continue
					}
					if !pe.isDir && !pe.isSymlink {
						allEntries = append(allEntries, commitEntry{name: pe.name, pxarSlim: pe})
					}
				}
				for j := range allEntries {
					e := &allEntries[j]
					if e.pxarSlim.isDir || e.pxarSlim.isSymlink {
						e.sortKey = e.pxarSlim.entryStart
					} else {
						e.sortKey = e.pxarSlim.contentOffset
					}
				}
				sort.Slice(allEntries, func(a, bIdx int) bool {
					if allEntries[a].pxarSlim.isDir != allEntries[bIdx].pxarSlim.isDir {
						return allEntries[a].pxarSlim.isDir
					}
					return allEntries[a].sortKey < allEntries[bIdx].sortKey
				})

				refEntries = append(refEntries, allEntries...)
				for _, edge := range edges {
					node := nodes[edge.ChildID]
					if node == nil {
						continue
					}
					ce := commitEntry{name: edge.Name, node: node}
					if node.Kind == NodeFile && node.HasData {
						newDataEntries = append(newDataEntries, ce)
					} else {
						refEntries = append(refEntries, ce)
					}
				}

				_ = refEntries
				_ = newDataEntries
			}
		})
	}
}

func BenchmarkPxarEntryConstruction(b *testing.B) {
	node := &GraphNode{
		ID:         42,
		Kind:       NodeFile,
		Mode:       0o644,
		UID:        1000,
		GID:        1000,
		Size:       4096,
		MtimeNs:    1700000000e9,
		CtimeNs:    1700000000e9,
		HasData:    false,
		SymlinkTgt: "/some/target",
	}
	xattrs := []format.XAttr{
		format.NewXAttr([]byte("user.attr1"), []byte("value1")),
		format.NewXAttr([]byte("security.capability"), []byte{0, 1, 2, 3}),
	}

	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		meta := nodeToMetadata(node, xattrs)
		entry := &pxar.Entry{
			Path:     "testfile.dat",
			Kind:     pxar.KindFile,
			Metadata: meta,
			FileSize: node.Size,
		}
		_ = entry
	}
}

func BenchmarkPxarEntryReuse(b *testing.B) {
	node := &GraphNode{
		ID:         42,
		Kind:       NodeFile,
		Mode:       0o644,
		UID:        1000,
		GID:        1000,
		Size:       4096,
		MtimeNs:    1700000000e9,
		CtimeNs:    1700000000e9,
		HasData:    false,
		SymlinkTgt: "/some/target",
	}
	xattrs := []format.XAttr{
		format.NewXAttr([]byte("user.attr1"), []byte("value1")),
		format.NewXAttr([]byte("security.capability"), []byte{0, 1, 2, 3}),
	}
	var buf pxar.Entry

	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		meta := nodeToMetadata(node, xattrs)
		buf.Path = "testfile.dat"
		buf.Kind = pxar.KindFile
		buf.Metadata = meta
		buf.FileSize = node.Size
		_ = &buf
	}
}

func clonePxarEntry(e *pxar.Entry, name string) *pxar.Entry {
	clone := *e
	clone.Path = name
	return &clone
}

func BenchmarkClonePxarEntry(b *testing.B) {
	entry := &pxar.Entry{
		Path:          "original_name.dat",
		Kind:          pxar.KindFile,
		FileSize:      4096,
		PayloadOffset: 12345678,
		Metadata:      pxar.Metadata{},
	}

	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		clone := clonePxarEntry(entry, "renamed.dat")
		_ = clone
	}
}

func BenchmarkClonePxarEntryReuse(b *testing.B) {
	entry := &pxar.Entry{
		Path:          "original_name.dat",
		Kind:          pxar.KindFile,
		FileSize:      4096,
		PayloadOffset: 12345678,
		Metadata:      pxar.Metadata{},
	}
	var buf pxar.Entry

	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		buf = *entry
		buf.Path = "renamed.dat"
		_ = &buf
	}
}

func BenchmarkNodeToMetadata(b *testing.B) {
	node := &GraphNode{
		Kind:    NodeFile,
		Mode:    0o644,
		UID:     1000,
		GID:     1000,
		MtimeNs: 1700000000e9,
	}

	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		_ = nodeToMetadata(node, nil)
	}
}

func BenchmarkJoinPath(b *testing.B) {
	b.Run("root", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			_ = joinPath("/", "entry_name")
		}
	})
	b.Run("deep", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			_ = joinPath("/a/b/c/d/e", "entry_name")
		}
	})
}

type mockWriter struct {
	noopWriter
	entries int
}

func (m *mockWriter) WriteEntry(_ *pxar.Entry, _ []byte) error {
	m.entries++
	return nil
}
func (m *mockWriter) WriteEntryRef(_ *pxar.Entry, _ uint64) error {
	m.entries++
	return nil
}
func (m *mockWriter) BeginDirectory(_ string, _ *pxar.Metadata) error { return nil }
func (m *mockWriter) EndDirectory() error                             { return nil }

func BenchmarkEmitAlphabeticalJournalSymlink(b *testing.B) {
	mfs := &MutableFS{
		verbose: false,
	}
	w := &mockWriter{}
	ow := &commitWalkState{
		mfs:        mfs,
		writer:     w,
		xattrCache: make(map[int64][]format.XAttr),
	}

	node := &GraphNode{
		ID:         42,
		Kind:       NodeSymlink,
		Mode:       0o777,
		UID:        1000,
		GID:        1000,
		SymlinkTgt: "/target/path",
	}
	ce := &commitEntry{name: "symlink_name", node: node}

	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		_ = ow.emitJournalEntry(ce, "/parent")
	}
}

func BenchmarkEmitAlphabeticalJournalFileEmpty(b *testing.B) {
	mfs := &MutableFS{
		verbose: false,
	}
	w := &mockWriter{}
	ow := &commitWalkState{
		mfs:        mfs,
		writer:     w,
		xattrCache: make(map[int64][]format.XAttr),
	}

	node := &GraphNode{
		ID:      42,
		Kind:    NodeFile,
		Mode:    0o644,
		UID:     1000,
		GID:     1000,
		Size:    0,
		MtimeNs: 1700000000e9,
	}
	ce := &commitEntry{name: "empty_file", node: node}

	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		_ = ow.emitJournalEntry(ce, "/parent")
	}
}

func BenchmarkVerifyBackedFileHashes(b *testing.B) {
	dir, err := os.MkdirTemp("", "bench-verify-*")
	if err != nil {
		b.Fatal(err)
	}
	defer func() { _ = os.RemoveAll(dir) }()

	mfs := &MutableFS{
		mutableDir: dir,
	}

	const fileCount = 50
	hashes := make(map[string]uint64, fileCount)
	for i := range fileCount {
		relPath := fmt.Sprintf("file_%04d.bin", i)
		absPath := filepath.Join(dir, relPath)
		data := make([]byte, 64*1024)
		for j := range data {
			data[j] = byte(i + j)
		}
		if err := os.WriteFile(absPath, data, 0o644); err != nil {
			b.Fatal(err)
		}
		hashes[relPath] = 0
	}

	prog := &noopProgress{}

	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		_ = verifyBackedFileHashes(mfs, hashes, prog)
	}
}

type noopProgress struct{}

func (noopProgress) SetPhase(ProgressPhase) {}
func (noopProgress) SetMsg(string)          {}
func (noopProgress) Done(string)            {}
func (noopProgress) Error(string)           {}
func (noopProgress) AddFile(int64)          {}
func (noopProgress) SetTotals(int64, int64) {}
func (noopProgress) State() ProgressState   { return ProgressState{} }

func BenchmarkTwoPointerMerge(b *testing.B) {
	for _, size := range []int{100, 1000, 10000} {
		b.Run(fmt.Sprintf("n=%d", size), func(b *testing.B) {
			pxarEntries := makeDirEntries(size)
			sort.Slice(pxarEntries, func(i, j int) bool {
				return pxarEntries[i].name < pxarEntries[j].name
			})

			edges := makeGraphEdges(size / 10)
			sort.Slice(edges, func(i, j int) bool {
				return edges[i].Name < edges[j].Name
			})

			edgeNames := make(map[string]bool, len(edges))
			for _, e := range edges {
				edgeNames[e.Name] = true
			}

			filtered := 0
			for i := range pxarEntries {
				if edgeNames[pxarEntries[i].name] {
					continue
				}
				if filtered != i {
					pxarEntries[filtered] = pxarEntries[i]
				}
				filtered++
			}
			pxarEntries = pxarEntries[:filtered]

			b.ResetTimer()
			b.ReportAllocs()
			for iter := 0; iter < b.N; iter++ {
				var result []commitEntry
				pi, ji := 0, 0
				for pi < len(pxarEntries) || ji < len(edges) {
					var ce commitEntry
					if pi >= len(pxarEntries) {
						ce = commitEntry{name: edges[ji].Name, node: &GraphNode{Kind: NodeFile, RedirectTo: "/" + edges[ji].Name, ID: edges[ji].ChildID}}
						ji++
					} else if ji >= len(edges) {
						ce = commitEntry{name: pxarEntries[pi].name, pxarSlim: &pxarEntries[pi]}
						pi++
					} else if pxarEntries[pi].name < edges[ji].Name {
						ce = commitEntry{name: pxarEntries[pi].name, pxarSlim: &pxarEntries[pi]}
						pi++
					} else {
						ce = commitEntry{name: edges[ji].Name, node: &GraphNode{Kind: NodeFile, RedirectTo: "/" + edges[ji].Name, ID: edges[ji].ChildID}}
						ji++
					}
					result = append(result, ce)
				}
				_ = result
			}
		})
	}
}

func BenchmarkAddToPendingRefs(b *testing.B) {
	for _, n := range []int{100, 1000} {
		b.Run(fmt.Sprintf("n=%d", n), func(b *testing.B) {
			ow := &commitWalkState{
				mfs:         &MutableFS{verbose: false},
				pendingRefs: make([]commitEntry, 0, maxPendingRefs),
			}

			b.ResetTimer()
			b.ReportAllocs()
			for iter := 0; iter < b.N; iter++ {
				ow.pendingRefs = ow.pendingRefs[:0]
				for i := range n {
					ce := commitEntry{
						name:     fmt.Sprintf("f_%d", i),
						pxarSlim: &dirEntrySlim{name: fmt.Sprintf("f_%d", i), contentOffset: uint64(i * 100), isReg: true},
						sortKey:  uint64(i * 100),
					}
					ow.pendingRefs = append(ow.pendingRefs, ce)
				}
			}
		})
	}
}

func BenchmarkFlushPendingRefs(b *testing.B) {
	w := &mockWriter{}
	ow := &commitWalkState{
		mfs:    &MutableFS{verbose: false},
		writer: w,
	}

	for i := range maxPendingRefs {
		ce := commitEntry{
			name:     fmt.Sprintf("f_%d", i),
			pxarSlim: &dirEntrySlim{name: fmt.Sprintf("f_%d", i), contentOffset: uint64((maxPendingRefs - i) * 100), isReg: true},
			sortKey:  uint64((maxPendingRefs - i) * 100),
		}
		ow.pendingRefs = append(ow.pendingRefs, ce)
	}

	b.ResetTimer()
	b.ReportAllocs()
	for iter := 0; iter < b.N; iter++ {
		ow.pendingRefs = ow.pendingRefs[:0]
		for i := range maxPendingRefs {
			ce := commitEntry{
				name:     fmt.Sprintf("f_%d", i),
				pxarSlim: &dirEntrySlim{name: fmt.Sprintf("f_%d", i), contentOffset: uint64((maxPendingRefs - i) * 100), isReg: true},
				sortKey:  uint64((maxPendingRefs - i) * 100),
			}
			ow.pendingRefs = append(ow.pendingRefs, ce)
		}
		sort.Slice(ow.pendingRefs, func(i, j int) bool {
			return ow.pendingRefs[i].sortKey < ow.pendingRefs[j].sortKey
		})
	}
}

func BenchmarkMergeMetaWithPxar(b *testing.B) {
	journalMeta := pxar.Metadata{
		Stat: format.Stat{
			Mode:  format.ModeIFREG | 0o644,
			UID:   1000,
			GID:   1000,
			Mtime: format.StatxTimestamp{Secs: 1700000000, Nanos: 123},
		},
		XAttrs: []format.XAttr{
			format.NewXAttr([]byte("user.a"), []byte("v1")),
			format.NewXAttr([]byte("user.b"), []byte("v2")),
		},
	}
	pxarEntry := &pxar.Entry{
		Metadata: pxar.Metadata{
			Stat: format.Stat{
				Mode:  format.ModeIFREG | 0o755,
				Flags: 0x10,
			},
			FCaps: []byte{1, 2, 3, 4},
			ACL: pxar.ACL{
				Users: []format.ACLUser{{UID: 1000, Permissions: 7}},
			},
		},
	}

	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		_ = mergeMetaWithPxar(journalMeta, pxarEntry)
	}
}

func BenchmarkCommitWalkFull(b *testing.B) {
	for _, n := range []int{100, 1000} {
		b.Run(fmt.Sprintf("n=%d", n), func(b *testing.B) {
			w := &mockWriter{}
			ow := &commitWalkState{
				mfs:          &MutableFS{verbose: false},
				writer:       w,
				xattrCache:   make(map[int64][]format.XAttr),
				backedHashes: make(map[string]uint64),
				pendingRefs:  make([]commitEntry, 0, maxPendingRefs),
			}

			pxarEntries := makeDirEntries(n)
			sort.Slice(pxarEntries, func(i, j int) bool {
				return pxarEntries[i].name < pxarEntries[j].name
			})

			edges := makeGraphEdges(n / 10)
			sort.Slice(edges, func(i, j int) bool {
				return edges[i].Name < edges[j].Name
			})

			edgeNames := make(map[string]bool, len(edges))
			for _, e := range edges {
				edgeNames[e.Name] = true
			}

			b.ResetTimer()
			b.ReportAllocs()
			for iter := 0; iter < b.N; iter++ {
				ow.pendingRefs = ow.pendingRefs[:0]

				filtered := 0
				for i := range pxarEntries {
					if edgeNames[pxarEntries[i].name] {
						continue
					}
					if filtered != i {
						pxarEntries[filtered] = pxarEntries[i]
					}
					filtered++
				}
				pxarFiltered := pxarEntries[:filtered]

				pi, ji := 0, 0
				for pi < len(pxarFiltered) || ji < len(edges) {
					var ce commitEntry
					if pi >= len(pxarFiltered) {
						ce = commitEntry{name: edges[ji].Name, node: &GraphNode{Kind: NodeFile, HasData: true, Size: 4096, MtimeNs: 1}}
						ji++
					} else if ji >= len(edges) {
						ce = commitEntry{name: pxarFiltered[pi].name, pxarSlim: &pxarFiltered[pi]}
						pi++
					} else if pxarFiltered[pi].name < edges[ji].Name {
						ce = commitEntry{name: pxarFiltered[pi].name, pxarSlim: &pxarFiltered[pi]}
						pi++
					} else {
						ce = commitEntry{name: edges[ji].Name, node: &GraphNode{Kind: NodeFile, HasData: true, Size: 4096, MtimeNs: 1}}
						ji++
					}

					if ce.pxarSlim != nil && ce.pxarSlim.isReg {
						ce.sortKey = ce.pxarSlim.contentOffset
						ow.pendingRefs = append(ow.pendingRefs, ce)
					} else if ce.pxarSlim != nil && ce.pxarSlim.isDir {
						_ = w.BeginDirectory(ce.name, nil)
						_ = w.EndDirectory()
					} else if ce.node != nil && ce.node.HasData {
						_ = w.WriteEntryReader(&pxar.Entry{Path: ce.name, Kind: pxar.KindFile, FileSize: ce.node.Size}, nil, ce.node.Size)
					} else {
						_ = w.WriteEntry(&pxar.Entry{Path: ce.name, Kind: pxar.KindFile}, nil)
					}
				}

				sort.Slice(ow.pendingRefs, func(i, j int) bool {
					return ow.pendingRefs[i].sortKey < ow.pendingRefs[j].sortKey
				})
				for _, ce := range ow.pendingRefs {
					_ = w.WriteEntryRef(&pxar.Entry{Path: ce.name, Kind: pxar.KindFile}, ce.sortKey)
				}
			}
		})
	}
}

func BenchmarkCommitWalkBloat(b *testing.B) {
	tests := []struct {
		rootDirs    int
		filesPerDir int
	}{
		{100, 100},
		{1000, 50},
	}
	for _, tt := range tests {
		name := fmt.Sprintf("nested-root=%d-sub=%d", tt.rootDirs, tt.filesPerDir)
		b.Run(name, func(b *testing.B) {
			b.ReportAllocs()
			for iter := 0; iter < b.N; iter++ {
				entries := makeDirEntries(tt.rootDirs)
				for i := range entries {
					entries[i].isDir = true
				}
				pi := 0
				for pi < len(entries) {
					if entries[pi].name == "" {
						panic("unreachable")
					}
					_ = makeDirEntries(tt.filesPerDir)
					pi++
				}
				_ = entries
			}
		})
	}
}
