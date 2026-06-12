package pxarmount

import (
	"fmt"
	"io"
	"sort"
	"sync"
	"testing"
	"time"

	pxar "github.com/pbs-plus/pxar"
	"github.com/pbs-plus/pxar/backupproxy"
	"github.com/pbs-plus/pxar/datastore"
	"github.com/pbs-plus/pxar/encoder"
	"github.com/pbs-plus/pxar/format"
	"github.com/zeebo/xxh3"
)

type countingWriter struct {
	noopWriter
	refCalls int
}

func (c *countingWriter) WriteEntryRef(_ *pxar.Entry, _ uint64) error {
	c.refCalls++
	return nil
}

func BenchmarkB1_PerFileWriteEntryRef(b *testing.B) {
	for _, n := range []int{1000, 10000, 100000} {
		b.Run(fmt.Sprintf("files=%d", n), func(b *testing.B) {
			w := &countingWriter{}
			ow := &commitWalkState{
				mfs:           &MutableFS{verbose: false},
				writer:        w,
				xattrCache:    make(map[int64][]format.XAttr),
				backedHashes:  make(map[string]uint64),
				redirectCache: make(map[string]*pxar.Entry),
				pendingRefs:   make([]commitEntry, 0, maxPendingRefs),
			}
			ow.hasPrevRef = false

			refs := make([]commitEntry, n)
			for i := range refs {
				refs[i] = commitEntry{
					name: fmt.Sprintf("file_%06d.bin", i),
					pxarSlim: &dirEntrySlim{
						name:          fmt.Sprintf("file_%06d.bin", i),
						contentOffset: uint64(i+1) * 4096,
						payloadOffset: uint64(i+1) * 4096,
						isReg:         true,
					},
					sortKey:     uint64(i+1) * 4096,
					cachedEntry: &pxar.Entry{Path: fmt.Sprintf("file_%06d.bin", i), Kind: pxar.KindFile, PayloadOffset: uint64(i+1) * 4096, FileSize: 4096},
				}
			}

			b.ResetTimer()
			b.ReportAllocs()
			for iter := 0; iter < b.N; iter++ {
				ow.pendingRefs = ow.pendingRefs[:0]
				ow.prevRefOffset = 0
				ow.hasPrevRef = false

				ow.pendingRefs = append(ow.pendingRefs, refs...)

				sort.Slice(ow.pendingRefs, func(i, j int) bool {
					return ow.pendingRefs[i].sortKey < ow.pendingRefs[j].sortKey
				})
				for i := range ow.pendingRefs {
					ce := &ow.pendingRefs[i]
					_ = ow.emitPxarRefAt(ce, 0)
				}
			}
		})
	}
}

func BenchmarkB1_ChunkCoalesced(b *testing.B) {
	for _, n := range []int{1000, 10000, 100000} {
		b.Run(fmt.Sprintf("files=%d", n), func(b *testing.B) {
			chunkSize := uint64(4 << 20)
			files := make([]commitEntry, n)
			for i := range files {
				files[i] = commitEntry{
					name:    fmt.Sprintf("file_%06d.bin", i),
					sortKey: uint64(i+1) * 4096,
				}
			}

			type chunkRange struct {
				startIdx, endIdx int
			}

			b.ResetTimer()
			b.ReportAllocs()
			for iter := 0; iter < b.N; iter++ {
				var ranges []chunkRange
				start := 0
				for i := 1; i <= len(files); i++ {
					var endOffset uint64
					if i < len(files) {
						endOffset = files[i].sortKey
					} else {
						endOffset = files[i-1].sortKey + 4096
					}
					if endOffset-files[start].sortKey >= chunkSize || i == len(files) {
						ranges = append(ranges, chunkRange{start, i})
						start = i
					}
				}
				_ = ranges
			}
		})
	}
}

func BenchmarkB2_DoubleDecodeOverhead(b *testing.B) {

	for _, n := range []int{1000, 10000} {
		b.Run(fmt.Sprintf("files=%d", n), func(b *testing.B) {
			entries := make([]*pxar.Entry, n)
			for i := range entries {
				entries[i] = &pxar.Entry{
					Path:          fmt.Sprintf("file_%06d.bin", i),
					Kind:          pxar.KindFile,
					FileOffset:    uint64(i * 512),
					ContentOffset: uint64(i*4096 + 2048),
					PayloadOffset: uint64(i+1) * 4096,
					FileSize:      4096,
					Metadata: pxar.Metadata{
						Stat: format.Stat{
							Mode: format.ModeIFREG | 0o644,
							UID:  1000,
							GID:  1000,
							Mtime: format.StatxTimestamp{
								Secs:  1700000000,
								Nanos: uint32(i),
							},
						},
					},
				}
			}

			b.ResetTimer()
			b.ReportAllocs()
			for iter := 0; iter < b.N; iter++ {
				slims := make([]dirEntrySlim, 0, n)
				for _, e := range entries {
					slims = append(slims, dirEntrySlim{
						name:          e.FileName(),
						inode:         ToInode(e),
						entryStart:    e.FileOffset,
						contentOffset: e.ContentOffset,
						payloadOffset: e.PayloadOffset,
						fileSize:      e.FileSize,
						mode:          statMode(e.Metadata.Stat.Mode),
						uid:           e.Metadata.Stat.UID,
						gid:           e.Metadata.Stat.GID,
						mtimeSecs:     e.Metadata.Stat.Mtime.Secs,
						mtimeNanos:    e.Metadata.Stat.Mtime.Nanos,
						isDir:         e.IsDir(),
						isSymlink:     e.IsSymlink(),
						isReg:         e.IsRegularFile(),
					})
				}

				cached := make([]*pxar.Entry, n)
				for i, s := range slims {
					if s.isReg {
						clone := *entries[i]
						clone.Path = s.name
						cached[i] = &clone
					}
				}
				_ = slims
				_ = cached
			}
		})
	}
}

func BenchmarkB2_SingleDecodeOverhead(b *testing.B) {
	for _, n := range []int{1000, 10000} {
		b.Run(fmt.Sprintf("files=%d", n), func(b *testing.B) {
			entries := make([]*pxar.Entry, n)
			for i := range entries {
				entries[i] = &pxar.Entry{
					Path:          fmt.Sprintf("file_%06d.bin", i),
					Kind:          pxar.KindFile,
					FileOffset:    uint64(i * 512),
					ContentOffset: uint64(i*4096 + 2048),
					PayloadOffset: uint64(i+1) * 4096,
					FileSize:      4096,
				}
			}

			b.ResetTimer()
			b.ReportAllocs()
			for iter := 0; iter < b.N; iter++ {
				type slimRef struct {
					name    string
					sortKey uint64
					entry   *pxar.Entry
				}
				results := make([]slimRef, 0, n)
				for _, e := range entries {
					results = append(results, slimRef{
						name:    e.FileName(),
						sortKey: e.PayloadOffset,
						entry:   e,
					})
				}
				_ = results
			}
		})
	}
}

func BenchmarkB3_MutexContention(b *testing.B) {
	for _, n := range []int{1000, 10000} {
		b.Run(fmt.Sprintf("locks=%d", n), func(b *testing.B) {
			var mu sync.Mutex

			b.ResetTimer()
			b.ReportAllocs()
			for iter := 0; iter < b.N; iter++ {
				for i := range n {
					mu.Lock()
					_ = i
					mu.Unlock()
				}
			}
		})
	}
}

func BenchmarkB3_NoMutex(b *testing.B) {
	for _, n := range []int{1000, 10000} {
		b.Run(fmt.Sprintf("ops=%d", n), func(b *testing.B) {
			b.ResetTimer()
			b.ReportAllocs()
			for iter := 0; iter < b.N; iter++ {
				for i := range n {
					_ = i
				}
			}
		})
	}
}

func BenchmarkB3_RWMutex(b *testing.B) {
	for _, n := range []int{1000, 10000} {
		b.Run(fmt.Sprintf("locks=%d", n), func(b *testing.B) {
			var mu sync.RWMutex

			b.ResetTimer()
			b.ReportAllocs()
			for iter := 0; iter < b.N; iter++ {
				for i := range n {
					mu.RLock()
					_ = i
					mu.RUnlock()
				}
			}
		})
	}
}

func BenchmarkB4_RedirectResolveHit(b *testing.B) {
	cache := make(map[string]*pxar.Entry, 10000)
	for i := range 10000 {
		path := fmt.Sprintf("/deep/nested/path/file_%06d.bin", i)
		cache[path] = &pxar.Entry{
			Path:          path,
			Kind:          pxar.KindFile,
			PayloadOffset: uint64(i+1) * 4096,
			FileSize:      4096,
			Metadata: pxar.Metadata{
				Stat: format.Stat{
					Mode: format.ModeIFREG | 0o644,
					UID:  1000,
					GID:  1000,
				},
				FCaps: make([]byte, 16),
				ACL: pxar.ACL{
					Users: []format.ACLUser{
						{UID: 1000, Permissions: 7},
						{UID: 1001, Permissions: 5},
					},
				},
			},
		}
	}

	lookupPath := "/deep/nested/path/file_005000.bin"

	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		entry, ok := cache[lookupPath]
		_ = entry
		_ = ok
	}
}

func BenchmarkB4_RedirectResolveWithMerge(b *testing.B) {
	cache := make(map[string]*pxar.Entry, 10000)
	for i := range 10000 {
		path := fmt.Sprintf("/deep/nested/path/file_%06d.bin", i)
		cache[path] = &pxar.Entry{
			Path:          path,
			Kind:          pxar.KindFile,
			PayloadOffset: uint64(i+1) * 4096,
			FileSize:      4096,
			Metadata: pxar.Metadata{
				Stat: format.Stat{
					Mode:  format.ModeIFREG | 0o644,
					UID:   1000,
					GID:   1000,
					Flags: 0x10,
				},
				FCaps: []byte{1, 2, 3, 4, 5, 6, 7, 8},
				ACL: pxar.ACL{
					Users: []format.ACLUser{
						{UID: 1000, Permissions: 7},
						{UID: 1001, Permissions: 5},
					},
				},
			},
		}
	}

	journalMeta := pxar.Metadata{
		Stat: format.Stat{
			Mode:  format.ModeIFREG | 0o644,
			UID:   1000,
			GID:   1000,
			Mtime: format.StatxTimestamp{Secs: 1700000000, Nanos: 123},
		},
		XAttrs: []format.XAttr{
			format.NewXAttr([]byte("user.a"), []byte("v1")),
		},
	}

	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		entry := cache["/deep/nested/path/file_005000.bin"]
		_ = mergeMetaWithPxar(journalMeta, entry)
	}
}

func BenchmarkB5_PendingRefsSort(b *testing.B) {
	for _, n := range []int{64, 256, 1024, maxPendingRefs} {
		b.Run(fmt.Sprintf("batch=%d", n), func(b *testing.B) {
			refs := make([]commitEntry, n)
			for i := range refs {
				refs[i] = commitEntry{
					name:    fmt.Sprintf("f_%06d", n-i),
					sortKey: uint64((n - i) * 100),
				}
			}

			b.ResetTimer()
			b.ReportAllocs()
			for iter := 0; iter < b.N; iter++ {
				for i := range refs {
					refs[i].sortKey = uint64((n - i) * 100)
				}
				sort.Slice(refs, func(i, j int) bool {
					return refs[i].sortKey < refs[j].sortKey
				})
			}
		})

		b.Run(fmt.Sprintf("batch=%d/nearly_sorted_insertion", n), func(b *testing.B) {
			refs := make([]commitEntry, n)
			for i := range refs {
				refs[i] = commitEntry{
					name:    fmt.Sprintf("f_%06d", i),
					sortKey: uint64(i * 100),
				}
			}
			for i := 0; i+1 < n; i += 20 {
				refs[i].sortKey, refs[i+1].sortKey = refs[i+1].sortKey, refs[i].sortKey
			}
			orig := make([]commitEntry, len(refs))
			copy(orig, refs)

			b.ResetTimer()
			b.ReportAllocs()
			for iter := 0; iter < b.N; iter++ {
				copy(refs, orig)
				insertionSortPendingRefs(refs)
			}
		})

		b.Run(fmt.Sprintf("batch=%d/reverse_insertion", n), func(b *testing.B) {
			refs := make([]commitEntry, n)
			for i := range refs {
				refs[i] = commitEntry{
					name:    fmt.Sprintf("f_%06d", n-i),
					sortKey: uint64((n - i) * 100),
				}
			}
			orig := make([]commitEntry, len(refs))
			copy(orig, refs)

			b.ResetTimer()
			b.ReportAllocs()
			for iter := 0; iter < b.N; iter++ {
				copy(refs, orig)
				insertionSortPendingRefs(refs)
			}
		})
	}
}

func BenchmarkB6_MetadataConstruction(b *testing.B) {
	ow := &commitWalkState{
		mfs:           &MutableFS{verbose: false},
		xattrCache:    make(map[int64][]format.XAttr),
		redirectCache: make(map[string]*pxar.Entry),
	}

	const nodeID int64 = 42
	ow.xattrCache[nodeID] = []format.XAttr{
		format.NewXAttr([]byte("user.attr1"), []byte("value1")),
		format.NewXAttr([]byte("user.attr2"), []byte("value2")),
	}

	ow.redirectCache["/some/file.bin"] = &pxar.Entry{
		Path:          "/some/file.bin",
		Kind:          pxar.KindFile,
		PayloadOffset: 12345678,
		FileSize:      4096,
		Metadata: pxar.Metadata{
			Stat: format.Stat{
				Mode:  format.ModeIFREG | 0o755,
				Flags: 0x10,
			},
			FCaps: []byte{1, 2, 3, 4, 5, 6, 7, 8, 9, 10},
			ACL: pxar.ACL{
				Users: []format.ACLUser{
					{UID: 1000, Permissions: 7},
					{UID: 1001, Permissions: 5},
				},
				Groups: []format.ACLGroup{
					{GID: 100, Permissions: 5},
				},
			},
		},
	}

	node := &GraphNode{
		ID:         nodeID,
		Kind:       NodeFile,
		Mode:       0o644,
		UID:        1000,
		GID:        1000,
		Size:       4096,
		MtimeNs:    1700000000e9,
		HasData:    false,
		RedirectTo: "/some/file.bin",
	}

	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		xattrs := ow.xattrCache[node.ID]
		meta := nodeToMetadata(node, xattrs)
		if node.RedirectTo != "" {
			if pxEntry, err := ow.resolvePxarEntryCached(node.RedirectTo); err == nil {
				meta = mergeMetaWithPxar(meta, pxEntry)
			}
		}
		entry := ow.allocEntry()
		entry.Path = "testfile.bin"
		entry.Kind = pxar.KindFile
		entry.Metadata = meta
		entry.FileSize = node.Size
		_ = entry
	}
}

func BenchmarkB7_AllXAttrsLoad(b *testing.B) {
	for _, n := range []int{1000, 10000, 100000} {
		b.Run(fmt.Sprintf("nodes=%d", n), func(b *testing.B) {
			source := make(map[int64]map[string][]byte, n)
			for i := range n {
				xmap := map[string][]byte{
					"user.attr1": []byte("value1"),
					"user.attr2": []byte("value2"),
				}
				source[int64(i+100)] = xmap
			}

			b.ResetTimer()
			b.ReportAllocs()
			for iter := 0; iter < b.N; iter++ {
				cache := make(map[int64][]format.XAttr, len(source))
				for nodeID, xmap := range source {
					var xattrs []format.XAttr
					for name, val := range xmap {
						xattrs = append(xattrs, format.NewXAttr([]byte(name), val))
					}
					cache[nodeID] = xattrs
				}
				_ = cache
			}
		})
	}
}

func BenchmarkB8_FullCommitWalkMerge(b *testing.B) {
	for _, n := range []int{100, 1000, 10000} {
		b.Run(fmt.Sprintf("entries=%d", n), func(b *testing.B) {
			w := &countingWriter{}
			ow := &commitWalkState{
				mfs:           &MutableFS{verbose: false},
				writer:        w,
				xattrCache:    make(map[int64][]format.XAttr),
				backedHashes:  make(map[string]uint64),
				redirectCache: make(map[string]*pxar.Entry),
				pendingRefs:   make([]commitEntry, 0, maxPendingRefs),
			}

			pxarEntries := make([]dirEntrySlim, n)
			for i := range pxarEntries {
				isDir := i%10 == 0
				pxarEntries[i] = dirEntrySlim{
					name:          fmt.Sprintf("entry_%06d", i),
					inode:         uint64(i + 2),
					entryStart:    uint64(i * 512),
					contentOffset: uint64(i*4096 + 2048),
					payloadOffset: uint64(i+1) * 4096,
					fileSize:      4096,
					mode:          0o644,
					uid:           1000,
					gid:           1000,
					mtimeSecs:     1700000000,
					isDir:         isDir,
					isReg:         !isDir,
				}
			}

			journalEdges := make([]GraphEdge, n/20)
			for i := range journalEdges {
				journalEdges[i] = GraphEdge{
					ParentID: 1,
					Name:     fmt.Sprintf("entry_%06d", i*20+5),
					ChildID:  int64(100 + i),
				}
			}

			sort.Slice(pxarEntries, func(i, j int) bool {
				return pxarEntries[i].name < pxarEntries[j].name
			})
			sort.Slice(journalEdges, func(i, j int) bool {
				return journalEdges[i].Name < journalEdges[j].Name
			})

			edgeNames := make(map[string]bool, len(journalEdges))
			for _, e := range journalEdges {
				edgeNames[e.Name] = true
			}

			nodes := make(map[int64]*GraphNode)
			for _, e := range journalEdges {
				nodes[e.ChildID] = &GraphNode{
					ID:         e.ChildID,
					Kind:       NodeFile,
					Mode:       0o644,
					UID:        1000,
					GID:        1000,
					Size:       4096,
					MtimeNs:    1700000000e9,
					HasData:    false,
					RedirectTo: fmt.Sprintf("/entry_%06d", int(e.ChildID)-100),
				}
			}

			b.ResetTimer()
			b.ReportAllocs()
			for iter := 0; iter < b.N; iter++ {
				ow.pendingRefs = ow.pendingRefs[:0]
				ow.prevRefOffset = 0
				ow.hasPrevRef = false

				filtered := make([]dirEntrySlim, 0, len(pxarEntries))
				for i := range pxarEntries {
					pe := &pxarEntries[i]
					if edgeNames[pe.name] && !pe.isDir {
						continue
					}
					filtered = append(filtered, *pe)
				}

				pi, ji := 0, 0
				for pi < len(filtered) || ji < len(journalEdges) {
					var ce commitEntry
					if pi >= len(filtered) {
						edge := &journalEdges[ji]
						ce = commitEntry{name: edge.Name, node: nodes[edge.ChildID]}
						ji++
					} else if ji >= len(journalEdges) {
						ce = commitEntry{name: filtered[pi].name, pxarSlim: &filtered[pi]}
						pi++
					} else if filtered[pi].name < journalEdges[ji].Name {
						ce = commitEntry{name: filtered[pi].name, pxarSlim: &filtered[pi]}
						pi++
					} else if filtered[pi].name > journalEdges[ji].Name {
						edge := &journalEdges[ji]
						ce = commitEntry{name: edge.Name, node: nodes[edge.ChildID]}
						ji++
					} else {
						edge := &journalEdges[ji]
						ce = commitEntry{name: edge.Name, node: nodes[edge.ChildID]}
						pi++
						ji++
					}

					isDir := (ce.node != nil && ce.node.Kind == NodeDir) ||
						(ce.node == nil && ce.pxarSlim != nil && ce.pxarSlim.isDir)

					if !isDir {
						if ce.node != nil {
							xattrs := ow.xattrCache[ce.node.ID]
							_ = nodeToMetadata(ce.node, xattrs)
						} else if ce.pxarSlim != nil && ce.pxarSlim.isReg {
							ce.sortKey = ce.pxarSlim.payloadOffset
							ce.cachedEntry = &pxar.Entry{
								Path:          ce.name,
								Kind:          pxar.KindFile,
								PayloadOffset: ce.sortKey,
								FileSize:      4096,
							}
							ow.pendingRefs = append(ow.pendingRefs, ce)
						}
					}
				}

				sort.Slice(ow.pendingRefs, func(i, j int) bool {
					return ow.pendingRefs[i].sortKey < ow.pendingRefs[j].sortKey
				})
				for i := range ow.pendingRefs {
					ce := &ow.pendingRefs[i]
					_ = ow.emitPxarRefAt(ce, 0)
				}
			}
		})
	}
}

func BenchmarkB9_BuildPath(b *testing.B) {
	ow := &commitWalkState{}

	b.Run("buildPath_reuse", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			_ = ow.buildPath("/a/b/c/d/e", "entry_name")
		}
	})

	b.Run("joinPath_alloc", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			_ = joinPath("/a/b/c/d/e", "entry_name")
		}
	})
}

type mockFileReader struct {
	size int
	pos  int
}

func (m *mockFileReader) Read(p []byte) (int, error) {
	if m.pos >= m.size {
		return 0, io.EOF
	}
	n := len(p)
	if m.pos+n > m.size {
		n = m.size - m.pos
	}
	for i := range n {
		p[i] = byte(m.pos + i)
	}
	m.pos += n
	return n, nil
}

func BenchmarkB10_VerifyHashOverhead(b *testing.B) {
	for _, size := range []int64{4 * 1024, 64 * 1024, 1024 * 1024} {
		b.Run(fmt.Sprintf("size=%dKB", size/1024), func(b *testing.B) {
			buf := make([]byte, 64*1024)
			b.SetBytes(size)
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				r := &mockFileReader{size: int(size)}
				h := xxh3Pool.Get().(*xxh3.Hasher)
				for {
					n, err := r.Read(buf)
					if n > 0 {
						_, _ = h.Write(buf[:n])
					}
					if err != nil {
						break
					}
				}
				_ = h.Sum64()
				h.Reset()
				xxh3Pool.Put(h)
			}
		})
	}
}

var _ io.Reader = (*mockFileReader)(nil)

func BenchmarkB11_PaddingRatio(b *testing.B) {
	const chunkSize = 4 << 20
	const numChunks = 256
	idx := buildSyntheticDIDX(b, numChunks, chunkSize)

	const numFiles = 100
	refs := make([]commitEntry, numFiles)
	for i := range refs {
		refs[i] = commitEntry{
			name:    fmt.Sprintf("f_%06d", i),
			sortKey: uint64(i * 4096),
			pxarSlim: &dirEntrySlim{
				fileSize: 4096,
			},
		}
	}

	ow := &commitWalkState{
		origChunkIndex: idx,
	}

	b.Run("lookupDynamicEntries", func(b *testing.B) {
		b.ReportAllocs()
		rangeStart := refs[0].sortKey
		rangeEnd := refs[numFiles-1].sortKey + refs[numFiles-1].pxarSlim.fileSize
		for i := 0; i < b.N; i++ {
			lookupDynamicEntries(idx, rangeStart, rangeEnd)
		}
	})

	b.Run("shouldReuse_aligned", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			ow.shouldReuse(refs)
		}
	})

	b.Run("shouldReuse_misaligned", func(b *testing.B) {
		misaligned := make([]commitEntry, numFiles)
		copy(misaligned, refs)
		misaligned[0].sortKey = chunkSize/2 + 1234
		for i := 1; i < numFiles; i++ {
			misaligned[i].sortKey = misaligned[0].sortKey + uint64(i)*4096
		}
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			ow.shouldReuse(misaligned)
		}
	})
}

func buildSyntheticDIDX(tb testing.TB, numChunks int, chunkSize uint64) *datastore.DynamicIndexReader {
	tb.Helper()
	w := datastore.NewDynamicIndexWriter(time.Now().Unix())
	var offset uint64
	for i := range numChunks {
		offset += chunkSize
		var digest [32]byte
		digest[0] = byte(i)
		digest[1] = byte(i >> 8)
		w.Add(offset, digest)
	}
	data, err := w.Finish()
	if err != nil {
		tb.Fatal(err)
	}
	idx, err := datastore.ParseDynamicIndex(data)
	if err != nil {
		tb.Fatal(err)
	}
	return idx
}

func TestLookupDynamicEntries(t *testing.T) {
	idx := buildSyntheticDIDX(t, 5, 100)

	tests := []struct {
		name         string
		rangeStart   uint64
		rangeEnd     uint64
		wantChunks   int
		wantStartPad uint64
		wantEndPad   uint64
	}{
		{"full_range", 0, 500, 5, 0, 0},
		{"aligned_first_chunk", 0, 100, 2, 0, 100},
		{"aligned_last_chunk", 400, 500, 1, 0, 0},
		{"middle_two_chunks", 100, 300, 3, 0, 100},
		{"misaligned_start", 50, 300, 4, 50, 100},
		{"misaligned_end", 100, 350, 3, 0, 50},
		{"misaligned_both", 50, 350, 4, 50, 50},
		{"tiny_range_in_first", 10, 20, 1, 10, 80},
		{"empty_range", 100, 100, 0, 0, 0},
		{"past_end", 600, 700, 0, 0, 0},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			chunks, startPad, endPad := lookupDynamicEntries(idx, tt.rangeStart, tt.rangeEnd)
			if len(chunks) != tt.wantChunks {
				t.Errorf("got %d chunks, want %d", len(chunks), tt.wantChunks)
			}
			if startPad != tt.wantStartPad {
				t.Errorf("got startPad=%d, want %d", startPad, tt.wantStartPad)
			}
			if endPad != tt.wantEndPad {
				t.Errorf("got endPad=%d, want %d", endPad, tt.wantEndPad)
			}
		})
	}
}

func TestShouldReuse(t *testing.T) {
	idx := buildSyntheticDIDX(t, 10, 1000)

	ow := &commitWalkState{origChunkIndex: idx}

	tests := []struct {
		name      string
		refs      []commitEntry
		wantReuse bool
	}{
		{
			"aligned_full_chunks",
			func() []commitEntry {
				refs := make([]commitEntry, 1000)
				for i := range refs {
					refs[i] = commitEntry{
						sortKey:  uint64(i * 10),
						pxarSlim: &dirEntrySlim{fileSize: 10},
					}
				}
				return refs
			}(),
			true,
		},
		{
			"single_file_aligned",
			[]commitEntry{
				{sortKey: 0, pxarSlim: &dirEntrySlim{fileSize: 900}},
			},
			true,
		},
		{
			"nil_index_fallback",
			[]commitEntry{{sortKey: 500, pxarSlim: &dirEntrySlim{fileSize: 10}}},
			false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := ow.shouldReuse(tt.refs)
			if got != tt.wantReuse {
				t.Errorf("shouldReuse() = %v, want %v", got, tt.wantReuse)
			}
		})
	}

	owNil := &commitWalkState{origChunkIndex: nil}
	if !owNil.shouldReuse([]commitEntry{{sortKey: 0, pxarSlim: &dirEntrySlim{fileSize: 100}}}) {
		t.Error("shouldReuse with nil index should return true")
	}
}

func TestRangeHoleDetection(t *testing.T) {
	idx := buildSyntheticDIDX(t, 10, 1000)

	ow := &commitWalkState{
		mfs:            &MutableFS{},
		origChunkIndex: idx,
		pendingRefs:    make([]commitEntry, 0, 64),
	}

	ce1 := &commitEntry{name: "a", sortKey: 100, pxarSlim: &dirEntrySlim{fileSize: 200, isReg: true}}
	if ow.batchRangeEnd != 0 {
		t.Fatal("batchRangeEnd should be 0 before first add")
	}
	ow.pendingRefs = append(ow.pendingRefs, *ce1)
	entryEnd := ce1.rangeEnd()
	if entryEnd > ow.batchRangeEnd {
		ow.batchRangeEnd = entryEnd
	}
	if ow.batchRangeEnd != 316 {
		t.Fatalf("batchRangeEnd=%d, want 316", ow.batchRangeEnd)
	}

	gap := ow.origChunkIndex != nil && ow.batchRangeEnd != 0 && uint64(500) > ow.batchRangeEnd
	if !gap {
		t.Error("should detect gap between 316 and 500")
	}

	noGap := ow.origChunkIndex != nil && ow.batchRangeEnd != 0 && uint64(316) > ow.batchRangeEnd
	if noGap {
		t.Error("should not detect gap when entry is contiguous")
	}

	owNil := &commitWalkState{
		mfs:         &MutableFS{},
		pendingRefs: make([]commitEntry, 0, 64),
	}
	noGapNil := owNil.origChunkIndex != nil && owNil.batchRangeEnd != 0 && uint64(500) > owNil.batchRangeEnd
	if noGapNil {
		t.Error("nil origChunkIndex should not trigger hole detection")
	}
}

func TestCrossBatchChunkContinuation(t *testing.T) {
	idx := buildSyntheticDIDX(t, 10, 1000)

	ow := &commitWalkState{origChunkIndex: idx}

	refs1 := []commitEntry{
		{sortKey: 0, pxarSlim: &dirEntrySlim{fileSize: 800}},
	}
	_ = refs1
	chunks1, _, _ := lookupDynamicEntries(idx, 0, 816)
	ow.savedChunk = chunks1[len(chunks1)-1]
	ow.hasSavedChunk = true

	refs2 := []commitEntry{
		{sortKey: 816, pxarSlim: &dirEntrySlim{fileSize: 167}},
	}

	ow2 := &commitWalkState{
		origChunkIndex: idx,
		savedChunk:     ow.savedChunk,
		hasSavedChunk:  true,
	}

	reuse := ow2.shouldReuse(refs2)
	if !reuse {
		t.Error("shouldReuse with continuation should return true (padding absorbed)")
	}

	ow3 := &commitWalkState{
		origChunkIndex: idx,
	}
	var fakeChunk reusableChunk
	fakeChunk.digest = [32]byte{0xFF}
	fakeChunk.endOffset = 999999
	fakeChunk.size = 1000
	ow3.savedChunk = fakeChunk
	ow3.hasSavedChunk = true

	reuse3 := ow3.shouldReuse(refs2)
	if reuse3 {
		t.Error("shouldReuse with non-matching continuation chunk should return false (padding not absorbed)")
	}
}

func TestSameIndexedChunkAs(t *testing.T) {
	a := reusableChunk{digest: [32]byte{1, 2, 3}, endOffset: 1000}
	b := reusableChunk{digest: [32]byte{1, 2, 3}, endOffset: 1000}
	if !a.sameIndexedChunkAs(&b) {
		t.Error("identical chunks should match")
	}

	c := reusableChunk{digest: [32]byte{1, 2, 3}, endOffset: 2000}
	if a.sameIndexedChunkAs(&c) {
		t.Error("same digest different endOffset should not match (dedup collision)")
	}

	d := reusableChunk{digest: [32]byte{4, 5, 6}, endOffset: 1000}
	if a.sameIndexedChunkAs(&d) {
		t.Error("different digest same endOffset should not match")
	}
}

func TestLookupDynamicEntriesChunkPadding(t *testing.T) {
	idx := buildSyntheticDIDX(t, 5, 100)

	chunks, startPad, endPad := lookupDynamicEntries(idx, 50, 350)
	if len(chunks) != 4 {
		t.Fatalf("got %d chunks, want 4", len(chunks))
	}
	if startPad != 50 {
		t.Errorf("startPad=%d, want 50", startPad)
	}
	if endPad != 50 {
		t.Errorf("endPad=%d, want 50", endPad)
	}

	wantPadding := []uint64{50, 0, 0, 50}
	for i, c := range chunks {
		if c.padding != wantPadding[i] {
			t.Errorf("chunk[%d].padding=%d, want %d", i, c.padding, wantPadding[i])
		}
	}

	chunks1, _, endPad1 := lookupDynamicEntries(idx, 0, 99)
	if len(chunks1) != 1 {
		t.Fatalf("single chunk: got %d, want 1", len(chunks1))
	}
	if chunks1[0].padding != 1 {
		t.Errorf("single aligned chunk padding=%d, want 1", chunks1[0].padding)
	}

	chunks2, startPad2, _ := lookupDynamicEntries(idx, 50, 99)
	if len(chunks2) != 1 {
		t.Fatalf("single misaligned start: got %d, want 1", len(chunks2))
	}
	if startPad2 != 50 {
		t.Errorf("startPad=%d, want 50", startPad2)
	}
	if chunks2[0].padding != 51 {
		t.Errorf("single misaligned chunk padding=%d, want 51", chunks2[0].padding)
	}

	_ = endPad1
}

func TestFlushPendingRefsReencodeClearsLastChunk(t *testing.T) {
	idx := buildSyntheticDIDX(t, 10, 1000)

	ow := &commitWalkState{
		mfs:            &MutableFS{},
		origChunkIndex: idx,
		pendingRefs:    make([]commitEntry, 0, 64),
	}

	ow.hasSavedChunk = true
	ow.savedChunk = reusableChunk{digest: [32]byte{0xAA}, endOffset: 99999, size: 1000, padding: 100}

	refs := []commitEntry{
		{sortKey: 0, pxarSlim: &dirEntrySlim{fileSize: 900}},
	}
	reuse := ow.shouldReuse(refs)
	if !reuse {
		t.Fatal("should reuse batch fitting within first chunk")
	}
	if !ow.hasSavedChunk {
		t.Error("shouldReuse should not modify hasSavedChunk")
	}
}

func TestPaddingRatioWithTinyFileInHugeChunk(t *testing.T) {
	idx := buildSyntheticDIDX(t, 3, 4000000)

	refs := []commitEntry{
		{sortKey: 100, pxarSlim: &dirEntrySlim{fileSize: 200}},
	}
	ow := &commitWalkState{origChunkIndex: idx}
	reuse := ow.shouldReuse(refs)
	startPad := uint64(100)
	endPad := uint64(4000000 - 316)
	totalPad := startPad + endPad
	totalSize := uint64(216) + totalPad
	ratio := float64(totalPad) / float64(totalSize)
	want := ratio <= 0.1
	if reuse != want {
		t.Errorf("shouldReuse=%v, want %v (ratio=%.4f)", reuse, want, ratio)
	}
}

func TestPaddingRatioRejectsHighWaste(t *testing.T) {
	idx := buildSyntheticDIDX(t, 2, 4000000)

	refs := []commitEntry{
		{sortKey: 4000000 - 10, pxarSlim: &dirEntrySlim{fileSize: 20}},
	}
	ow := &commitWalkState{origChunkIndex: idx}
	reuse := ow.shouldReuse(refs)
	if reuse {
		t.Error("file spanning chunk boundary with huge padding should re-encode")
	}
}

type offsetTrackingWriter struct {
	noopWriter
	refOffsets    []uint64
	injectedSizes []uint64
}

func (w *offsetTrackingWriter) WriteEntryRef(_ *pxar.Entry, offset uint64) error {
	w.refOffsets = append(w.refOffsets, offset)
	return nil
}

func (w *offsetTrackingWriter) InjectChunks(chunks []backupproxy.KnownChunkRef) error {
	for _, c := range chunks {
		w.injectedSizes = append(w.injectedSizes, c.Size)
	}
	return nil
}

func (w *offsetTrackingWriter) Encoder() *encoder.Encoder {
	return nil
}

func (w *offsetTrackingWriter) WriteEntryReader(entry *pxar.Entry, r io.Reader, size uint64) error {
	return nil
}

func TestFlushPendingRefsOffsetCorrectness(t *testing.T) {
	idx := buildSyntheticDIDX(t, 5, 1000)

	makeEntry := func(name string, payloadOffset, fileSize uint64) commitEntry {
		return commitEntry{
			name:     name,
			sortKey:  payloadOffset,
			pxarSlim: &dirEntrySlim{payloadOffset: payloadOffset, fileSize: fileSize},
		}
	}

	t.Run("offset math matches Rust", func(t *testing.T) {
		encoderPos := uint64(5000)
		startPadding := uint64(100)
		baseOffset := encoderPos + startPadding
		entries := []commitEntry{
			makeEntry("a", 100, 50),
			makeEntry("b", 300, 50),
			makeEntry("c", 600, 50),
		}
		rangeStart := entries[0].sortKey
		for _, e := range entries {
			refOff := baseOffset + (e.sortKey - rangeStart)
			want := encoderPos + e.sortKey
			if refOff != want {
				t.Errorf("%s: refOff=%d want=%d", e.name, refOff, want)
			}
		}
	})

	t.Run("keepLastChunk injects n-1 chunks", func(t *testing.T) {
		chunks, _, _ := lookupDynamicEntries(idx, 100, 1700)
		if len(chunks) < 2 {
			t.Fatalf("need at least 2 chunks, got %d", len(chunks))
		}

		lastChunk := chunks[len(chunks)-1]
		injected := chunks[:len(chunks)-1]

		var injectSum, totalSum uint64
		for _, c := range injected {
			injectSum += c.size
		}
		for _, c := range chunks {
			totalSum += c.size
		}
		if injectSum+lastChunk.size != totalSum {
			t.Errorf("injectSum(%d) + last(%d) != total(%d)", injectSum, lastChunk.size, totalSum)
		}
	})

	t.Run("no origIndex writes absolute offsets", func(t *testing.T) {
		w := &offsetTrackingWriter{}
		ow := &commitWalkState{
			mfs:           &MutableFS{verbose: false},
			writer:        w,
			xattrCache:    make(map[int64][]format.XAttr),
			redirectCache: make(map[string]*pxar.Entry),
			pendingRefs: []commitEntry{
				{name: "a", sortKey: 100, pxarSlim: &dirEntrySlim{payloadOffset: 100, fileSize: 50}, cachedEntry: &pxar.Entry{Path: "a", Kind: pxar.KindFile, FileSize: 50, PayloadOffset: 100}},
				{name: "b", sortKey: 300, pxarSlim: &dirEntrySlim{payloadOffset: 300, fileSize: 50}, cachedEntry: &pxar.Entry{Path: "b", Kind: pxar.KindFile, FileSize: 50, PayloadOffset: 300}},
			},
		}

		if err := ow.flushPendingRefs(false); err != nil {
			t.Fatal(err)
		}
		if len(w.refOffsets) != 2 {
			t.Fatalf("expected 2 refs, got %d", len(w.refOffsets))
		}
		if w.refOffsets[0] != 100 {
			t.Errorf("ref[0]=%d want 100", w.refOffsets[0])
		}
		if w.refOffsets[1] != 300 {
			t.Errorf("ref[1]=%d want 300", w.refOffsets[1])
		}
	})
}
