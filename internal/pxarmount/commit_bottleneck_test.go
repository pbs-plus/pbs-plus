package pxarmount

import (
	"fmt"
	"io"
	"sort"
	"sync"
	"testing"
	"time"

	pxar "github.com/pbs-plus/pxar"
	"github.com/pbs-plus/pxar/datastore"
	"github.com/pbs-plus/pxar/format"
	"github.com/zeebo/xxh3"
)

// ============================================================================
// Bottleneck benchmarks: measuring the specific overhead identified in the
// pxar-mount commit path vs the Rust standard backup path.
//
// B1: Per-file WriteEntryRef overhead (no chunk coalescing)
// B2: Double decode per pxar file (readDirRaw + ReadEntryAt)
// B3: Global readerMu mutex contention
// B4: Per-redirect Lookup + ReadEntryAt (resolvePxarEntryCached)
// B5: Redundant verify hash pass
// B6: AllXAttrs upfront load
// B7: Pending refs sort + flush batch overhead
// ============================================================================

// ---------------------------------------------------------------------------
// B1: Per-file WriteEntryRef overhead
// ---------------------------------------------------------------------------

// countingWriter tracks WriteEntryRef calls.
type countingWriter struct {
	noopWriter
	refCalls int
}

func (c *countingWriter) WriteEntryRef(_ *pxar.Entry, _ uint64) error {
	c.refCalls++
	return nil
}

// BenchmarkB1_PerFileWriteEntryRef measures the per-file overhead of emitting
// one WriteEntryRef per unchanged file. This is the dominant bottleneck:
// 5M files × (alloc + monotonic check + metadata write) vs Rust's ~thousands
// of chunk injections.
//
// The mock writer has zero I/O cost, so this measures pure CPU/alloc overhead
// of the Go-level call path.
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
			ow.hasLastRefPayload = false

			// Pre-build entries with monotonically increasing offsets.
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
				ow.lastRefPayloadOffset = 0
				ow.hasLastRefPayload = false

				// Add all refs (simulates addToPendingRefs for all pxar files).
				ow.pendingRefs = append(ow.pendingRefs, refs...)

				// Flush: sort + emit one WriteEntryRef per file.
				sort.Slice(ow.pendingRefs, func(i, j int) bool {
					return ow.pendingRefs[i].sortKey < ow.pendingRefs[j].sortKey
				})
				for i := range ow.pendingRefs {
					ce := &ow.pendingRefs[i]
					_ = ow.emitPxarRef(ce, "/test")
				}
			}
		})
	}
}

// BenchmarkB1_ChunkCoalesced measures the TARGET cost if we coalesced files
// into chunk ranges and injected chunks instead. This uses a simulated
// "injectChunk" that processes a batch of contiguous files as one operation.
func BenchmarkB1_ChunkCoalesced(b *testing.B) {
	for _, n := range []int{1000, 10000, 100000} {
		b.Run(fmt.Sprintf("files=%d", n), func(b *testing.B) {
			// Simulate chunk coalescing: group files into 4MB chunks.
			chunkSize := uint64(4 << 20) // 4 MB
			files := make([]commitEntry, n)
			for i := range files {
				files[i] = commitEntry{
					name:    fmt.Sprintf("file_%06d.bin", i),
					sortKey: uint64(i+1) * 4096,
				}
			}

			// Simulate coalescing: one operation per chunk range.
			type chunkRange struct {
				startIdx, endIdx int
			}

			b.ResetTimer()
			b.ReportAllocs()
			for iter := 0; iter < b.N; iter++ {
				// Coalesce into chunk ranges.
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

// ---------------------------------------------------------------------------
// B2: Double decode per pxar file
// ---------------------------------------------------------------------------

// BenchmarkB2_DoubleDecodeOverhead measures the cost of doing TWO decode passes
// per pxar file: one in readDirRaw (ListDirectory minimal) and one in
// emitAlphabeticalPxar (ReadEntryAt full). The second pass is the "double decode"
// overhead unique to pxar-mount.
func BenchmarkB2_DoubleDecodeOverhead(b *testing.B) {
	// This benchmark measures the per-file overhead of constructing a
	// dirEntrySlim from a pxar.Entry (simulating readDirRaw decode) PLUS
	// the subsequent ReadEntryAt overhead.
	//
	// We can't use the actual archive reader in a unit test, so we measure
	// the struct conversion + allocation overhead that represents the
	// decode cost per file.

	for _, n := range []int{1000, 10000} {
		b.Run(fmt.Sprintf("files=%d", n), func(b *testing.B) {
			// Pass 1: readDirRaw — creates dirEntrySlim from each entry.
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
				// Pass 1: readDirRaw — create slim entries.
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

				// Pass 2: emitAlphabeticalPxar — ReadEntryAt + cache.
				// Simulate the cachedEntry assignment (pointer copy).
				cached := make([]*pxar.Entry, n)
				for i, s := range slims {
					if s.isReg {
						// Simulates ReadEntryAt — copies the entry.
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

// BenchmarkB2_SingleDecodeOverhead measures the same flow but with only one
// decode pass (the theoretical optimum if we could avoid the double decode).
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
				// Single pass: extract what we need directly.
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

// ---------------------------------------------------------------------------
// B3: Global readerMu mutex contention
// ---------------------------------------------------------------------------

// BenchmarkB3_MutexContention measures the overhead of acquiring readerMu
// for every archive operation. With 20+ lock/unlock pairs per directory in
// the hot path, this adds up on large archives.
func BenchmarkB3_MutexContention(b *testing.B) {
	for _, n := range []int{1000, 10000} {
		b.Run(fmt.Sprintf("locks=%d", n), func(b *testing.B) {
			var mu sync.Mutex
			// Simulate the critical section: a no-op read.
			// In production, this holds the lock during ListDirectory/ReadEntryAt.

			b.ResetTimer()
			b.ReportAllocs()
			for iter := 0; iter < b.N; iter++ {
				for i := range n {
					mu.Lock()
					// Critical section: ~50ns of archive decode work
					_ = i
					mu.Unlock()
				}
			}
		})
	}
}

// BenchmarkB3_NoMutex measures the same workload without the mutex.
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

// BenchmarkB3_RWMutex measures using RWMutex instead (potential fix).
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

// ---------------------------------------------------------------------------
// B4: Per-redirect resolvePxarEntryCached overhead
// ---------------------------------------------------------------------------

// BenchmarkB4_RedirectResolveHit measures the cache hit path (map lookup only).
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

// BenchmarkB4_RedirectResolveMiss measures the cache miss path which would
// trigger Lookup + ReadEntryAt. We measure the allocation overhead of the
// mergeMetaWithPxar call that follows every resolve.
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
		// Cache hit + merge (happens per journal redirect file).
		entry := cache["/deep/nested/path/file_005000.bin"]
		_ = mergeMetaWithPxar(journalMeta, entry)
	}
}

// ---------------------------------------------------------------------------
// B5: Pending refs sort overhead (per directory)
// ---------------------------------------------------------------------------

// BenchmarkB5_PendingRefsSort measures the sort cost of the pendingRefs batch.
// With maxPendingRefs=4096, this is called when the batch fills or at dir boundary.
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

		// Nearly-sorted case: insertion sort should be O(n) here.
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

		// Worst-case reverse: insertion sort falls back to sort.Slice.
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

// ---------------------------------------------------------------------------
// B6: Per-file metadata construction overhead
// ---------------------------------------------------------------------------

// BenchmarkB6_MetadataConstruction measures the full per-file path from
// journal node → nodeToMetadata → mergeMetaWithPxar → allocEntry → WriteEntryRef.
// This represents the complete per-file CPU cost in the commit walk.
func BenchmarkB6_MetadataConstruction(b *testing.B) {
	ow := &commitWalkState{
		mfs:           &MutableFS{verbose: false},
		xattrCache:    make(map[int64][]format.XAttr),
		redirectCache: make(map[string]*pxar.Entry),
	}

	// Set up xattr cache for node.
	const nodeID int64 = 42
	ow.xattrCache[nodeID] = []format.XAttr{
		format.NewXAttr([]byte("user.attr1"), []byte("value1")),
		format.NewXAttr([]byte("user.attr2"), []byte("value2")),
	}

	// Set up redirect cache with a pxar entry that has fcaps/acl.
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
		// Full per-file path: nodeToMetadata + resolve + merge + alloc + fields.
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

// ---------------------------------------------------------------------------
// B7: AllXAttrs upfront load simulation
// ---------------------------------------------------------------------------

// BenchmarkB7_AllXAttrsLoad measures the cost of building a large
// map[int64][]format.XAttr from all journal xattrs.
func BenchmarkB7_AllXAttrsLoad(b *testing.B) {
	for _, n := range []int{1000, 10000, 100000} {
		b.Run(fmt.Sprintf("nodes=%d", n), func(b *testing.B) {
			// Simulate journal's AllXAttrs output.
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

// ---------------------------------------------------------------------------
// B8: Full commitWalk merge+dispatch simulation (integration)
// ---------------------------------------------------------------------------

// BenchmarkB8_FullCommitWalkMerge measures the complete per-directory merge
// loop: filter pxar + sort + two-pointer merge + addToPendingRefs + flush.
// This is the hot loop that runs for every directory in the archive.
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

			// Build pxar entries: 90% files, 10% dirs.
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

			// 5% journal overlay entries.
			journalEdges := make([]GraphEdge, n/20)
			for i := range journalEdges {
				journalEdges[i] = GraphEdge{
					ParentID: 1,
					Name:     fmt.Sprintf("entry_%06d", i*20+5), // sparse overlay
					ChildID:  int64(100 + i),
				}
			}

			// Pre-sort for merge.
			sort.Slice(pxarEntries, func(i, j int) bool {
				return pxarEntries[i].name < pxarEntries[j].name
			})
			sort.Slice(journalEdges, func(i, j int) bool {
				return journalEdges[i].Name < journalEdges[j].Name
			})

			// Pre-build edgeNames.
			edgeNames := make(map[string]bool, len(journalEdges))
			for _, e := range journalEdges {
				edgeNames[e.Name] = true
			}

			// Pre-build journal nodes.
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
				ow.lastRefPayloadOffset = 0
				ow.hasLastRefPayload = false

				// Filter pxar.
				filtered := make([]dirEntrySlim, 0, len(pxarEntries))
				for i := range pxarEntries {
					pe := &pxarEntries[i]
					if edgeNames[pe.name] && !pe.isDir {
						continue
					}
					filtered = append(filtered, *pe)
				}

				// Two-pointer merge + dispatch.
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

					// Dispatch.
					isDir := (ce.node != nil && ce.node.Kind == NodeDir) ||
						(ce.node == nil && ce.pxarSlim != nil && ce.pxarSlim.isDir)

					if !isDir {
						if ce.node != nil {
							// Journal file — compute metadata.
							xattrs := ow.xattrCache[ce.node.ID]
							_ = nodeToMetadata(ce.node, xattrs)
							// Would do resolvePxarEntryCached + merge + emitBackedFile/WriteEntry
						} else if ce.pxarSlim != nil && ce.pxarSlim.isReg {
							ce.sortKey = ce.pxarSlim.payloadOffset
							// Simulate cachedEntry assignment.
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

				// Flush: sort + emit refs.
				sort.Slice(ow.pendingRefs, func(i, j int) bool {
					return ow.pendingRefs[i].sortKey < ow.pendingRefs[j].sortKey
				})
				for i := range ow.pendingRefs {
					ce := &ow.pendingRefs[i]
					_ = ow.emitPxarRef(ce, "/test")
				}
			}
		})
	}
}

// ---------------------------------------------------------------------------
// B9: BuildPath / path string concatenation overhead
// ---------------------------------------------------------------------------

// BenchmarkB9_BuildPath measures the buildPath method vs naive joinPath.
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

// ---------------------------------------------------------------------------
// B10: Verify hash pass overhead (double hashing)
// ---------------------------------------------------------------------------

// mockFileReader simulates reading a file for hashing.
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

// BenchmarkB10_VerifyHashOverhead measures the cost of re-hashing a file
// that was already hashed during upload (TeeReader). This represents the
// redundant verification pass.
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

// Ensure interface compliance at compile time.
var _ io.Reader = (*mockFileReader)(nil)

// ---------------------------------------------------------------------------
// B11: Padding ratio heuristic — lookupDynamicEntries + shouldReuse
// ---------------------------------------------------------------------------

// BenchmarkB11_PaddingRatio measures the cost of the padding ratio calculation
// that decides between PAYLOAD_REF reuse and re-encoding.
func BenchmarkB11_PaddingRatio(b *testing.B) {
	// Build a synthetic DIDX with 256 chunks of ~4MB each (~1GB total).
	const chunkSize = 4 << 20
	const numChunks = 256
	idx := buildSyntheticDIDX(b, numChunks, chunkSize)

	// Build a batch of pending refs that span 100 contiguous files.
	const numFiles = 100
	refs := make([]commitEntry, numFiles)
	for i := range refs {
		refs[i] = commitEntry{
			name:    fmt.Sprintf("f_%06d", i),
			sortKey: uint64(i * 4096), // 4KB files, contiguous
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
		// Files aligned to chunk boundaries → low padding → reuse.
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			ow.shouldReuse(refs)
		}
	})

	b.Run("shouldReuse_misaligned", func(b *testing.B) {
		// Shift range to create misalignment → potentially high padding.
		misaligned := make([]commitEntry, numFiles)
		copy(misaligned, refs)
		// Start in the middle of a chunk — creates start padding.
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

// buildSyntheticDIDX creates a DynamicIndexReader with synthetic chunk entries.
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
	// 5 chunks of 100 bytes each, total 500 bytes.
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
		{"aligned_first_chunk", 0, 100, 1, 0, 0},
		{"aligned_last_chunk", 400, 500, 1, 0, 0},
		{"middle_two_chunks", 100, 300, 2, 0, 0},
		{"misaligned_start", 50, 300, 3, 50, 0},
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
	// 10 chunks of 1000 bytes each, total 10000 bytes.
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
				// 1000 files × 10 bytes = 10000 bytes, aligned to chunk boundaries.
				refs := make([]commitEntry, 1000)
				for i := range refs {
					refs[i] = commitEntry{
						sortKey:  uint64(i * 10),
						pxarSlim: &dirEntrySlim{fileSize: 10},
					}
				}
				return refs
			}(),
			true, // no padding → reuse
		},
		{
			"single_file_aligned",
			[]commitEntry{
				{sortKey: 0, pxarSlim: &dirEntrySlim{fileSize: 1000}},
			},
			true, // exactly one chunk → no padding
		},
		{
			"nil_index_fallback",
			[]commitEntry{{sortKey: 500, pxarSlim: &dirEntrySlim{fileSize: 10}}},
			false, // huge padding (500 start + 490 end) → re-encode
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

	// Verify the nil index fallback.
	owNil := &commitWalkState{origChunkIndex: nil}
	if !owNil.shouldReuse([]commitEntry{{sortKey: 0, pxarSlim: &dirEntrySlim{fileSize: 100}}}) {
		t.Error("shouldReuse with nil index should return true")
	}
}
