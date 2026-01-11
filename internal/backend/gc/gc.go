//go:build linux

package gc

import (
	"bufio"
	"bytes"
	"context"
	"encoding/binary"
	"encoding/hex"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"time"

	"github.com/cockroachdb/pebble"
	"github.com/cockroachdb/pebble/bloom"
	"github.com/puzpuzpuz/xsync/v4"
	"golang.org/x/sync/errgroup"
)

const (
	HeaderSize         = 4096
	BatchSize          = 100000
	MaxWorkers         = 32
	MaxSweepWorkers    = 128
	DefaultGracePeriod = 24 * 3600
)

var (
	MagicFixed   = []byte{47, 127, 65, 237, 145, 253, 15, 205}
	MagicDynamic = []byte{28, 145, 78, 165, 25, 186, 179, 205}
	prefixMark   = []byte("m:")
	prefixIndex  = []byte("i:")
)

type GCStatus struct {
	ProcessedIndices *xsync.Counter
	SkippedIndices   *xsync.Counter
	TotalChunks      *xsync.Counter
	RemovedChunks    *xsync.Counter
	RemovedBytes     *xsync.Counter
	DiskChunks       *xsync.Counter
	DiskBytes        *xsync.Counter
}

func NewGCStatus() *GCStatus {
	return &GCStatus{
		ProcessedIndices: xsync.NewCounter(),
		SkippedIndices:   xsync.NewCounter(),
		TotalChunks:      xsync.NewCounter(),
		RemovedChunks:    xsync.NewCounter(),
		RemovedBytes:     xsync.NewCounter(),
		DiskChunks:       xsync.NewCounter(),
		DiskBytes:        xsync.NewCounter(),
	}
}

type MarkStore struct {
	db    *pebble.DB
	cache *pebble.Cache
}

func NewMarkStore(path string) (*MarkStore, error) {
	cache := pebble.NewCache(1024 << 20)
	opts := &pebble.Options{
		Cache:        cache,
		DisableWAL:   true,
		BytesPerSync: 4 * 1024 * 1024,
		Filters: map[string]pebble.FilterPolicy{
			"rocksdb.BuiltinBloomFilter": bloom.FilterPolicy(10),
		},
		MemTableSize:          64 << 20,
		L0CompactionThreshold: 2,
		LBaseMaxBytes:         512 << 20,
		MaxOpenFiles:          10000,
	}

	db, err := pebble.Open(filepath.Join(path, "gc_pebble"), opts)
	if err != nil {
		cache.Unref()
		return nil, err
	}
	return &MarkStore{db: db, cache: cache}, nil
}

func (s *MarkStore) Close() {
	_ = s.db.Close()
	s.cache.Unref()
}

func (s *MarkStore) ShouldProcessIndex(path string, info fs.FileInfo) bool {
	key := append(prefixIndex, []byte(path)...)
	val, closer, err := s.db.Get(key)
	if err != nil {
		return true
	}
	defer closer.Close()
	if len(val) < 16 {
		return true
	}
	return int64(binary.LittleEndian.Uint64(val[0:8])) != info.ModTime().Unix() ||
		int64(binary.LittleEndian.Uint64(val[8:16])) != info.Size()
}

func (s *MarkStore) MarkIndexProcessed(path string, info fs.FileInfo) error {
	key := append(prefixIndex, []byte(path)...)
	buf := make([]byte, 16)
	binary.LittleEndian.PutUint64(buf[0:8], uint64(info.ModTime().Unix()))
	binary.LittleEndian.PutUint64(buf[8:16], uint64(info.Size()))
	return s.db.Set(key, buf, pebble.NoSync)
}

func (s *MarkStore) BatchMark(batch [][32]byte, ts int64) error {
	tsBuf := make([]byte, 8)
	binary.LittleEndian.PutUint64(tsBuf, uint64(ts))
	b := s.db.NewBatch()
	for _, digest := range batch {
		key := append(prefixMark, digest[:]...)
		_ = b.Set(key, tsBuf, nil)
	}
	return b.Commit(pebble.NoSync)
}

func (s *MarkStore) IsMarked(digest []byte, minTs int64) bool {
	key := append(prefixMark, digest...)
	val, closer, err := s.db.Get(key)
	if err != nil {
		return false
	}
	defer closer.Close()
	return int64(binary.LittleEndian.Uint64(val)) >= minTs
}

func RunGC(ctx context.Context, datastorePath string) (*GCStatus, error) {
	lockFile, err := AcquireProxmoxSharedLock(datastorePath)
	if err != nil {
		return nil, err
	}
	defer lockFile.Close()

	store, err := NewMarkStore(datastorePath)
	if err != nil {
		return nil, err
	}
	defer store.Close()

	// 2. Get the Oldest Writer from kernel locks
	oldestWriter, _ := GetOldestWriter(datastorePath)

	// 3. Fallback to current time if no writers are found
	if oldestWriter == 0 {
		oldestWriter = time.Now().Unix()
	}

	// 4. Calculate final cutoff
	// Use the minimum of (Oldest Worker Task) and (Oldest Kernel Lock)
	// Plus the 24h grace period and safety buffer.
	gcStart := time.Now().Unix()
	cutoff := oldestWriter - DefaultGracePeriod - 300

	status := NewGCStatus()

	g, ctx := errgroup.WithContext(ctx)
	paths := make(chan string, 10000)

	g.Go(func() error {
		defer close(paths)
		return filepath.WalkDir(datastorePath, func(path string, d fs.DirEntry, err error) error {
			if err != nil || d.IsDir() {
				return nil
			}
			ext := filepath.Ext(path)
			if ext == ".fidx" || ext == ".didx" {
				select {
				case paths <- path:
				case <-ctx.Done():
					return ctx.Err()
				}
			}
			return nil
		})
	})

	for i := 0; i < MaxWorkers; i++ {
		g.Go(func() error {
			batch := make([][32]byte, 0, BatchSize)
			for path := range paths {
				info, err := os.Stat(path)
				if err != nil {
					continue
				}
				if !store.ShouldProcessIndex(path, info) {
					status.SkippedIndices.Inc()
					continue
				}

				err = processIndex(path, func(d [32]byte) error {
					batch = append(batch, d)
					if len(batch) >= BatchSize {
						if err := store.BatchMark(batch, gcStart); err != nil {
							return err
						}
						status.TotalChunks.Add(int64(len(batch)))
						batch = batch[:0]
					}
					return nil
				})
				if err != nil {
					return err
				}

				if len(batch) > 0 {
					_ = store.BatchMark(batch, gcStart)
					status.TotalChunks.Add(int64(len(batch)))
					batch = batch[:0]
				}
				_ = store.MarkIndexProcessed(path, info)
				status.ProcessedIndices.Inc()
			}
			return nil
		})
	}

	if err := g.Wait(); err != nil {
		return nil, err
	}
	_ = store.db.Flush()

	// --- Phase 2: Sweep ---
	chunkBase := filepath.Join(datastorePath, ".chunks")
	subdirs, _ := os.ReadDir(chunkBase)

	dirQueue := xsync.NewMPMCQueue[string](len(subdirs) + 1)
	for _, sd := range subdirs {
		if sd.IsDir() {
			dirQueue.TryEnqueue(sd.Name())
		}
	}

	sweepG, _ := errgroup.WithContext(ctx)
	for i := 0; i < MaxSweepWorkers; i++ {
		sweepG.Go(func() error {
			for {
				subdir, ok := dirQueue.TryDequeue()
				if !ok {
					return nil
				}

				fullPath := filepath.Join(chunkBase, subdir)
				_ = filepath.WalkDir(fullPath, func(p string, d fs.DirEntry, err error) error {
					if err != nil || d.IsDir() {
						return nil
					}

					name := d.Name()
					if len(name) != 64 {

						return nil
					}
					digest, _ := hex.DecodeString(name)

					if store.IsMarked(digest, gcStart) {
						if info, err := d.Info(); err == nil {
							status.DiskChunks.Inc()
							status.DiskBytes.Add(info.Size())
						}
						return nil
					}

					info, err := d.Info()
					if err != nil {
						return nil
					}

					if info.ModTime().Unix() < cutoff {
						if os.Remove(p) == nil {
							status.RemovedChunks.Inc()
							status.RemovedBytes.Add(info.Size())
						}
					} else {
						status.DiskChunks.Inc()
						status.DiskBytes.Add(info.Size())
					}
					return nil
				})
			}
		})
	}

	return status, sweepG.Wait()
}

func processIndex(path string, cb func([32]byte) error) error {
	f, err := os.Open(path)
	if err != nil {
		return err
	}
	defer f.Close()

	info, err := f.Stat()
	if err != nil {
		return err
	}

	header := make([]byte, 8)
	if _, err := f.ReadAt(header, 0); err != nil {
		return err
	}

	var entrySize, hashOffset int
	if bytes.Equal(header, MagicFixed) {
		entrySize, hashOffset = 32, 0
	} else if bytes.Equal(header, MagicDynamic) {
		entrySize, hashOffset = 40, 8
	} else {
		return nil // Not an index file
	}

	payloadSize := info.Size() - HeaderSize
	if payloadSize < 0 {
		return nil
	}
	count := payloadSize / int64(entrySize)

	if _, err := f.Seek(HeaderSize, 0); err != nil {
		return err
	}

	reader := bufio.NewReaderSize(f, 1024*1024)
	buf := make([]byte, entrySize)

	for i := int64(0); i < count; i++ {
		_, err := io.ReadFull(reader, buf)
		if err != nil {
			return err
		}

		var digest [32]byte
		copy(digest[:], buf[hashOffset:hashOffset+32])
		if err := cb(digest); err != nil {
			return err
		}
	}
	return nil
}
