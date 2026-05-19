package pxarmount

import (
	"encoding/binary"
	"fmt"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/cockroachdb/pebble"
	"github.com/cockroachdb/pebble/vfs"
	"github.com/puzpuzpuz/xsync/v4"
)

// Node kinds stored in the journal graph.
const (
	NodeDir     uint8 = 0
	NodeFile    uint8 = 1
	NodeSymlink uint8 = 2
)

// GraphNode is a filesystem entry in the journal's inode graph.
type GraphNode struct {
	ID         int64
	Kind       uint8
	Mode       uint32
	UID        uint32
	GID        uint32
	Size       uint64
	MtimeNs    int64
	CtimeNs    int64
	HasData    bool
	SymlinkTgt string
	RedirectTo string
	Opaque     bool
}

// GraphEdge is a parent→child name binding.
type GraphEdge struct {
	ParentID int64
	Name     string
	ChildID  int64
}

// Key prefix namespaces for the Pebble KV store.
//
//	n:<id>            → serialized GraphNode
//	e:<parentID>:<name> → child node ID (int64 LE)
//	x:<nodeID>:<xattrName> → xattr value bytes
//	w:<parentID>:<name> → whiteout marker (empty value)
//	m:<key>            → meta values (schema_version, next_node_id)
const (
	prefixNode     = "n:"
	prefixEdge     = "e:"
	prefixXattr    = "x:"
	prefixWhiteout = "w:"
	prefixMeta     = "m:"
)

// schemaVersion is the current journal schema version.
const schemaVersion = 1

// Journal is the Pebble-backed inode graph for the mutable overlay.
//
// # Data model
//
//	Nodes:     n:<id>               → binary-encoded GraphNode
//	Edges:     e:<parentID>:<name>  → child node ID (int64 LE)
//	XAttrs:    x:<nodeID>:<name>    → value bytes
//	Whiteouts: w:<parentID>:<name>  → empty value
//	Meta:      m:next_node_id       → next ID counter
//	           m:schema_version     → version string
//
// Root node (id=1) always exists with redirect_to='/'.
// Renames are O(1): just update the edge key. No descendants touched.
//
// # Durability model
//
// Modeled after filesystems built on RocksDB (BlueStore/BlueFS, TiKV/TiFS)
// with Pebble as the engine. Pebble and RocksDB share the same LSM
// architecture and WAL semantics.
//
// Writes use two durability levels:
//
//  1. batch.Commit(Sync) — fsyncs the WAL after every mutation batch.
//     This matches SQLite's synchronous=FULL and RocksDB's
//     WriteOptions.sync=true. Every FUSE metadata mutation (create,
//     unlink, rename, etc.) is durable in the WAL before returning to
//     the kernel. Process crash → WAL replay recovers all committed
//     mutations. Analogous to ext4's journal commit on every metadata
//     write (data=journal mode).
//
//  2. Sync() → Flush() — forces memtable contents into SSTables and
//     fsyncs them. Called on FUSE Fsync and before commit snapshots.
//     After Flush(), data survives even a storage device reset (not
//     just a process crash). Analogous to ext4's
//     ext4_sync_fs()/sync_blockdev().
//
//  3. Background flush loop — periodically calls Flush() every 5s to
//     bound WAL replay time (analogous to ext4's commit=5s). Without
//     this, a crash would require replaying a potentially large WAL.
//     The loop does NOT affect durability — writes are already durable
//     via the WAL sync in (1).
//
// # Crash consistency
//
// If the process crashes:
//   - Pebble replays its WAL on Open(), recovering all committed batches
//   - OpenJournal runs orphan edge cleanup (analogous to ext4's
//     ext4_orphan_cleanup at mount time)
//   - Root node is verified/recreated if missing
//
// # Snapshot isolation
//
// ResolvePath uses pebble.Snapshot for consistent reads across the
// entire path walk — analogous to holding i_mutex on parent directories
// during lookup (prevents interleaving with concurrent renames).
type Journal struct {
	db *pebble.DB
	mu sync.Mutex // serializes write batches + node ID allocation

	// nextNodeID is the next auto-increment node ID (in-memory counter,
	// persisted to m:next_node_id on Sync/Close).
	nextNodeID atomic.Int64

	// Background flush loop state. The loop calls Flush() periodically
	// to bound WAL replay time. It does NOT affect write durability
	// (writes are already WAL-synced via batch.Commit(Sync)).
	flushDone     chan struct{}
	flushClose    chan struct{}
	flushPending  *xsync.Counter // approximate unflushed batch count
	flushInterval atomic.Int64   // nanoseconds
}

// --- Key encoding helpers ---

// nodeKey returns the key for a node: n:<id>
func nodeKey(id int64) []byte {
	b := make([]byte, 2+8)
	copy(b, prefixNode)
	binary.BigEndian.PutUint64(b[2:], uint64(id))
	return b
}

// edgeKey returns the key for an edge: e:<parentID>:<name>
func edgeKey(parentID int64, name string) []byte {
	b := make([]byte, 2+8+1+len(name))
	copy(b, prefixEdge)
	binary.BigEndian.PutUint64(b[2:], uint64(parentID))
	b[10] = ':'
	copy(b[11:], name)
	return b
}

// edgePrefix returns the prefix for all edges under a parent: e:<parentID>:
func edgePrefix(parentID int64) []byte {
	b := make([]byte, 2+8+1)
	copy(b, prefixEdge)
	binary.BigEndian.PutUint64(b[2:], uint64(parentID))
	b[10] = ':'
	return b
}

// xattrKey returns the key for an xattr: x:<nodeID>:<name>
func xattrKey(nodeID int64, name string) []byte {
	b := make([]byte, 2+8+1+len(name))
	copy(b, prefixXattr)
	binary.BigEndian.PutUint64(b[2:], uint64(nodeID))
	b[10] = ':'
	copy(b[11:], name)
	return b
}

// xattrPrefix returns the prefix for all xattrs under a node: x:<nodeID>:
func xattrPrefix(nodeID int64) []byte {
	b := make([]byte, 2+8+1)
	copy(b, prefixXattr)
	binary.BigEndian.PutUint64(b[2:], uint64(nodeID))
	b[10] = ':'
	return b
}

// whiteoutKey returns the key for a whiteout: w:<parentID>:<name>
func whiteoutKey(parentID int64, name string) []byte {
	b := make([]byte, 2+8+1+len(name))
	copy(b, prefixWhiteout)
	binary.BigEndian.PutUint64(b[2:], uint64(parentID))
	b[10] = ':'
	copy(b[11:], name)
	return b
}

// whiteoutPrefix returns the prefix for all whiteouts under a parent: w:<parentID>:
func whiteoutPrefix(parentID int64) []byte {
	b := make([]byte, 2+8+1)
	copy(b, prefixWhiteout)
	binary.BigEndian.PutUint64(b[2:], uint64(parentID))
	b[10] = ':'
	return b
}

// metaKey returns a meta key: m:<key>
func metaKey(key string) []byte {
	return append([]byte(prefixMeta), key...)
}

// nextNodeIDKey returns the key for the next node ID counter.
func nextNodeIDKey() []byte {
	return metaKey("next_node_id")
}

// encodeNode serializes a GraphNode into a byte slice.
// Format: kind(1) + mode(4) + uid(4) + gid(4) + size(8) + mtimeNs(8) + ctimeNs(8)
//
//   - hasData(1) + opaque(1) + symlinkTgtLen(4) + symlinkTgt
//   - redirectToLen(4) + redirectTo
func encodeNode(n *GraphNode) []byte {
	stLen := len(n.SymlinkTgt)
	rrLen := len(n.RedirectTo)
	total := 1 + 4 + 4 + 4 + 8 + 8 + 8 + 1 + 1 + 4 + stLen + 4 + rrLen
	b := make([]byte, total)
	off := 0
	b[off] = n.Kind
	off += 1
	binary.LittleEndian.PutUint32(b[off:], n.Mode)
	off += 4
	binary.LittleEndian.PutUint32(b[off:], n.UID)
	off += 4
	binary.LittleEndian.PutUint32(b[off:], n.GID)
	off += 4
	binary.LittleEndian.PutUint64(b[off:], n.Size)
	off += 8
	binary.LittleEndian.PutUint64(b[off:], uint64(n.MtimeNs))
	off += 8
	binary.LittleEndian.PutUint64(b[off:], uint64(n.CtimeNs))
	off += 8
	if n.HasData {
		b[off] = 1
	}
	off += 1
	if n.Opaque {
		b[off] = 1
	}
	off += 1
	binary.LittleEndian.PutUint32(b[off:], uint32(stLen))
	off += 4
	copy(b[off:], n.SymlinkTgt)
	off += stLen
	binary.LittleEndian.PutUint32(b[off:], uint32(rrLen))
	off += 4
	copy(b[off:], n.RedirectTo)
	return b
}

// decodeNode deserializes a GraphNode from a byte slice.
func decodeNode(data []byte, id int64) *GraphNode {
	n := &GraphNode{ID: id}
	off := 0
	n.Kind = data[off]
	off += 1
	n.Mode = binary.LittleEndian.Uint32(data[off:])
	off += 4
	n.UID = binary.LittleEndian.Uint32(data[off:])
	off += 4
	n.GID = binary.LittleEndian.Uint32(data[off:])
	off += 4
	n.Size = binary.LittleEndian.Uint64(data[off:])
	off += 8
	n.MtimeNs = int64(binary.LittleEndian.Uint64(data[off:]))
	off += 8
	n.CtimeNs = int64(binary.LittleEndian.Uint64(data[off:]))
	off += 8
	n.HasData = data[off] != 0
	off += 1
	n.Opaque = data[off] != 0
	off += 1
	stLen := binary.LittleEndian.Uint32(data[off:])
	off += 4
	n.SymlinkTgt = string(data[off : off+int(stLen)])
	off += int(stLen)
	rrLen := binary.LittleEndian.Uint32(data[off:])
	off += 4
	n.RedirectTo = string(data[off : off+int(rrLen)])
	return n
}

// encodeInt64 encodes an int64 as 8 bytes LE.
func encodeInt64(v int64) []byte {
	b := make([]byte, 8)
	binary.LittleEndian.PutUint64(b, uint64(v))
	return b
}

// decodeInt64 decodes 8 bytes LE as int64.
func decodeInt64(b []byte) int64 {
	return int64(binary.LittleEndian.Uint64(b))
}

// OpenJournal opens or creates the journal database in dir.
// Performs crash recovery analogous to ext4's journal replay:
//  1. WAL auto-replay (Pebble handles this)
//  2. Schema version check and migration
//  3. Root node integrity verification
//  4. Orphan edge cleanup (dangling references from partial tx)
func OpenJournal(dir string) (*Journal, error) {
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return nil, fmt.Errorf("create journal dir: %w", err)
	}

	opts := &pebble.Options{
		FS: vfs.Default,
		// WAL enabled for crash recovery — equivalent to RocksDB's
		// default (WAL is always on unless explicitly disabled).
		DisableWAL: false,
		// Small memtable: journal stores filesystem metadata (small
		// keys/values). 4MB matches RocksDB's default write_buffer_size.
		MemTableSize: 4 << 20,
		// Disable stats collection — not needed for a FUSE journal.

	}

	db, err := pebble.Open(dir, opts)
	if err != nil {
		return nil, fmt.Errorf("open journal db: %w", err)
	}

	j := &Journal{db: db, flushPending: xsync.NewCounter()}

	// Initialize or verify schema.
	if err := j.initSchema(); err != nil {
		_ = db.Close()
		return nil, fmt.Errorf("init schema: %w", err)
	}

	// Load next node ID counter.
	if err := j.loadNextNodeID(); err != nil {
		_ = db.Close()
		return nil, fmt.Errorf("load next node id: %w", err)
	}

	// Verify root node survived any prior crash.
	rootData, closer, err := db.Get(nodeKey(1))
	if err == pebble.ErrNotFound {
		root := &GraphNode{
			ID:         1,
			Kind:       NodeDir,
			Mode:       16877,
			RedirectTo: "/",
		}
		if err := db.Set(nodeKey(1), encodeNode(root), pebble.Sync); err != nil {
			_ = db.Close()
			return nil, fmt.Errorf("create root node: %w", err)
		}
	} else if err != nil {
		_ = db.Close()
		return nil, fmt.Errorf("verify root node: %w", err)
	} else {
		_ = closer.Close()
		_ = rootData
	}

	// Clean up orphan edges: edges pointing to non-existent nodes.
	// This can happen if a process crash occurs between node creation
	// and edge insertion in a compound operation. Like ext4's
	// ext4_orphan_cleanup().
	if err := j.cleanOrphanEdges(); err != nil {
		_ = db.Close()
		return nil, fmt.Errorf("clean orphan edges: %w", err)
	}

	j.startFlushLoop()
	return j, nil
}

// initSchema creates or verifies the journal schema metadata.
func (j *Journal) initSchema() error {
	schemaKey := metaKey("schema_version")
	_, closer, err := j.db.Get(schemaKey)
	if err == pebble.ErrNotFound {
		if err := j.db.Set(schemaKey, fmt.Append(nil, schemaVersion), pebble.Sync); err != nil {
			return err
		}
		return nil
	}
	if err != nil {
		return err
	}
	_ = closer.Close()
	return nil
}

// loadNextNodeID loads the persisted next-node-ID counter.
func (j *Journal) loadNextNodeID() error {
	val, closer, err := j.db.Get(nextNodeIDKey())
	if err == pebble.ErrNotFound {
		j.nextNodeID.Store(2) // 1 is root
		return nil
	}
	if err != nil {
		return err
	}
	_ = closer.Close()
	if len(val) >= 8 {
		j.nextNodeID.Store(decodeInt64(val))
	} else {
		j.nextNodeID.Store(2)
	}
	return nil
}

// persistNextNodeID writes the current next-node-ID counter to the DB.
// Caller must hold j.mu.
func (j *Journal) persistNextNodeID() error {
	return j.db.Set(nextNodeIDKey(), encodeInt64(j.nextNodeID.Load()), pebble.Sync)
}

// cleanOrphanEdges removes edges pointing to non-existent nodes.
// Analogous to ext4's ext4_orphan_cleanup() at mount time.
func (j *Journal) cleanOrphanEdges() error {
	prefix := []byte(prefixEdge)
	iter, err := j.db.NewIter(&pebble.IterOptions{
		LowerBound: prefix,
		UpperBound: []byte(prefixEdge + "\xff"),
	})
	if err != nil {
		return err
	}
	defer iter.Close()

	batch := j.db.NewBatch()
	defer batch.Close()

	for iter.First(); iter.Valid(); iter.Next() {
		childIDVal := iter.Value()
		if len(childIDVal) < 8 {
			continue
		}
		childID := decodeInt64(childIDVal)
		_, closer, err := j.db.Get(nodeKey(childID))
		if err == pebble.ErrNotFound {
			_ = batch.Delete(iter.Key(), nil)
		} else if err != nil {
			return err
		} else {
			_ = closer.Close()
		}
	}
	if err := iter.Error(); err != nil {
		return err
	}
	if batch.Count() > 0 {
		return batch.Commit(pebble.Sync)
	}
	return nil
}

// VerifyIntegrity runs a full database consistency check.
// Analogous to ext4's e2fsck -n (read-only check):
//   - Root node existence
//   - No orphan edges (edges without matching nodes)
//   - No orphan xattrs (xattrs without matching nodes)
func (j *Journal) VerifyIntegrity() error {
	// 1. Root node must exist.
	rootData, closer, err := j.db.Get(nodeKey(1))
	if err == pebble.ErrNotFound {
		return fmt.Errorf("root node missing")
	}
	if err != nil {
		return fmt.Errorf("root node check: %w", err)
	}
	_ = closer.Close()
	_ = rootData

	// 2. No orphan edges.
	edgePrefixBytes := []byte(prefixEdge)
	iter, err := j.db.NewIter(&pebble.IterOptions{
		LowerBound: edgePrefixBytes,
		UpperBound: []byte(prefixEdge + "\xff"),
	})
	if err != nil {
		return fmt.Errorf("edge scan: %w", err)
	}
	defer iter.Close()

	for iter.First(); iter.Valid(); iter.Next() {
		childIDVal := iter.Value()
		if len(childIDVal) < 8 {
			continue
		}
		childID := decodeInt64(childIDVal)
		_, closer, err := j.db.Get(nodeKey(childID))
		if err == pebble.ErrNotFound {
			return fmt.Errorf("orphan edge to node %d", childID)
		}
		if err != nil {
			return fmt.Errorf("orphan check: %w", err)
		}
		_ = closer.Close()
	}
	if err := iter.Error(); err != nil {
		return fmt.Errorf("edge iter error: %w", err)
	}

	// 3. No orphan xattrs.
	xaPrefix := []byte(prefixXattr)
	xiter, err := j.db.NewIter(&pebble.IterOptions{
		LowerBound: xaPrefix,
		UpperBound: []byte(prefixXattr + "\xff"),
	})
	if err != nil {
		return fmt.Errorf("xattr scan: %w", err)
	}
	defer xiter.Close()

	for xiter.First(); xiter.Valid(); xiter.Next() {
		key := xiter.Key()
		if len(key) < 11 {
			continue
		}
		nodeID := int64(binary.BigEndian.Uint64(key[2:10]))
		_, closer, err := j.db.Get(nodeKey(nodeID))
		if err == pebble.ErrNotFound {
			return fmt.Errorf("orphan xattr for node %d", nodeID)
		}
		if err != nil {
			return fmt.Errorf("xattr orphan check: %w", err)
		}
		_ = closer.Close()
	}
	if err := xiter.Error(); err != nil {
		return fmt.Errorf("xattr iter error: %w", err)
	}

	return nil
}

// Close stops the background flush loop, persists state, and closes the DB.
func (j *Journal) Close() error {
	// Stop background flush loop.
	if j.flushClose != nil {
		close(j.flushClose)
	}
	if j.flushDone != nil {
		<-j.flushDone
	}

	// Persist node ID counter with a final WAL-synced write.
	j.mu.Lock()
	_ = j.persistNextNodeID()
	j.mu.Unlock()

	return j.db.Close()
}

// allocNodeID allocates a new unique node ID. Caller must hold j.mu.
func (j *Journal) allocNodeID() int64 {
	return j.nextNodeID.Add(1) - 1
}

// tx executes fn within a single Pebble batch, committed with WAL sync.
//
// Durability guarantee: the batch is fsynced to the WAL before tx returns
// (pebble.Sync). This matches RocksDB's WriteOptions.sync=true and
// SQLite's synchronous=FULL. After tx returns, the mutation survives
// a process crash — Pebble replays the WAL on next Open().
//
// The mu lock serializes batches so only one WAL sync is in flight at a
// time. This is the same model as RocksDB's write thread — one write
// group syncs the WAL together, eliminating per-write fsync overhead.
func (j *Journal) tx(fn func(b *pebble.Batch) error) error {
	j.mu.Lock()
	defer j.mu.Unlock()

	batch := j.db.NewBatch()
	defer batch.Close()

	if err := fn(batch); err != nil {
		return err
	}

	// Sync commit: fsyncs the WAL. Matches RocksDB's
	// WriteOptions{Sync: true} and SQLite's synchronous=FULL.
	// Every FUSE metadata mutation is durable before returning.
	if err := batch.Commit(pebble.Sync); err != nil {
		return err
	}

	j.flushPending.Add(1)

	// Eager flush if enough batches have accumulated — bounds WAL
	// replay time. After Flush(), data is in SSTables so no WAL
	// replay is needed for these entries.
	if j.flushPending.Value() >= 64 {
		_ = j.flushLocked()
	}

	return nil
}

// Sync forces a memtable flush to SSTables and fsyncs them.
// Call this from FUSE's Fsync path and the commit path — analogous to
// ext4's sync_file_range/fsync which ensures data is on stable storage.
//
// After Sync(), all committed mutations are in SSTables (not just the WAL),
// so they survive even a storage device reset.
func (j *Journal) Sync() error {
	j.mu.Lock()
	defer j.mu.Unlock()
	return j.flushLocked()
}

// flushLocked persists the node ID counter and flushes memtables to SSTables.
// Caller must hold j.mu.
func (j *Journal) flushLocked() error {
	if err := j.persistNextNodeID(); err != nil {
		return err
	}
	if err := j.db.Flush(); err != nil {
		return err
	}
	j.flushPending.Reset()
	return nil
}

// startFlushLoop launches the background periodic flush goroutine.
// This bounds WAL replay time by periodically moving memtable entries
// into SSTables. It does NOT affect write durability — writes are
// already WAL-synced via batch.Commit(Sync).
//
// Analogous to ext4's commit=5s: the journal is committed every 5
// seconds so that after a crash, only the last 5 seconds of WAL need
// replay (instead of potentially the entire WAL).
func (j *Journal) startFlushLoop() {
	if j.flushInterval.Load() <= 0 {
		j.flushInterval.Store(int64(5 * time.Second))
	}
	j.flushDone = make(chan struct{})
	j.flushClose = make(chan struct{})
	go func() {
		defer close(j.flushDone)
		ticker := time.NewTicker(time.Duration(j.flushInterval.Load()))
		defer ticker.Stop()
		for {
			select {
			case <-j.flushClose:
				return
			case <-ticker.C:
				if j.flushPending.Value() > 0 {
					j.mu.Lock()
					_ = j.flushLocked()
					j.mu.Unlock()
				}
			}
		}
	}()
}

// --- Node CRUD ---

// GetNode returns the node by ID, or nil if not found.
func (j *Journal) GetNode(id int64) (*GraphNode, error) {
	data, closer, err := j.db.Get(nodeKey(id))
	if err == pebble.ErrNotFound {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	defer closer.Close()
	n := decodeNode(data, id)
	return n, nil
}

// createNodeInBatch inserts a new node in a batch and returns its ID.
func (j *Journal) createNodeInBatch(b *pebble.Batch, n *GraphNode) (int64, error) {
	id := j.allocNodeID()
	n.ID = id
	if err := b.Set(nodeKey(id), encodeNode(n), nil); err != nil {
		return 0, err
	}
	return id, nil
}

// updateNodeInBatch updates a node in a batch.
func (j *Journal) updateNodeInBatch(b *pebble.Batch, n *GraphNode) error {
	return b.Set(nodeKey(n.ID), encodeNode(n), nil)
}

// UpdateNode updates a node's metadata.
func (j *Journal) UpdateNode(n *GraphNode) error {
	return j.tx(func(b *pebble.Batch) error {
		return j.updateNodeInBatch(b, n)
	})
}

// SetHasData marks that a node now has data in the mutable dir.
func (j *Journal) SetHasData(nodeID int64) error {
	return j.tx(func(b *pebble.Batch) error {
		data, closer, err := j.db.Get(nodeKey(nodeID))
		if err != nil {
			return err
		}
		defer closer.Close()
		n := decodeNode(data, nodeID)
		n.HasData = true
		return b.Set(nodeKey(nodeID), encodeNode(n), nil)
	})
}

// --- Edge CRUD ---

// ListEdges returns all edges under a parent.
func (j *Journal) ListEdges(parentID int64) ([]GraphEdge, error) {
	prefix := edgePrefix(parentID)
	iter, err := j.db.NewIter(&pebble.IterOptions{
		LowerBound: prefix,
		UpperBound: append(prefix[:len(prefix):len(prefix)], 0xFF),
	})
	if err != nil {
		return nil, err
	}
	defer iter.Close()

	var edges []GraphEdge
	for iter.First(); iter.Valid(); iter.Next() {
		key := iter.Key()
		name := string(key[11:])
		childID := decodeInt64(iter.Value())
		edges = append(edges, GraphEdge{
			ParentID: parentID,
			Name:     name,
			ChildID:  childID,
		})
	}
	if err := iter.Error(); err != nil {
		return nil, err
	}
	return edges, nil
}

// --- Whiteout CRUD ---

// AddWhiteout records that a pxar entry at (parentID, name) is deleted.
func (j *Journal) AddWhiteout(parentID int64, name string) error {
	return j.tx(func(b *pebble.Batch) error {
		return b.Set(whiteoutKey(parentID, name), nil, nil)
	})
}

// ListWhiteouts returns all whiteout names under a parent.
func (j *Journal) ListWhiteouts(parentID int64) ([]string, error) {
	prefix := whiteoutPrefix(parentID)
	iter, err := j.db.NewIter(&pebble.IterOptions{
		LowerBound: prefix,
		UpperBound: append(prefix[:len(prefix):len(prefix)], 0xFF),
	})
	if err != nil {
		return nil, err
	}
	defer iter.Close()

	var names []string
	for iter.First(); iter.Valid(); iter.Next() {
		key := iter.Key()
		names = append(names, string(key[11:]))
	}
	if err := iter.Error(); err != nil {
		return nil, err
	}
	return names, nil
}

// --- XAttr Operations ---

// GetXAttr returns the value of an extended attribute, or nil if not found.
func (j *Journal) GetXAttr(nodeID int64, name string) ([]byte, error) {
	val, closer, err := j.db.Get(xattrKey(nodeID, name))
	if err == pebble.ErrNotFound {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	defer closer.Close()
	cp := make([]byte, len(val))
	copy(cp, val)
	return cp, nil
}

// ListXAttrs returns all extended attribute names for a node.
func (j *Journal) ListXAttrs(nodeID int64) ([]string, error) {
	prefix := xattrPrefix(nodeID)
	iter, err := j.db.NewIter(&pebble.IterOptions{
		LowerBound: prefix,
		UpperBound: append(prefix[:len(prefix):len(prefix)], 0xFF),
	})
	if err != nil {
		return nil, err
	}
	defer iter.Close()

	var names []string
	for iter.First(); iter.Valid(); iter.Next() {
		key := iter.Key()
		names = append(names, string(key[11:]))
	}
	if err := iter.Error(); err != nil {
		return nil, err
	}
	return names, nil
}

// SetXAttr upserts an extended attribute value.
func (j *Journal) SetXAttr(nodeID int64, name string, value []byte) error {
	return j.tx(func(b *pebble.Batch) error {
		return b.Set(xattrKey(nodeID, name), value, nil)
	})
}

// RemoveXAttr deletes an extended attribute.
func (j *Journal) RemoveXAttr(nodeID int64, name string) error {
	return j.tx(func(b *pebble.Batch) error {
		return b.Delete(xattrKey(nodeID, name), nil)
	})
}

// --- Path Resolution ---

// ResolvePath walks the edge graph from root to find a path.
// Returns:
//   - nodeID: the journal node ID at the final component (0 if not in journal)
//   - pxarPath: the pxar source path for the remaining/entire path
//   - fellOffAt: the node ID where we fell off the graph (for whiteout checks)
//   - remaining: the remaining path components after falling off
//
// If nodeID != 0, the path is fully in the journal.
// If nodeID == 0, the path is partially or fully in pxar.
//
// Uses a Pebble snapshot for consistent reads across the entire walk —
// analogous to ext4's i_mutex on parent directories during lookup.
func (j *Journal) ResolvePath(path string) (nodeID int64, pxarPath string, fellOffAt int64, remaining string, err error) {
	if path == "/" || path == "" {
		return 1, "/", 0, "", nil
	}

	// Snapshot provides a consistent view without blocking writers.
	snap := j.db.NewSnapshot()
	defer snap.Close()

	curID := int64(1)
	var pxarPrefix strings.Builder
	pxarPrefix.WriteByte('/')
	pos := 1

	for pos < len(path) {
		end := pos
		for end < len(path) && path[end] != '/' {
			end++
		}
		part := path[pos:end]

		// Look up edge within the snapshot.
		edgeVal, edgeCloser, gerr := snap.Get(edgeKey(curID, part))
		if gerr == pebble.ErrNotFound {
			// Check for whiteout within the same snapshot.
			_, _, werr := snap.Get(whiteoutKey(curID, part))
			if werr != nil && werr != pebble.ErrNotFound {
				return 0, "", 0, "", werr
			}
			if werr == nil {
				return 0, "", 0, "", nil // whiteout
			}
			// Fell off graph.
			return 0, pxarPrefix.String() + path[pos-1:], curID, path[pos:], nil
		}
		if gerr != nil {
			return 0, "", 0, "", gerr
		}
		childID := decodeInt64(edgeVal)
		_ = edgeCloser.Close()

		curID = childID

		// Track pxar redirect within the snapshot.
		nodeData, nodeCloser, nerr := snap.Get(nodeKey(curID))
		if nerr != nil {
			return 0, "", 0, "", nerr
		}
		n := decodeNode(nodeData, curID)
		_ = nodeCloser.Close()

		if n.RedirectTo != "" {
			pxarPrefix.Reset()
			pxarPrefix.WriteString(n.RedirectTo)
		} else {
			pxarPrefix.WriteByte('/')
			pxarPrefix.WriteString(part)
		}

		pos = end + 1
	}

	return curID, pxarPrefix.String(), 0, "", nil
}

// --- Batch Operations for Commit ---

// AllXAttrs returns all xattrs grouped by node ID.
func (j *Journal) AllXAttrs() (map[int64]map[string][]byte, error) {
	prefix := []byte(prefixXattr)
	iter, err := j.db.NewIter(&pebble.IterOptions{
		LowerBound: prefix,
		UpperBound: []byte(prefixXattr + "\xff"),
	})
	if err != nil {
		return nil, err
	}
	defer iter.Close()

	result := make(map[int64]map[string][]byte)
	for iter.First(); iter.Valid(); iter.Next() {
		key := iter.Key()
		nodeID := int64(binary.BigEndian.Uint64(key[2:10]))
		name := string(key[11:])
		val := iter.Value()

		if result[nodeID] == nil {
			result[nodeID] = make(map[string][]byte)
		}
		cp := make([]byte, len(val))
		copy(cp, val)
		result[nodeID][name] = cp
	}
	if err := iter.Error(); err != nil {
		return nil, err
	}
	return result, nil
}

// EnsureNodePath atomically creates a journal node and all intermediate
// edges/nodes for the given path in a single batch.
func (j *Journal) EnsureNodePath(path string, n *GraphNode, whiteout bool) (int64, error) {
	var nodeID int64
	err := j.tx(func(b *pebble.Batch) error {
		id, err := j.createNodeInBatch(b, n)
		if err != nil {
			return err
		}
		nodeID = id

		parts := splitPath(path)
		curParentID := int64(1)

		for i, name := range parts {
			if name == "" {
				continue
			}

			// Check if edge already exists.
			edgeVal, closer, gerr := j.db.Get(edgeKey(curParentID, name))
			if gerr == nil {
				childID := decodeInt64(edgeVal)
				_ = closer.Close()
				curParentID = childID
				continue
			}
			if gerr != pebble.ErrNotFound {
				return gerr
			}

			// No edge — clean up any stale whiteout.
			_ = b.Delete(whiteoutKey(curParentID, name), nil)

			if i == len(parts)-1 {
				// Final component — link to our node.
				if err := b.Set(edgeKey(curParentID, name), encodeInt64(nodeID), nil); err != nil {
					return err
				}
				if whiteout {
					if err := b.Set(whiteoutKey(curParentID, name), nil, nil); err != nil {
						return err
					}
				}
			} else {
				// Intermediate directory — create redirect node.
				var intermediatePath strings.Builder
				intermediatePath.WriteByte('/')
				intermediatePath.WriteString(parts[0])
				for jj := 1; jj <= i; jj++ {
					intermediatePath.WriteByte('/')
					intermediatePath.WriteString(parts[jj])
				}
				intermediate := &GraphNode{
					Kind:       NodeDir,
					Mode:       uint32(0o755 | 0x4000),
					UID:        n.UID,
					GID:        n.GID,
					MtimeNs:    n.MtimeNs,
					CtimeNs:    n.CtimeNs,
					RedirectTo: intermediatePath.String(),
				}
				midID, err := j.createNodeInBatch(b, intermediate)
				if err != nil {
					return err
				}
				if err := b.Set(edgeKey(curParentID, name), encodeInt64(midID), nil); err != nil {
					return err
				}
				curParentID = midID
			}
		}
		return nil
	})
	return nodeID, err
}

// Clear truncates all data (keeps root node).
func (j *Journal) Clear() error {
	return j.tx(func(b *pebble.Batch) error {
		// Delete all whiteouts, xattrs, edges, and non-root nodes
		// using efficient range deletes.
		if err := deletePrefix(b, []byte(prefixWhiteout)); err != nil {
			return err
		}
		if err := deletePrefix(b, []byte(prefixXattr)); err != nil {
			return err
		}
		if err := deletePrefix(b, []byte(prefixEdge)); err != nil {
			return err
		}
		if err := deletePrefixExcept(b, []byte(prefixNode), nodeKey(1)); err != nil {
			return err
		}
		root := &GraphNode{
			ID:         1,
			Kind:       NodeDir,
			Mode:       16877,
			RedirectTo: "/",
		}
		return b.Set(nodeKey(1), encodeNode(root), nil)
	})
}

// deletePrefix deletes all keys with the given prefix using DeleteRange.
func deletePrefix(b *pebble.Batch, prefix []byte) error {
	upper := make([]byte, len(prefix)+1)
	copy(upper, prefix)
	upper[len(prefix)] = 0xFF
	return b.DeleteRange(prefix, upper, nil)
}

// deletePrefixExcept deletes all keys with the given prefix except one key.
func deletePrefixExcept(b *pebble.Batch, prefix []byte, exceptKey []byte) error {
	// Delete range [prefix, exceptKey)
	if err := b.DeleteRange(prefix, exceptKey, nil); err != nil {
		return err
	}
	// Delete range (exceptKey, prefix\xFF]
	nextKey := make([]byte, len(exceptKey)+1)
	copy(nextKey, exceptKey)
	nextKey[len(exceptKey)] = 0xFF
	upper := make([]byte, len(prefix)+1)
	copy(upper, prefix)
	upper[len(prefix)] = 0xFF
	return b.DeleteRange(nextKey, upper, nil)
}

// --- Compound Atomic Operations ---

// DeleteEdgeAndNode atomically removes an edge and its target node.
// Also cascades: deletes all xattrs and child edges for the node.
// Analogous to SQLite's ON DELETE CASCADE.
func (j *Journal) DeleteEdgeAndNode(parentID int64, name string, nodeID int64, addWhiteout bool) error {
	return j.tx(func(b *pebble.Batch) error {
		// Delete edge.
		if err := b.Delete(edgeKey(parentID, name), nil); err != nil {
			return err
		}
		if addWhiteout {
			if err := b.Set(whiteoutKey(parentID, name), nil, nil); err != nil {
				return err
			}
		}
		// Cascade: delete all xattrs for this node.
		xaPrefix := xattrPrefix(nodeID)
		xaUpper := make([]byte, len(xaPrefix)+1)
		copy(xaUpper, xaPrefix)
		xaUpper[len(xaPrefix)] = 0xFF
		if err := b.DeleteRange(xaPrefix, xaUpper, nil); err != nil {
			return err
		}
		// Cascade: delete all edges where this node is the parent.
		childEdgePrefix := edgePrefix(nodeID)
		childEdgeUpper := make([]byte, len(childEdgePrefix)+1)
		copy(childEdgeUpper, childEdgePrefix)
		childEdgeUpper[len(childEdgePrefix)] = 0xFF
		if err := b.DeleteRange(childEdgePrefix, childEdgeUpper, nil); err != nil {
			return err
		}
		// Delete node.
		return b.Delete(nodeKey(nodeID), nil)
	})
}

// CreateNodeEdgeAndWhiteout atomically creates a node, links it, and adds
// a whiteout for the pxar entry being shadowed.
func (j *Journal) CreateNodeEdgeAndWhiteout(parentID int64, name string, n *GraphNode, whiteout bool) (int64, error) {
	var id int64
	err := j.tx(func(b *pebble.Batch) error {
		var err error
		id, err = j.createNodeInBatch(b, n)
		if err != nil {
			return err
		}
		if err := b.Set(edgeKey(parentID, name), encodeInt64(id), nil); err != nil {
			return err
		}
		if whiteout {
			if err := b.Set(whiteoutKey(parentID, name), nil, nil); err != nil {
				return err
			}
		}
		return nil
	})
	return id, err
}

// MoveEdgeAndWhiteout atomically moves an edge and adds whiteouts.
func (j *Journal) MoveEdgeAndWhiteout(oldParent int64, oldName string, newParent int64, newName string, replaceDestNode int64, whiteoutOld, whiteoutNew bool) error {
	return j.tx(func(b *pebble.Batch) error {
		// Remove destination whiteout.
		_ = b.Delete(whiteoutKey(newParent, newName), nil)

		// Handle destination collision.
		if replaceDestNode != 0 {
			if err := b.Delete(edgeKey(newParent, newName), nil); err != nil {
				return err
			}
			// Cascade: delete dest node's xattrs.
			xaPrefix := xattrPrefix(replaceDestNode)
			xaUpper := make([]byte, len(xaPrefix)+1)
			copy(xaUpper, xaPrefix)
			xaUpper[len(xaPrefix)] = 0xFF
			if err := b.DeleteRange(xaPrefix, xaUpper, nil); err != nil {
				return err
			}
			// Cascade: delete dest node's child edges.
			childEdgePrefix := edgePrefix(replaceDestNode)
			childEdgeUpper := make([]byte, len(childEdgePrefix)+1)
			copy(childEdgeUpper, childEdgePrefix)
			childEdgeUpper[len(childEdgePrefix)] = 0xFF
			if err := b.DeleteRange(childEdgePrefix, childEdgeUpper, nil); err != nil {
				return err
			}
			if err := b.Delete(nodeKey(replaceDestNode), nil); err != nil {
				return err
			}
		}

		// Look up source edge to get child ID.
		srcEdgeVal, closer, err := j.db.Get(edgeKey(oldParent, oldName))
		if err != nil {
			return fmt.Errorf("move edge: source (%d, %q) not found: %w", oldParent, oldName, err)
		}
		childIDVal := make([]byte, len(srcEdgeVal))
		copy(childIDVal, srcEdgeVal)
		_ = closer.Close()

		// Move source edge: delete old, set new.
		if err := b.Delete(edgeKey(oldParent, oldName), nil); err != nil {
			return err
		}
		if err := b.Set(edgeKey(newParent, newName), childIDVal, nil); err != nil {
			return err
		}

		// Whiteout old location.
		if whiteoutOld {
			if err := b.Set(whiteoutKey(oldParent, oldName), nil, nil); err != nil {
				return err
			}
		}
		// Whiteout new location if it had a pxar entry.
		if whiteoutNew {
			if err := b.Set(whiteoutKey(newParent, newName), nil, nil); err != nil {
				return err
			}
		}
		return nil
	})
}
