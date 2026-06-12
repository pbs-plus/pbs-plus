package pxarmount

import (
	"encoding/binary"
	"fmt"
	"os"
	"strings"
	"sync/atomic"
	"time"

	"github.com/cockroachdb/pebble"
	"github.com/cockroachdb/pebble/vfs"
	"github.com/pbs-plus/pxar/format"
	"github.com/puzpuzpuz/xsync/v4"
)

const (
	NodeDir     uint8 = 0
	NodeFile    uint8 = 1
	NodeSymlink uint8 = 2
)

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

type GraphEdge struct {
	ParentID int64
	Name     string
	ChildID  int64
}

const (
	prefixNode     = "n:"
	prefixEdge     = "e:"
	prefixXattr    = "x:"
	prefixWhiteout = "w:"
	prefixMeta     = "m:"
)

const schemaVersion = 1

type Journal struct {
	db *pebble.DB

	nextNodeID atomic.Int64

	flushDone    chan struct{}
	flushClose   chan struct{}
	flushPending *xsync.Counter

	writeQueue chan *journalWrite
	writeDone  chan struct{}
}

type journalWrite struct {
	fn   func(b *pebble.Batch) error
	done chan error
}

func nodeKey(id int64) []byte {
	b := make([]byte, 2+8)
	copy(b, prefixNode)
	binary.BigEndian.PutUint64(b[2:], uint64(id))
	return b
}

func edgeKey(parentID int64, name string) []byte {
	b := make([]byte, 2+8+1+len(name))
	copy(b, prefixEdge)
	binary.BigEndian.PutUint64(b[2:], uint64(parentID))
	b[10] = ':'
	copy(b[11:], name)
	return b
}

func edgePrefix(parentID int64) []byte {
	b := make([]byte, 2+8+1)
	copy(b, prefixEdge)
	binary.BigEndian.PutUint64(b[2:], uint64(parentID))
	b[10] = ':'
	return b
}

func xattrKey(nodeID int64, name string) []byte {
	b := make([]byte, 2+8+1+len(name))
	copy(b, prefixXattr)
	binary.BigEndian.PutUint64(b[2:], uint64(nodeID))
	b[10] = ':'
	copy(b[11:], name)
	return b
}

func xattrPrefix(nodeID int64) []byte {
	b := make([]byte, 2+8+1)
	copy(b, prefixXattr)
	binary.BigEndian.PutUint64(b[2:], uint64(nodeID))
	b[10] = ':'
	return b
}

func whiteoutKey(parentID int64, name string) []byte {
	b := make([]byte, 2+8+1+len(name))
	copy(b, prefixWhiteout)
	binary.BigEndian.PutUint64(b[2:], uint64(parentID))
	b[10] = ':'
	copy(b[11:], name)
	return b
}

func whiteoutPrefix(parentID int64) []byte {
	b := make([]byte, 2+8+1)
	copy(b, prefixWhiteout)
	binary.BigEndian.PutUint64(b[2:], uint64(parentID))
	b[10] = ':'
	return b
}

func metaKey(key string) []byte {
	return append([]byte(prefixMeta), key...)
}

func nextNodeIDKey() []byte {
	return metaKey("next_node_id")
}

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

func encodeInt64(v int64) []byte {
	b := make([]byte, 8)
	binary.LittleEndian.PutUint64(b, uint64(v))
	return b
}

func decodeInt64(b []byte) int64 {
	return int64(binary.LittleEndian.Uint64(b))
}

func OpenJournal(dir string) (*Journal, error) {
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return nil, fmt.Errorf("create journal dir: %w", err)
	}

	opts := &pebble.Options{
		FS:           vfs.Default,
		DisableWAL:   false,
		MemTableSize: 4 << 20,
	}

	db, err := pebble.Open(dir, opts)
	if err != nil {
		return nil, fmt.Errorf("open journal db: %w", err)
	}

	j := &Journal{db: db, flushPending: xsync.NewCounter()}

	if err := j.initSchema(); err != nil {
		_ = db.Close()
		return nil, fmt.Errorf("init schema: %w", err)
	}

	if err := j.loadNextNodeID(); err != nil {
		_ = db.Close()
		return nil, fmt.Errorf("load next node id: %w", err)
	}

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

	if err := j.cleanOrphanEdges(); err != nil {
		_ = db.Close()
		return nil, fmt.Errorf("clean orphan edges: %w", err)
	}

	j.startWriteLoop()
	j.startFlushLoop()
	return j, nil
}

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

func (j *Journal) loadNextNodeID() error {
	val, closer, err := j.db.Get(nextNodeIDKey())
	if err == pebble.ErrNotFound {
		j.nextNodeID.Store(2)
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

func (j *Journal) persistNextNodeID() error {
	return j.db.Set(nextNodeIDKey(), encodeInt64(j.nextNodeID.Load()), pebble.Sync)
}

func (j *Journal) cleanOrphanEdges() error {
	prefix := []byte(prefixEdge)
	iter, err := j.db.NewIter(&pebble.IterOptions{
		LowerBound: prefix,
		UpperBound: []byte(prefixEdge + "\xff"),
	})
	if err != nil {
		return err
	}
	defer func() { _ = iter.Close() }()

	batch := j.db.NewBatch()
	defer func() { _ = batch.Close() }()

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

func (j *Journal) VerifyIntegrity() error {
	rootData, closer, err := j.db.Get(nodeKey(1))
	if err == pebble.ErrNotFound {
		return fmt.Errorf("root node missing")
	}
	if err != nil {
		return fmt.Errorf("root node check: %w", err)
	}
	_ = closer.Close()
	_ = rootData

	edgePrefixBytes := []byte(prefixEdge)
	iter, err := j.db.NewIter(&pebble.IterOptions{
		LowerBound: edgePrefixBytes,
		UpperBound: []byte(prefixEdge + "\xff"),
	})
	if err != nil {
		return fmt.Errorf("edge scan: %w", err)
	}
	defer func() { _ = iter.Close() }()

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

	xaPrefix := []byte(prefixXattr)
	xiter, err := j.db.NewIter(&pebble.IterOptions{
		LowerBound: xaPrefix,
		UpperBound: []byte(prefixXattr + "\xff"),
	})
	if err != nil {
		return fmt.Errorf("xattr scan: %w", err)
	}
	defer func() { _ = xiter.Close() }()

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

func (j *Journal) Close() error {
	close(j.writeQueue)
	<-j.writeDone

	if j.flushClose != nil {
		close(j.flushClose)
	}
	if j.flushDone != nil {
		<-j.flushDone
	}

	_ = j.persistNextNodeID()
	return j.db.Close()
}

func (j *Journal) allocNodeID() int64 {
	return j.nextNodeID.Add(1) - 1
}

func (j *Journal) tx(fn func(b *pebble.Batch) error) error {
	w := &journalWrite{fn: fn, done: make(chan error, 1)}
	j.writeQueue <- w
	return <-w.done
}

func (j *Journal) Sync() error {
	w := &journalWrite{fn: func(b *pebble.Batch) error { return nil }, done: make(chan error, 1)}
	j.writeQueue <- w
	err := <-w.done
	if err != nil {
		return err
	}
	return j.db.Flush()
}

func (j *Journal) startWriteLoop() {
	j.writeQueue = make(chan *journalWrite, 256)
	j.writeDone = make(chan struct{})
	go func() {
		defer close(j.writeDone)
		for w := range j.writeQueue {
			batch := j.db.NewBatch()
			err := w.fn(batch)
			if err != nil {
				_ = batch.Close()
				w.done <- err
				continue
			}
			if err := batch.Commit(pebble.Sync); err != nil {
				_ = batch.Close()
				w.done <- err
				continue
			}
			_ = batch.Close()
			j.flushPending.Add(1)
			w.done <- nil
			if j.flushPending.Value() >= 64 {
				_ = j.persistNextNodeID()
				if err := j.db.Flush(); err == nil {
					j.flushPending.Reset()
				}
			}
		}
	}()
}

func (j *Journal) startFlushLoop() {
	j.flushDone = make(chan struct{})
	j.flushClose = make(chan struct{})
	go func() {
		defer close(j.flushDone)
		ticker := time.NewTicker(5 * time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-j.flushClose:
				return
			case <-ticker.C:
				if j.flushPending.Value() > 0 {
					_ = j.persistNextNodeID()
					if err := j.db.Flush(); err == nil {
						j.flushPending.Reset()
					}
				}
			}
		}
	}()
}

func (j *Journal) GetNode(id int64) (*GraphNode, error) {
	data, closer, err := j.db.Get(nodeKey(id))
	if err == pebble.ErrNotFound {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	defer func() { _ = closer.Close() }()
	n := decodeNode(data, id)
	return n, nil
}

func (j *Journal) createNodeInBatch(b *pebble.Batch, n *GraphNode) (int64, error) {
	id := j.allocNodeID()
	n.ID = id
	if err := b.Set(nodeKey(id), encodeNode(n), nil); err != nil {
		return 0, err
	}
	return id, nil
}

func (j *Journal) updateNodeInBatch(b *pebble.Batch, n *GraphNode) error {
	return b.Set(nodeKey(n.ID), encodeNode(n), nil)
}

func (j *Journal) UpdateNode(n *GraphNode) error {
	return j.tx(func(b *pebble.Batch) error {
		return j.updateNodeInBatch(b, n)
	})
}

func (j *Journal) SetHasData(nodeID int64) error {
	return j.tx(func(b *pebble.Batch) error {
		data, closer, err := j.db.Get(nodeKey(nodeID))
		if err != nil {
			return err
		}
		defer func() { _ = closer.Close() }()
		n := decodeNode(data, nodeID)
		n.HasData = true
		return b.Set(nodeKey(nodeID), encodeNode(n), nil)
	})
}

func (j *Journal) ListEdges(parentID int64) ([]GraphEdge, error) {
	prefix := edgePrefix(parentID)
	iter, err := j.db.NewIter(&pebble.IterOptions{
		LowerBound: prefix,
		UpperBound: append(prefix[:len(prefix):len(prefix)], 0xFF),
	})
	if err != nil {
		return nil, err
	}
	defer func() { _ = iter.Close() }()

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

func (j *Journal) AddWhiteout(parentID int64, name string) error {
	return j.tx(func(b *pebble.Batch) error {
		return b.Set(whiteoutKey(parentID, name), nil, nil)
	})
}

func (j *Journal) ListWhiteouts(parentID int64) ([]string, error) {
	prefix := whiteoutPrefix(parentID)
	iter, err := j.db.NewIter(&pebble.IterOptions{
		LowerBound: prefix,
		UpperBound: append(prefix[:len(prefix):len(prefix)], 0xFF),
	})
	if err != nil {
		return nil, err
	}
	defer func() { _ = iter.Close() }()

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

func (j *Journal) GetXAttr(nodeID int64, name string) ([]byte, error) {
	val, closer, err := j.db.Get(xattrKey(nodeID, name))
	if err == pebble.ErrNotFound {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	defer func() { _ = closer.Close() }()
	cp := make([]byte, len(val))
	copy(cp, val)
	return cp, nil
}

func (j *Journal) ListXAttrs(nodeID int64) ([]string, error) {
	prefix := xattrPrefix(nodeID)
	iter, err := j.db.NewIter(&pebble.IterOptions{
		LowerBound: prefix,
		UpperBound: append(prefix[:len(prefix):len(prefix)], 0xFF),
	})
	if err != nil {
		return nil, err
	}
	defer func() { _ = iter.Close() }()

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

func (j *Journal) XAttrsForNode(nodeID int64) ([]format.XAttr, error) {
	prefix := xattrPrefix(nodeID)
	iter, err := j.db.NewIter(&pebble.IterOptions{
		LowerBound: prefix,
		UpperBound: append(prefix[:len(prefix):len(prefix)], 0xFF),
	})
	if err != nil {
		return nil, err
	}
	defer func() { _ = iter.Close() }()

	var xattrs []format.XAttr
	for iter.First(); iter.Valid(); iter.Next() {
		key := iter.Key()
		val := iter.Value()
		name := make([]byte, len(key)-11)
		copy(name, key[11:])
		value := make([]byte, len(val))
		copy(value, val)
		xattrs = append(xattrs, format.NewXAttr(name, value))
	}
	if err := iter.Error(); err != nil {
		return nil, err
	}
	return xattrs, nil
}

func (j *Journal) SetXAttr(nodeID int64, name string, value []byte) error {
	return j.tx(func(b *pebble.Batch) error {
		return b.Set(xattrKey(nodeID, name), value, nil)
	})
}

func (j *Journal) RemoveXAttr(nodeID int64, name string) error {
	return j.tx(func(b *pebble.Batch) error {
		return b.Delete(xattrKey(nodeID, name), nil)
	})
}

func (j *Journal) ResolvePath(path string) (nodeID int64, pxarPath string, fellOffAt int64, remaining string, err error) {
	if path == "/" || path == "" {
		return 1, "/", 0, "", nil
	}

	snap := j.db.NewSnapshot()
	defer func() { _ = snap.Close() }()

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

		edgeVal, edgeCloser, gerr := snap.Get(edgeKey(curID, part))
		if gerr == pebble.ErrNotFound {
			_, _, werr := snap.Get(whiteoutKey(curID, part))
			if werr != nil && werr != pebble.ErrNotFound {
				return 0, "", 0, "", werr
			}
			if werr == nil {
				return 0, "", 0, "", nil
			}
			return 0, pxarPrefix.String() + path[pos-1:], curID, path[pos:], nil
		}
		if gerr != nil {
			return 0, "", 0, "", gerr
		}
		childID := decodeInt64(edgeVal)
		_ = edgeCloser.Close()

		curID = childID

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

func (j *Journal) AllXAttrs() (map[int64]map[string][]byte, error) {
	prefix := []byte(prefixXattr)
	iter, err := j.db.NewIter(&pebble.IterOptions{
		LowerBound: prefix,
		UpperBound: []byte(prefixXattr + "\xff"),
	})
	if err != nil {
		return nil, err
	}
	defer func() { _ = iter.Close() }()

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

			_ = b.Delete(whiteoutKey(curParentID, name), nil)

			if i == len(parts)-1 {
				if err := b.Set(edgeKey(curParentID, name), encodeInt64(nodeID), nil); err != nil {
					return err
				}
				if whiteout {
					if err := b.Set(whiteoutKey(curParentID, name), nil, nil); err != nil {
						return err
					}
				}
			} else {
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

func (j *Journal) Clear() error {
	return j.tx(func(b *pebble.Batch) error {
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

func deletePrefix(b *pebble.Batch, prefix []byte) error {
	upper := make([]byte, len(prefix)+1)
	copy(upper, prefix)
	upper[len(prefix)] = 0xFF
	return b.DeleteRange(prefix, upper, nil)
}

func deletePrefixExcept(b *pebble.Batch, prefix []byte, exceptKey []byte) error {
	if err := b.DeleteRange(prefix, exceptKey, nil); err != nil {
		return err
	}
	nextKey := make([]byte, len(exceptKey)+1)
	copy(nextKey, exceptKey)
	nextKey[len(exceptKey)] = 0xFF
	upper := make([]byte, len(prefix)+1)
	copy(upper, prefix)
	upper[len(prefix)] = 0xFF
	return b.DeleteRange(nextKey, upper, nil)
}

func (j *Journal) DeleteEdgeAndNode(parentID int64, name string, nodeID int64, addWhiteout bool) error {
	return j.tx(func(b *pebble.Batch) error {
		if err := b.Delete(edgeKey(parentID, name), nil); err != nil {
			return err
		}
		if addWhiteout {
			if err := b.Set(whiteoutKey(parentID, name), nil, nil); err != nil {
				return err
			}
		}
		xaPrefix := xattrPrefix(nodeID)
		xaUpper := make([]byte, len(xaPrefix)+1)
		copy(xaUpper, xaPrefix)
		xaUpper[len(xaPrefix)] = 0xFF
		if err := b.DeleteRange(xaPrefix, xaUpper, nil); err != nil {
			return err
		}
		childEdgePrefix := edgePrefix(nodeID)
		childEdgeUpper := make([]byte, len(childEdgePrefix)+1)
		copy(childEdgeUpper, childEdgePrefix)
		childEdgeUpper[len(childEdgePrefix)] = 0xFF
		if err := b.DeleteRange(childEdgePrefix, childEdgeUpper, nil); err != nil {
			return err
		}
		return b.Delete(nodeKey(nodeID), nil)
	})
}

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

func (j *Journal) MoveEdgeAndWhiteout(oldParent int64, oldName string, newParent int64, newName string, replaceDestNode int64, whiteoutOld, whiteoutNew bool) error {
	return j.tx(func(b *pebble.Batch) error {
		_ = b.Delete(whiteoutKey(newParent, newName), nil)

		if replaceDestNode != 0 {
			if err := b.Delete(edgeKey(newParent, newName), nil); err != nil {
				return err
			}
			xaPrefix := xattrPrefix(replaceDestNode)
			xaUpper := make([]byte, len(xaPrefix)+1)
			copy(xaUpper, xaPrefix)
			xaUpper[len(xaPrefix)] = 0xFF
			if err := b.DeleteRange(xaPrefix, xaUpper, nil); err != nil {
				return err
			}
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

		srcEdgeVal, closer, err := j.db.Get(edgeKey(oldParent, oldName))
		if err != nil {
			return fmt.Errorf("move edge: source (%d, %q) not found: %w", oldParent, oldName, err)
		}
		childIDVal := make([]byte, len(srcEdgeVal))
		copy(childIDVal, srcEdgeVal)
		_ = closer.Close()

		if err := b.Delete(edgeKey(oldParent, oldName), nil); err != nil {
			return err
		}
		if err := b.Set(edgeKey(newParent, newName), childIDVal, nil); err != nil {
			return err
		}

		if whiteoutOld {
			if err := b.Set(whiteoutKey(oldParent, oldName), nil, nil); err != nil {
				return err
			}
		}
		if whiteoutNew {
			if err := b.Set(whiteoutKey(newParent, newName), nil, nil); err != nil {
				return err
			}
		}
		return nil
	})
}
