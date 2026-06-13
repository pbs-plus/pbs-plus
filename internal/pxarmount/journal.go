package pxarmount

import (
	"encoding/binary"
	"fmt"
	"os"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"time"
	"unsafe"

	"github.com/cockroachdb/pebble"
	"github.com/cockroachdb/pebble/vfs"
	"github.com/pbs-plus/pxar/format"
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

type journalOp struct {
	s    pebbleSet
	keys []pebbleSet
}

type pebbleSet struct {
	key       []byte
	value     []byte
	delete    bool
	deleteEnd []byte
}

const pendingRingCap = 256

type Journal struct {
	db         *pebble.DB
	mu         sync.RWMutex
	nextNodeID atomic.Int64

	overlay     map[string][]byte
	pendingRing [pendingRingCap]journalOp
	pendingHead atomic.Uint64
	pendingTail uint64
	commitErr   error

	commitCh chan struct{}
	stopCh   chan struct{}
	stopped  chan struct{}
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

func (j *Journal) pushPendingOne(s pebbleSet) {
	h := j.pendingHead.Add(1) - 1
	j.pendingRing[h%pendingRingCap] = journalOp{s: s}
}

func (j *Journal) pushPendingMany(keys []pebbleSet) {
	h := j.pendingHead.Add(1) - 1
	j.pendingRing[h%pendingRingCap] = journalOp{keys: keys}
}

func bytesToString(b []byte) string {
	if len(b) == 0 {
		return ""
	}
	return unsafe.String(&b[0], len(b))
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

	j := &Journal{db: db, overlay: make(map[string][]byte), commitCh: make(chan struct{}, 1), stopCh: make(chan struct{}), stopped: make(chan struct{})}

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

	go j.commitLoop()
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

func (j *Journal) allocNodeID() int64 {
	return j.nextNodeID.Add(1) - 1
}

func (j *Journal) Close() error {
	close(j.stopCh)
	<-j.stopped

	j.mu.Lock()
	_ = j.persistNextNodeID()
	commitErr := j.commitErr
	j.mu.Unlock()

	if closeErr := j.db.Close(); closeErr != nil {
		return closeErr
	}
	return commitErr
}

func (j *Journal) txOne(s pebbleSet) error {
	j.mu.Lock()
	if s.deleteEnd != nil {
		ks, ke := bytesToString(s.key), bytesToString(s.deleteEnd)
		for k := range j.overlay {
			if k >= ks && k < ke {
				delete(j.overlay, k)
			}
		}
	} else if s.delete {
		j.overlay[bytesToString(s.key)] = nil
	} else {
		j.overlay[bytesToString(s.key)] = s.value
	}
	j.pushPendingOne(s)

	drain := j.pendingHead.Load()-j.pendingTail >= 64
	j.mu.Unlock()

	if drain {
		select {
		case j.commitCh <- struct{}{}:
		default:
		}
	}

	return nil
}

func (j *Journal) tx(keys ...pebbleSet) error {
	j.mu.Lock()
	for _, s := range keys {
		if s.deleteEnd != nil {
			ks, ke := bytesToString(s.key), bytesToString(s.deleteEnd)
			for k := range j.overlay {
				if k >= ks && k < ke {
					delete(j.overlay, k)
				}
			}
		} else if s.delete {
			j.overlay[bytesToString(s.key)] = nil
		} else {
			j.overlay[bytesToString(s.key)] = s.value
		}
	}
	if len(keys) == 1 {
		j.pushPendingOne(keys[0])
	} else {
		j.pushPendingMany(keys)
	}

	drain := j.pendingHead.Load()-j.pendingTail >= 64
	j.mu.Unlock()

	if drain {
		select {
		case j.commitCh <- struct{}{}:
		default:
		}
	}

	return nil
}

func (j *Journal) Sync() error {
	done := make(chan struct{})
	j.mu.Lock()
	j.pushPendingOne(pebbleSet{})
	j.mu.Unlock()

	go func() {
		for {
			j.mu.Lock()
			hasPending := j.pendingHead.Load() > j.pendingTail
			j.mu.Unlock()
			if !hasPending {
				close(done)
				return
			}
			select {
			case j.commitCh <- struct{}{}:
			default:
			}
			time.Sleep(time.Millisecond)
		}
	}()

	select {
	case j.commitCh <- struct{}{}:
	default:
	}

	<-done

	j.mu.Lock()
	err := j.commitErr
	j.commitErr = nil
	j.mu.Unlock()
	if err != nil {
		return err
	}

	j.mu.Lock()
	defer j.mu.Unlock()
	return j.persistNextNodeID()
}

func (j *Journal) commitLoop() {
	defer close(j.stopped)
	ticker := time.NewTicker(1 * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case <-j.stopCh:
			j.drainAllLocked()
			return
		case <-ticker.C:
			j.drainAllLocked()
		case <-j.commitCh:
			j.drainAllLocked()
		}
	}
}

func (j *Journal) drainAllLocked() {
	j.mu.Lock()
	if j.pendingHead.Load() == j.pendingTail {
		j.mu.Unlock()
		return
	}
	tail := j.pendingTail
	head := j.pendingHead.Load()
	j.pendingTail = head
	j.overlay = make(map[string][]byte)
	j.mu.Unlock()

	pb := j.db.NewBatch()
	for i := tail; i < head; i++ {
		op := j.pendingRing[i%pendingRingCap]
		if len(op.keys) > 0 {
			for _, s := range op.keys {
				if s.deleteEnd != nil {
					_ = pb.DeleteRange(s.key, s.deleteEnd, nil)
				} else if s.delete {
					_ = pb.Delete(s.key, nil)
				} else {
					_ = pb.Set(s.key, s.value, nil)
				}
			}
		} else {
			s := op.s
			if s.deleteEnd != nil {
				_ = pb.DeleteRange(s.key, s.deleteEnd, nil)
			} else if s.delete {
				_ = pb.Delete(s.key, nil)
			} else {
				_ = pb.Set(s.key, s.value, nil)
			}
		}
	}

	err := pb.Commit(pebble.Sync)
	_ = pb.Close()
	if err != nil {
		j.mu.Lock()
		j.commitErr = err
		j.mu.Unlock()
	}
}

func (j *Journal) overlayGet(key []byte) ([]byte, bool) {
	v, ok := j.overlay[string(key)]
	if !ok {
		return nil, false
	}
	if v == nil {
		return nil, true
	}
	return v, true
}

func (j *Journal) GetNode(id int64) (*GraphNode, error) {
	j.mu.RLock()
	if v, ok := j.overlayGet(nodeKey(id)); ok {
		j.mu.RUnlock()
		if v == nil {
			return nil, nil
		}
		return decodeNode(v, id), nil
	}
	j.mu.RUnlock()

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

func (j *Journal) createNodeInBatch(keys *[]pebbleSet, n *GraphNode) (int64, error) {
	id := j.allocNodeID()
	n.ID = id
	*keys = append(*keys, pebbleSet{key: nodeKey(id), value: encodeNode(n)})
	return id, nil
}

func (j *Journal) UpdateNode(n *GraphNode) error {
	return j.txOne(pebbleSet{key: nodeKey(n.ID), value: encodeNode(n)})
}

func (j *Journal) SetHasData(nodeID int64) error {
	j.mu.Lock()
	n, err := j.getNodeLocked(nodeID)
	j.mu.Unlock()
	if err != nil {
		return err
	}
	if n == nil {
		return fmt.Errorf("SetHasData: node %d not found", nodeID)
	}

	n.HasData = true
	return j.txOne(pebbleSet{key: nodeKey(nodeID), value: encodeNode(n)})
}

func (j *Journal) getNodeLocked(id int64) (*GraphNode, error) {
	if v, ok := j.overlayGet(nodeKey(id)); ok {
		if v == nil {
			return nil, nil
		}
		return decodeNode(v, id), nil
	}
	data, closer, err := j.db.Get(nodeKey(id))
	if err == pebble.ErrNotFound {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	defer func() { _ = closer.Close() }()
	return decodeNode(data, id), nil
}

func (j *Journal) getEdgeLocked(parentID int64, name string) (int64, bool, error) {
	k := edgeKey(parentID, name)
	if v, ok := j.overlayGet(k); ok {
		if v == nil {
			return 0, false, nil
		}
		return decodeInt64(v), true, nil
	}
	val, closer, err := j.db.Get(k)
	if err == pebble.ErrNotFound {
		return 0, false, nil
	}
	if err != nil {
		return 0, false, err
	}
	defer func() { _ = closer.Close() }()
	return decodeInt64(val), true, nil
}

func (j *Journal) getWhiteoutLocked(parentID int64, name string) (bool, error) {
	k := whiteoutKey(parentID, name)
	if v, ok := j.overlayGet(k); ok {
		return v != nil, nil
	}
	_, closer, err := j.db.Get(k)
	if err == pebble.ErrNotFound {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	_ = closer.Close()
	return true, nil
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

	j.mu.RLock()
	overlayDeletes := make(map[string]bool)
	overlayAdds := make(map[string]int64)
	for k, v := range j.overlay {
		if !strings.HasPrefix(k, string(prefix)) {
			continue
		}
		name := k[11:]
		if v == nil {
			overlayDeletes[name] = true
		} else {
			overlayAdds[name] = decodeInt64(v)
		}
	}
	j.mu.RUnlock()

	var edges []GraphEdge
	for iter.First(); iter.Valid(); iter.Next() {
		key := iter.Key()
		name := string(key[11:])
		if overlayDeletes[name] {
			continue
		}
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

	for name, childID := range overlayAdds {
		if overlayDeletes[name] {
			continue
		}
		found := false
		for i := range edges {
			if edges[i].Name == name {
				edges[i].ChildID = childID
				found = true
				break
			}
		}
		if !found {
			edges = append(edges, GraphEdge{ParentID: parentID, Name: name, ChildID: childID})
		}
	}

	slices.SortFunc(edges, func(a, b GraphEdge) int {
		if a.Name < b.Name {
			return -1
		}
		if a.Name > b.Name {
			return 1
		}
		return 0
	})

	return edges, nil
}

func (j *Journal) AddWhiteout(parentID int64, name string) error {
	return j.txOne(pebbleSet{key: whiteoutKey(parentID, name), value: []byte{1}})
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

	j.mu.RLock()
	overlayDeletes := make(map[string]bool)
	overlayAdds := make(map[string]bool)
	for k, v := range j.overlay {
		if !strings.HasPrefix(k, string(prefix)) {
			continue
		}
		name := k[11:]
		if v == nil {
			overlayDeletes[name] = true
		} else {
			overlayAdds[name] = true
		}
	}
	j.mu.RUnlock()

	var names []string
	for iter.First(); iter.Valid(); iter.Next() {
		key := iter.Key()
		name := string(key[11:])
		if overlayDeletes[name] {
			continue
		}
		names = append(names, name)
	}
	if err := iter.Error(); err != nil {
		return nil, err
	}

	for name := range overlayAdds {
		if overlayDeletes[name] {
			continue
		}
		found := slices.Contains(names, name)
		if !found {
			names = append(names, name)
		}
	}

	return names, nil
}

func (j *Journal) GetXAttr(nodeID int64, name string) ([]byte, error) {
	j.mu.RLock()
	if v, ok := j.overlayGet(xattrKey(nodeID, name)); ok {
		j.mu.RUnlock()
		if v == nil {
			return nil, nil
		}
		cp := make([]byte, len(v))
		copy(cp, v)
		return cp, nil
	}
	j.mu.RUnlock()

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

	j.mu.RLock()
	overlayDeletes := make(map[string]bool)
	overlayAdds := make(map[string]bool)
	for k, v := range j.overlay {
		if !strings.HasPrefix(k, string(prefix)) {
			continue
		}
		name := k[11:]
		if v == nil {
			overlayDeletes[name] = true
		} else {
			overlayAdds[name] = true
		}
	}
	j.mu.RUnlock()

	var names []string
	for iter.First(); iter.Valid(); iter.Next() {
		key := iter.Key()
		name := string(key[11:])
		if overlayDeletes[name] {
			continue
		}
		names = append(names, name)
	}
	if err := iter.Error(); err != nil {
		return nil, err
	}

	for name := range overlayAdds {
		if overlayDeletes[name] {
			continue
		}
		found := slices.Contains(names, name)
		if !found {
			names = append(names, name)
		}
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

	j.mu.RLock()
	overlayDeletes := make(map[string]bool)
	overlayAdds := make(map[string][]byte)
	for k, v := range j.overlay {
		if !strings.HasPrefix(k, string(prefix)) {
			continue
		}
		name := k[11:]
		if v == nil {
			overlayDeletes[name] = true
		} else {
			overlayAdds[name] = v
		}
	}
	j.mu.RUnlock()

	var xattrs []format.XAttr
	for iter.First(); iter.Valid(); iter.Next() {
		key := iter.Key()
		name := string(key[11:])
		if overlayDeletes[name] {
			continue
		}
		val := iter.Value()
		value := make([]byte, len(val))
		copy(value, val)
		xattrs = append(xattrs, format.NewXAttr([]byte(name), value))
	}
	if err := iter.Error(); err != nil {
		return nil, err
	}

	for name, v := range overlayAdds {
		if overlayDeletes[name] {
			continue
		}
		found := false
		for _, x := range xattrs {
			if string(x.Name()) == name {
				found = true
				break
			}
		}
		if !found {
			val := make([]byte, len(v))
			copy(val, v)
			xattrs = append(xattrs, format.NewXAttr([]byte(name), val))
		}
	}

	return xattrs, nil
}

func (j *Journal) SetXAttr(nodeID int64, name string, value []byte) error {
	return j.txOne(pebbleSet{key: xattrKey(nodeID, name), value: value})
}

func (j *Journal) RemoveXAttr(nodeID int64, name string) error {
	return j.txOne(pebbleSet{key: xattrKey(nodeID, name), delete: true})
}

func (j *Journal) ResolvePath(path string) (nodeID int64, pxarPath string, fellOffAt int64, remaining string, err error) {
	if path == "/" || path == "" {
		return 1, "/", 0, "", nil
	}

	j.mu.RLock()
	defer j.mu.RUnlock()

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

		childID, found, gerr := j.getEdgeLocked(curID, part)
		if gerr != nil {
			return 0, "", 0, "", gerr
		}
		if !found {
			wo, werr := j.getWhiteoutLocked(curID, part)
			if werr != nil {
				return 0, "", 0, "", werr
			}
			if wo {
				return 0, "", 0, "", nil
			}
			return 0, pxarPrefix.String() + path[pos-1:], curID, path[pos:], nil
		}
		curID = childID

		n, nerr := j.getNodeLocked(curID)
		if nerr != nil {
			return 0, "", 0, "", nerr
		}
		if n == nil {
			return 0, "", 0, "", fmt.Errorf("resolve: node %d missing", curID)
		}

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

	j.mu.RLock()
	overlayDeletes := make(map[string]bool)
	overlayAdds := make(map[string]struct {
		nodeID int64
		value  []byte
	})
	for k, v := range j.overlay {
		if !strings.HasPrefix(k, string(prefix)) {
			continue
		}
		nodeID := int64(binary.BigEndian.Uint64([]byte(k[2:10])))
		name := k[11:]
		fullKey := fmt.Sprintf("%d:%s", nodeID, name)
		if v == nil {
			overlayDeletes[fullKey] = true
		} else {
			overlayAdds[fullKey] = struct {
				nodeID int64
				value  []byte
			}{nodeID, v}
		}
	}
	j.mu.RUnlock()

	result := make(map[int64]map[string][]byte)
	for iter.First(); iter.Valid(); iter.Next() {
		key := iter.Key()
		nodeID := int64(binary.BigEndian.Uint64(key[2:10]))
		name := string(key[11:])
		fullKey := fmt.Sprintf("%d:%s", nodeID, name)
		if overlayDeletes[fullKey] {
			continue
		}
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

	for fullKey, add := range overlayAdds {
		if overlayDeletes[fullKey] {
			continue
		}
		nodeID := add.nodeID
		if result[nodeID] == nil {
			result[nodeID] = make(map[string][]byte)
		}
		parts := strings.SplitN(fullKey, ":", 2)
		if len(parts) == 2 {
			cp := make([]byte, len(add.value))
			copy(cp, add.value)
			result[nodeID][parts[1]] = cp
		}
	}

	return result, nil
}

func (j *Journal) EnsureNodePath(path string, n *GraphNode, whiteout bool) (int64, error) {
	var keys []pebbleSet
	id, err := j.createNodeInBatch(&keys, n)
	if err != nil {
		return 0, err
	}
	nodeID := id

	j.mu.Lock()

	parts := splitPath(path)
	curParentID := int64(1)

	for i, name := range parts {
		if name == "" {
			continue
		}

		childID, found, gerr := j.getEdgeLocked(curParentID, name)
		if gerr != nil {
			j.mu.Unlock()
			return 0, gerr
		}
		if found {
			curParentID = childID
			continue
		}

		keys = append(keys, pebbleSet{key: whiteoutKey(curParentID, name), delete: true})

		if i == len(parts)-1 {
			keys = append(keys, pebbleSet{key: edgeKey(curParentID, name), value: encodeInt64(nodeID)})
			if whiteout {
				keys = append(keys, pebbleSet{key: whiteoutKey(curParentID, name), value: []byte{1}})
			}
		} else {
			var intermediatePath strings.Builder
			intermediatePath.WriteByte('/')
			for jj := 0; jj <= i; jj++ {
				if jj > 0 {
					intermediatePath.WriteByte('/')
				}
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
			midID, cerr := j.createNodeInBatch(&keys, intermediate)
			if cerr != nil {
				j.mu.Unlock()
				return 0, cerr
			}
			keys = append(keys, pebbleSet{key: edgeKey(curParentID, name), value: encodeInt64(midID)})
			curParentID = midID
		}
	}

	j.mu.Unlock()

	return nodeID, j.tx(keys...)
}

func (j *Journal) Clear() error {
	j.mu.Lock()
	j.overlay = make(map[string][]byte)
	root := &GraphNode{
		ID:         1,
		Kind:       NodeDir,
		Mode:       16877,
		RedirectTo: "/",
	}
	j.overlay[string(nodeKey(1))] = encodeNode(root)
	j.mu.Unlock()

	var keys []pebbleSet
	keys = append(keys, pebbleSet{key: []byte(prefixWhiteout), deleteEnd: append([]byte(prefixWhiteout), 0xFF)})
	keys = append(keys, pebbleSet{key: []byte(prefixXattr), deleteEnd: append([]byte(prefixXattr), 0xFF)})
	keys = append(keys, pebbleSet{key: []byte(prefixEdge), deleteEnd: append([]byte(prefixEdge), 0xFF)})
	keys = append(keys, pebbleSet{key: nodeKey(1), value: encodeNode(root)})

	nextKey := make([]byte, len(nodeKey(1))+1)
	copy(nextKey, nodeKey(1))
	nextKey[len(nodeKey(1))] = 0xFF
	nodeUpper := append([]byte(prefixNode), 0xFF)
	keys = append(keys, pebbleSet{key: nextKey, deleteEnd: nodeUpper})

	return j.tx(keys...)
}

func (j *Journal) DeleteEdgeAndNode(parentID int64, name string, nodeID int64, addWhiteout bool) error {
	var keys []pebbleSet
	keys = append(keys, pebbleSet{key: edgeKey(parentID, name), delete: true})
	if addWhiteout {
		keys = append(keys, pebbleSet{key: whiteoutKey(parentID, name), value: []byte{1}})
	}
	keys = append(keys, pebbleSet{key: nodeKey(nodeID), delete: true})

	xaPrefix := xattrPrefix(nodeID)
	xaUpper := make([]byte, len(xaPrefix)+1)
	copy(xaUpper, xaPrefix)
	xaUpper[len(xaPrefix)] = 0xFF
	keys = append(keys, pebbleSet{key: xaPrefix, deleteEnd: xaUpper})

	childEdgePrefix := edgePrefix(nodeID)
	childEdgeUpper := make([]byte, len(childEdgePrefix)+1)
	copy(childEdgeUpper, childEdgePrefix)
	childEdgeUpper[len(childEdgePrefix)] = 0xFF
	keys = append(keys, pebbleSet{key: childEdgePrefix, deleteEnd: childEdgeUpper})

	return j.tx(keys...)
}

func (j *Journal) CreateNodeEdgeAndWhiteout(parentID int64, name string, n *GraphNode, whiteout bool) (int64, error) {
	var keys []pebbleSet
	id, err := j.createNodeInBatch(&keys, n)
	if err != nil {
		return 0, err
	}
	keys = append(keys, pebbleSet{key: edgeKey(parentID, name), value: encodeInt64(id)})
	if whiteout {
		keys = append(keys, pebbleSet{key: whiteoutKey(parentID, name), value: []byte{1}})
	}
	return id, j.tx(keys...)
}

func (j *Journal) MoveEdgeAndWhiteout(oldParent int64, oldName string, newParent int64, newName string, replaceDestNode int64, whiteoutOld, whiteoutNew bool) error {
	var keys []pebbleSet
	keys = append(keys, pebbleSet{key: whiteoutKey(newParent, newName), delete: true})

	if replaceDestNode != 0 {
		keys = append(keys, pebbleSet{key: edgeKey(newParent, newName), delete: true})
		xaPrefix := xattrPrefix(replaceDestNode)
		xaUpper := make([]byte, len(xaPrefix)+1)
		copy(xaUpper, xaPrefix)
		xaUpper[len(xaPrefix)] = 0xFF
		keys = append(keys, pebbleSet{key: xaPrefix, deleteEnd: xaUpper})
		childEdgePrefix := edgePrefix(replaceDestNode)
		childEdgeUpper := make([]byte, len(childEdgePrefix)+1)
		copy(childEdgeUpper, childEdgePrefix)
		childEdgeUpper[len(childEdgePrefix)] = 0xFF
		keys = append(keys, pebbleSet{key: childEdgePrefix, deleteEnd: childEdgeUpper})
		keys = append(keys, pebbleSet{key: nodeKey(replaceDestNode), delete: true})
	}

	j.mu.Lock()
	childID, found, err := j.getEdgeLocked(oldParent, oldName)
	if err != nil {
		j.mu.Unlock()
		return fmt.Errorf("move edge: source (%d, %q) not found: %w", oldParent, oldName, err)
	}
	if !found {
		j.mu.Unlock()
		return fmt.Errorf("move edge: source (%d, %q) not found", oldParent, oldName)
	}
	j.mu.Unlock()

	keys = append(keys, pebbleSet{key: edgeKey(oldParent, oldName), delete: true})
	keys = append(keys, pebbleSet{key: edgeKey(newParent, newName), value: encodeInt64(childID)})

	if whiteoutOld {
		keys = append(keys, pebbleSet{key: whiteoutKey(oldParent, oldName), value: []byte{1}})
	}
	if whiteoutNew {
		keys = append(keys, pebbleSet{key: whiteoutKey(newParent, newName), value: []byte{1}})
	}

	return j.tx(keys...)
}
