package pxarmount

import (
	"fmt"
	"slices"
	"strings"

	"github.com/cockroachdb/pebble"
	"github.com/pbs-plus/pbs-plus/internal/log"
)

func (j *Journal) overlayGet(key []byte) ([]byte, bool) {
	v, ok := j.overlay[bytesToString(key)]
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
	defer func() {
		if err := closer.Close(); err != nil {
			log.Error(err, "")
		}
	}()

	if err := j.verifyChecksum(id, data); err != nil {
		return nil, err
	}

	return decodeNode(data, id), nil
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
	defer func() {
		if err := closer.Close(); err != nil {
			log.Error(err, "")
		}
	}()

	if err := j.verifyChecksum(id, data); err != nil {
		return nil, err
	}

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
	defer func() {
		if err := closer.Close(); err != nil {
			log.Error(err, "")
		}
	}()
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
	if err := closer.Close(); err != nil {
		log.Error(err, "")
	}
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
	defer func() {
		if err := iter.Close(); err != nil {
			log.Error(err, "")
		}
	}()

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
	defer func() {
		if err := iter.Close(); err != nil {
			log.Error(err, "")
		}
	}()

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

func (j *Journal) ResolvePath(path string) (nodeID int64, pxarPath string, fellOffAt int64, remaining string, err error) {
	if path == "/" || path == "" {
		return 1, "/", 0, "", nil
	}

	j.mu.RLock()
	defer j.mu.RUnlock()

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

	for _, s := range keys {
		if s.delete {
			j.overlay[bytesToString(s.key)] = nil
		} else {
			j.overlay[bytesToString(s.key)] = s.value
		}
	}
	j.pushPendingMany(keys)

	drain := len(j.pending) >= 64
	j.mu.Unlock()

	if drain {
		select {
		case j.commitCh <- struct{}{}:
		default:
		}
	}

	return nodeID, nil
}

func (j *Journal) DeleteEdgeAndNode(parentID int64, name string, nodeID int64, addWhiteout bool) error {
	keys := make([]pebbleSet, 0, 6)
	keys = append(keys, pebbleSet{key: edgeKey(parentID, name), delete: true})
	if addWhiteout {
		keys = append(keys, pebbleSet{key: whiteoutKey(parentID, name), value: []byte{1}})
	}
	keys = append(keys, pebbleSet{key: nodeKey(nodeID), delete: true})
	keys = append(keys, pebbleSet{key: checksumKey(nodeID), delete: true})

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
	keys := make([]pebbleSet, 0, 3)
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
	keys := make([]pebbleSet, 0, 8)
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
		keys = append(keys, pebbleSet{key: checksumKey(replaceDestNode), delete: true})
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
