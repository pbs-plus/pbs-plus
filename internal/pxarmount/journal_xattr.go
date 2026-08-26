package pxarmount

import (
	"encoding/binary"
	"fmt"
	"slices"
	"strings"

	"github.com/cockroachdb/pebble"
	"github.com/pbs-plus/pbs-plus/internal/log"
	"github.com/pbs-plus/pxar/format"
)

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
	defer func() {
		if err := closer.Close(); err != nil {
			log.Error(err, "")
		}
	}()
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

func (j *Journal) XAttrsForNode(nodeID int64) ([]format.XAttr, error) {
	prefix := xattrPrefix(nodeID)
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

// because go-fuse reuses its data buffer across FUSE operations.

// because go-fuse reuses its data buffer across FUSE operations.
func (j *Journal) SetXAttr(nodeID int64, name string, value []byte) error {
	cp := make([]byte, len(value))
	copy(cp, value)
	return j.txOne(pebbleSet{key: xattrKey(nodeID, name), value: cp})
}

func (j *Journal) RemoveXAttr(nodeID int64, name string) error {
	return j.txOne(pebbleSet{key: xattrKey(nodeID, name), delete: true})
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
	defer func() {
		if err := iter.Close(); err != nil {
			log.Error(err, "")
		}
	}()

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
