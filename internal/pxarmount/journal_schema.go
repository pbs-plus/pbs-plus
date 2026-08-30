package pxarmount

import (
	"encoding/binary"
	"fmt"
	"os"

	"github.com/cockroachdb/pebble"
	"github.com/cockroachdb/pebble/vfs"
	"github.com/pbs-plus/pbs-plus/internal/log"
)

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

	j := &Journal{db: db, overlay: make(map[string][]byte), commitCh: make(chan struct{}, 4), stopCh: make(chan struct{}), stopped: make(chan struct{}), drained: make(chan struct{})}

	if err := j.initSchema(); err != nil {
		if err := db.Close(); err != nil {
			log.Error(err, "")
		}
		return nil, fmt.Errorf("init schema: %w", err)
	}

	if err := j.loadNextNodeID(); err != nil {
		if err := db.Close(); err != nil {
			log.Error(err, "")
		}
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
			if err := db.Close(); err != nil {
				log.Error(err, "")
			}
			return nil, fmt.Errorf("create root node: %w", err)
		}
	} else if err != nil {
		if err := db.Close(); err != nil {
			log.Error(err, "")
		}
		return nil, fmt.Errorf("verify root node: %w", err)
	} else {
		if err := closer.Close(); err != nil {
			log.Error(err, "")
		}
		_ = rootData
	}

	if err := j.cleanOrphanEdges(); err != nil {
		if err := db.Close(); err != nil {
			log.Error(err, "")
		}
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
	if err := closer.Close(); err != nil {
		log.Error(err, "")
	}
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
	if err := closer.Close(); err != nil {
		log.Error(err, "")
	}
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
	defer func() {
		if err := iter.Close(); err != nil {
			log.Error(err, "")
		}
	}()

	batch := j.db.NewBatch()
	defer func() {
		if err := batch.Close(); err != nil {
			log.Error(err, "")
		}
	}()

	for iter.First(); iter.Valid(); iter.Next() {
		childIDVal := iter.Value()
		if len(childIDVal) < 8 {
			continue
		}
		childID := decodeInt64(childIDVal)
		_, closer, err := j.db.Get(nodeKey(childID))
		if err == pebble.ErrNotFound {
			if err := batch.Delete(iter.Key(), nil); err != nil {
				log.Error(err, "")
			}
		} else if err != nil {
			return err
		} else {
			if err := closer.Close(); err != nil {
				log.Error(err, "")
			}
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
	if err := closer.Close(); err != nil {
		log.Error(err, "")
	}
	_ = rootData

	edgePrefixBytes := []byte(prefixEdge)
	iter, err := j.db.NewIter(&pebble.IterOptions{
		LowerBound: edgePrefixBytes,
		UpperBound: []byte(prefixEdge + "\xff"),
	})
	if err != nil {
		return fmt.Errorf("edge scan: %w", err)
	}
	defer func() {
		if err := iter.Close(); err != nil {
			log.Error(err, "")
		}
	}()

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
		if err := closer.Close(); err != nil {
			log.Error(err, "")
		}
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
	defer func() {
		if err := xiter.Close(); err != nil {
			log.Error(err, "")
		}
	}()

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
		if err := closer.Close(); err != nil {
			log.Error(err, "")
		}
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
	if err := j.persistNextNodeID(); err != nil {
		log.Error(err, "")
	}
	commitErr := j.commitErr
	j.mu.Unlock()

	if closeErr := j.db.Close(); closeErr != nil {
		return closeErr
	}
	return commitErr
}

func (j *Journal) Clear() error {
	root := &GraphNode{
		ID:         1,
		Kind:       NodeDir,
		Mode:       16877,
		RedirectTo: "/",
	}
	rootKey := nodeKey(1)
	rootEnc := encodeNode(root)
	rootCsum := checksumKey(1)

	j.mu.Lock()
	j.overlay = make(map[string][]byte, 1)
	j.overlay[bytesToString(rootKey)] = rootEnc
	j.mu.Unlock()

	keys := []pebbleSet{
		{key: []byte(prefixWhiteout), deleteEnd: append([]byte(prefixWhiteout), 0xFF)},
		{key: []byte(prefixXattr), deleteEnd: append([]byte(prefixXattr), 0xFF)},
		{key: []byte(prefixEdge), deleteEnd: append([]byte(prefixEdge), 0xFF)},
		{key: rootKey, value: rootEnc},
		{key: rootCsum, value: encodeUint32(fnv32(rootEnc))},
		{key: append(append([]byte(nil), rootKey...), 0xFF), deleteEnd: append([]byte(prefixNode), 0xFF)},
		{key: append(append([]byte(nil), rootCsum...), 0xFF), deleteEnd: append([]byte(prefixCsum), 0xFF)},
	}

	if err := j.tx(keys...); err != nil {
		return err
	}
	return j.Sync()
}
