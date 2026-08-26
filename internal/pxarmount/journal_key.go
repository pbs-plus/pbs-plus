package pxarmount

import (
	"encoding/binary"
)

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

func checksumKey(id int64) []byte {
	b := make([]byte, 10)
	copy(b, prefixCsum)
	binary.BigEndian.PutUint64(b[2:], uint64(id))
	return b
}
