package pxarmount

import (
	"encoding/binary"
	"fmt"
	"unsafe"

	"github.com/cockroachdb/pebble"
	"github.com/pbs-plus/pbs-plus/internal/log"
)

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

func (j *Journal) verifyChecksum(id int64, data []byte) error {
	csumData, closer, err := j.db.Get(checksumKey(id))
	if err == pebble.ErrNotFound {
		return nil
	}
	if err != nil {
		return err
	}
	defer func() {
		if err := closer.Close(); err != nil {
			log.Error(err, "")
		}
	}()
	if len(csumData) < 4 {
		return nil
	}
	expected := binary.LittleEndian.Uint32(csumData)
	actual := fnv32(data)
	if expected != actual {
		return fmt.Errorf("checksum mismatch for node %d: expected %08x, got %08x", id, expected, actual)
	}
	return nil
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

func fnv32(data []byte) uint32 {
	h := uint32(2166136261)
	for _, b := range data {
		h ^= uint32(b)
		h *= 16777619
	}
	return h
}

func encodeUint32(v uint32) []byte {
	b := make([]byte, 4)
	binary.LittleEndian.PutUint32(b, v)
	return b
}

func encodeInt64(v int64) []byte {
	b := make([]byte, 8)
	binary.LittleEndian.PutUint64(b, uint64(v))
	return b
}

func decodeInt64(b []byte) int64 {
	return int64(binary.LittleEndian.Uint64(b))
}
