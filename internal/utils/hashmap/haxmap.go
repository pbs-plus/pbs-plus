package hashmap

import (
	"encoding/binary"

	"github.com/alphadose/haxmap"
	"github.com/zeebo/xxh3"
)

func New[V any]() *haxmap.Map[string, V] {
	m := haxmap.New[string, V]()
	m.SetHasher(func(k string) uintptr {
		return uintptr(xxh3.HashString(k))
	})

	return m
}

func NewUint64[V any]() *haxmap.Map[uint64, V] {
	m := haxmap.New[uint64, V]()
	m.SetHasher(func(k uint64) uintptr {
		b := make([]byte, 8)
		binary.LittleEndian.PutUint64(b, k)
		return uintptr(xxh3.Hash(b))
	})

	return m
}
