package hashmap

import (
	"unsafe"

	"github.com/alphadose/haxmap"
	"github.com/zeebo/xxh3"
	"golang.org/x/exp/constraints"
)

type (
	hashable interface {
		constraints.Integer | constraints.Float | constraints.Complex | ~string | uintptr | ~unsafe.Pointer
	}
)

func New[K hashable, V any]() *haxmap.Map[K, V] {
	m := haxmap.New[K, V]()
	m.SetHasher(func(k K) uintptr {
		// Get the number of bytes used by k.
		size := unsafe.Sizeof(k)
		// Convert the address of k to a *byte pointer and use unsafe.Slice to get
		// a byte slice representing its memory.
		data := unsafe.Slice((*byte)(unsafe.Pointer(&k)), size)
		return uintptr(xxh3.Hash(data))
	})

	return m
}
