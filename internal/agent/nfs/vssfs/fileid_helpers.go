package vssfs

import (
	"github.com/zeebo/xxh3"
)

func generateFullPathID(path string) uint64 {
	return xxh3.HashString(path)
}

// quickMatch recomputes the hash and compares it.
func quickMatch(id uint64, path string) bool {
	return generateFullPathID(path) == id
}
