package types

type StatFS struct {
	Bsize   uint64 // Block size
	Blocks  uint64 // Total blocks
	Bfree   uint64 // Free blocks
	Bavail  uint64 // Available blocks
	Files   uint64 // Total files/inodes
	Ffree   uint64 // Free files/inodes
	NameLen uint64 // Maximum name length
}

