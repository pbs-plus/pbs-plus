//go:build !(linux && arm64)

package pxarmount

// statNlink fits fuse's uint32 nlink into Stat_t.Nlink (uint64 except linux/arm64).
func statNlink(n uint32) uint64 { return uint64(n) }
