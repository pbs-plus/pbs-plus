//go:build linux && arm64

package pxarmount

// statNlink fits fuse's uint32 nlink into Stat_t.Nlink (uint32 on linux/arm64).
func statNlink(n uint32) uint32 { return n }
