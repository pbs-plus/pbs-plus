//go:build !windows

package binswap

import (
	"fmt"
	"runtime"
)

func CheckBinaryMagic(data []byte) error {
	if len(data) < 4 {
		return fmt.Errorf("binary too small (%d bytes)", len(data))
	}
	switch runtime.GOOS {
	case "darwin":
		magic := uint32(data[0])<<24 | uint32(data[1])<<16 | uint32(data[2])<<8 | uint32(data[3])
		switch magic {
		case 0xfeedface, 0xfeedfacf, 0xcefaedfe, 0xcffaedfe, 0xcafebabe:
			return nil
		default:
			return fmt.Errorf("missing Mach-O magic")
		}
	default:
		if data[0] != 0x7f || data[1] != 'E' || data[2] != 'L' || data[3] != 'F' {
			return fmt.Errorf("missing ELF magic")
		}
	}
	return nil
}
