//go:build windows

package binswap

import (
	"fmt"
)

func CheckBinaryMagic(data []byte) error {
	if len(data) < 2 {
		return fmt.Errorf("binary too small (%d bytes)", len(data))
	}
	if data[0] != 'M' || data[1] != 'Z' {
		return fmt.Errorf("missing PE (MZ) magic")
	}
	return nil
}
