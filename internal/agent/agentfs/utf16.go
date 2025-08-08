package agentfs

import (
	"unicode/utf8"
)

func isHigh(r rune) bool {
	return r >= 0xd800 && r <= 0xdbff
}

func isLow(r rune) bool {
	return r >= 0xdc00 && r <= 0xdfff
}

// DecodeBytes decodes UTF-16 code units into UTF-8 bytes without extra allocations.
func UTF16DecodeBytes(p []uint16) []byte {
	// Preallocate enough space to avoid most reallocations
	// Worst case: 3 bytes per code unit (ASCII is 1 byte, BMP is up to 3 bytes)
	s := make([]byte, 0, len(p)*3)

	for i := 0; i < len(p); i++ {
		r := rune(0xfffd) // replacement char
		r1 := rune(p[i])

		if isHigh(r1) {
			if i+1 < len(p) {
				r2 := rune(p[i+1])
				if isLow(r2) {
					i++
					r = 0x10000 + (r1-0xd800)<<10 + (r2 - 0xdc00)
				}
			}
		} else if !isLow(r1) {
			r = r1
		}

		s = utf8.AppendRune(s, r)
	}

	return s
}
