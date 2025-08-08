package utils

import "unsafe"

func BytesToString(b []byte) string {
	p := unsafe.SliceData(b)
	return unsafe.String(p, len(b))
}

func StringToBytes(s string) []byte {
	p := unsafe.StringData(s)
	b := unsafe.Slice(p, len(s))
	return b
}
