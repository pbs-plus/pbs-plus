//go:build linux

package arpcfs

import (
	"archive/zip"
	"bytes"
	"encoding/binary"
	"hash/crc32"
	"testing"
)

// buildEmptySevenZip hand-rolls the 32-byte signature header; sevenzip has no writer.
func buildEmptySevenZip() []byte {
	b := make([]byte, 32)
	copy(b, []byte{'7', 'z', 0xBC, 0xAF, 0x27, 0x1C, 0x00, 0x04})
	binary.LittleEndian.PutUint32(b[8:], crc32.ChecksumIEEE(b[12:32]))
	return b
}

func buildEmptyZip(t *testing.T, comment string) []byte {
	t.Helper()
	var buf bytes.Buffer
	zw := zip.NewWriter(&buf)
	if comment != "" {
		if err := zw.SetComment(comment); err != nil {
			t.Fatal(err)
		}
	}
	if err := zw.Close(); err != nil {
		t.Fatal(err)
	}
	return buf.Bytes()
}

// Must parse, not error: readdir hides whatever expands, so a failure lists it.
func TestEmptyArchiveExpandsToNothing(t *testing.T) {
	cases := map[string][]byte{
		"zip":            buildEmptyZip(t, ""),
		"zip w/ comment": buildEmptyZip(t, "0123456789abcdefghij"),
		"7z":             buildEmptySevenZip(),
	}
	for name, data := range cases {
		if int64(len(data)) < zipMinSize {
			t.Errorf("%s: size %d below probe floor %d, never expanded", name, len(data), zipMinSize)
		}
		ov, err := parseArchiveOverlay(readAtBytes(data), int64(len(data)), zipMaxEntries)
		if err != nil {
			t.Errorf("%s: %v", name, err)
			continue
		}
		if len(ov.entries) != 0 || len(ov.dirs[""].children) != 0 {
			t.Errorf("%s: got %d entries, %d root children, want 0/0",
				name, len(ov.entries), len(ov.dirs[""].children))
		}
	}
}
