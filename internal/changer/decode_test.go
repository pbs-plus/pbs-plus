package changer

import (
	"testing"
)

// buildPage builds an Element Status response: 8-byte header + one 8-byte
// subheader + descriptor bytes. The subheader advertises pVolTag (0x80).
func buildPage(typeCode byte, descr []byte) []byte {
	page := make([]byte, 8) // Element Status Header (first addr + count, ignored by decoder)
	// SubHeader (8 bytes): type, flags(pVolTag), descLen(2), reserved, byteCount(3)
	byteCount := uint32(len(descr))
	sub := []byte{
		typeCode, 0x80,
		byte(len(descr) >> 8), byte(len(descr)),
		0,
		byte(byteCount >> 16), byte(byteCount >> 8), byte(byteCount),
	}
	page = append(page, sub...)
	page = append(page, descr...)
	return page
}

// buildStorageDesc builds a 12-byte storage descriptor + 36-byte barcode.
// addr, full, asc, src(optional). Volume tag padded with spaces.
func buildStorageDesc(addr uint16, full bool, asc byte, src uint16, voltag string) []byte {
	d := make([]byte, 12)
	d[0] = byte(addr >> 8)
	d[1] = byte(addr)
	flags1 := byte(0)
	if full {
		flags1 |= 0x01
	}
	d[2] = flags1
	d[3] = 0 // reserved
	d[4] = asc
	// d[5] ascq, d[6-8] reserved
	// d[9] = flags2 (SValid bit not used for storage Full state)
	d[10] = byte(src >> 8)
	d[11] = byte(src)
	tag := make([]byte, scsiVolumeTagLen)
	for i := 0; i < len(voltag) && i < scsiVolumeTagLen; i++ {
		tag[i] = voltag[i]
	}
	for i := len(voltag); i < scsiVolumeTagLen; i++ {
		tag[i] = ' '
	}
	return append(d, tag...)
}

// buildDriveDesc builds a 12-byte TransferDescriptor + 36-byte barcode.
// SValid (flags2 bit7) gates the source storage address.
func buildDriveDesc(addr uint16, full bool, asc byte, svalid bool, src uint16, voltag string) []byte {
	d := make([]byte, 12)
	d[0] = byte(addr >> 8)
	d[1] = byte(addr)
	flags1 := byte(0)
	if full {
		flags1 |= 0x01
	}
	d[2] = flags1
	d[4] = asc
	flags2 := byte(0)
	if svalid {
		flags2 |= 0x80
	}
	d[9] = flags2
	d[10] = byte(src >> 8)
	d[11] = byte(src)
	tag := make([]byte, scsiVolumeTagLen)
	for i := 0; i < len(voltag) && i < scsiVolumeTagLen; i++ {
		tag[i] = voltag[i]
	}
	for i := len(voltag); i < scsiVolumeTagLen; i++ {
		tag[i] = ' '
	}
	return append(d, tag...)
}

func TestDecodeStorageDescriptor(t *testing.T) {
	descr := buildStorageDesc(1006, true, 0, 0, "UN6265L8")
	page := buildPage(2, descr)
	elems, _, got, err := decodeElementStatusPage(page, 1006)
	if err != nil {
		t.Fatal(err)
	}
	if got != 1 || len(elems) != 1 {
		t.Fatalf("got=%d len=%d", got, len(elems))
	}
	e := elems[0]
	if e.Addr != 1006 {
		t.Errorf("Addr = %d, want 1006", e.Addr)
	}
	if !e.Full {
		t.Errorf("Full = false, want true")
	}
	if e.VolTag != "UN6265L8" {
		t.Errorf("VolTag = %q, want UN6265L8", e.VolTag)
	}
}

func TestDecodeDriveDescriptorSourceAndBarcode(t *testing.T) {
	// Drive addr=1, full, SValid set, source slot addr=1006, barcode UN6265L8.
	descr := buildDriveDesc(1, true, 0, true, 1006, "UN6265L8")
	page := buildPage(4, descr)
	elems, _, got, err := decodeElementStatusPage(page, 1)
	if err != nil {
		t.Fatal(err)
	}
	if got != 1 || len(elems) != 1 {
		t.Fatalf("got=%d len=%d", got, len(elems))
	}
	e := elems[0]
	if e.Addr != 1 {
		t.Errorf("Addr = %d, want 1", e.Addr)
	}
	if !e.Full {
		t.Errorf("Full = false, want true")
	}
	// Regression: source storage address must be read from bytes 10-11
	// (flags2 SValid at byte 9), not offset 13-15 as the old code did.
	if e.SrcAddr != 1006 {
		t.Errorf("SrcAddr = %d, want 1006", e.SrcAddr)
	}
	// Regression: barcode must start at byte 12, giving the full 8-char label,
	// not the truncated "6265L8" the old offset-14 read produced.
	if e.VolTag != "UN6265L8" {
		t.Errorf("VolTag = %q, want UN6265L8 (full barcode, not truncated)", e.VolTag)
	}
}

func TestDecodeDriveDescriptorSValidClear(t *testing.T) {
	// SValid clear: SrcAddr must be zeroed even if bytes 10-11 hold junk.
	descr := buildDriveDesc(1, true, 0, false, 9999, "UN6265L8")
	page := buildPage(4, descr)
	elems, _, _, err := decodeElementStatusPage(page, 1)
	if err != nil {
		t.Fatal(err)
	}
	if elems[0].SrcAddr != 0 {
		t.Errorf("SrcAddr = %d, want 0 when SValid clear", elems[0].SrcAddr)
	}
}
