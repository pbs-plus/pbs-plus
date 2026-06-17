// Decoding of the READ ELEMENT STATUS response (SMC-3). The response is:
//
//	Element Status Header (4 bytes):
//	  first_element_address[2], number_of_elements[2]
//	  — note: unlike the subheader, there is no byte-count here in all firmwares,
//	    so we treat the whole received buffer as the data and walk subpages.
//
//	Repeated Element Status Pages:
//	  SubHeader (8 bytes):
//	    element_type[1], flags[1] (bit7=PVolTag, bit6=AVolTag),
//	    descriptor_length[2], reserved[1], byte_count[3]
//	  followed by descriptors, each `descriptor_length` bytes long.
//
// Each descriptor begins with a common 8-byte header:
//
//	addr[2], flags1[1], asc[1], ascq[1], reserved[3]
//
// followed (for storage/transport) by flags2[1], src_storage_addr[2], and for
// drives by id_valid[1], scsi_bus_addr[1], reserved[1], flags2[1], src_addr[2].
// When PVolTag/AVolTag bits are set in the subheader, a 36-byte volume tag
// (barcode) is appended per tag before any per-element trailing data.
package changer

import (
	"errors"
	"fmt"
)

// decodeElementStatusPage walks the response and returns the decoded elements,
// the highest element address seen, the count received, and any error.
func decodeElementStatusPage(data []byte, start uint16) (elems []rawElement, lastAddr uint16, got uint16, err error) {
	if len(data) < 8 {
		return nil, 0, 0, errors.New("element-status response too short")
	}
	// Element Status Header (8 bytes): first_addr[2], num_elements[2],
	// reserved[1], byte_count[3]. The byte_count covers all following pages.
	firstReported := be16(data[0:2])
	numAvailable := be16(data[2:4])
	_ = firstReported
	_ = numAvailable

	body := data[8:]
	for len(body) >= 8 {
		typeCode := body[0]
		flags := body[1]
		descLen := be16(body[2:4])
		// body[4] reserved
		byteCount := be24(body[5:8])
		if int(byteCount) > len(body)-8 {
			byteCount = uint32(len(body) - 8)
		}
		descr := body[8 : 8+int(byteCount)]
		body = body[8+int(byteCount):]

		pVolTag := flags&0x80 != 0
		aVolTag := flags&0x40 != 0

		if descLen == 0 {
			return nil, 0, 0, errors.New("element descriptor length is zero")
		}
		if int(descLen) > len(descr) && len(descr) > 0 {
			descLen = uint16(len(descr))
		}
		for off := 0; off+int(descLen) <= len(descr); off += int(descLen) {
			d := descr[off : off+int(descLen)]
			e, derr := decodeDescriptor(d, typeCode, pVolTag, aVolTag)
			if derr != nil {
				err = derr
				return
			}
			elems = append(elems, e)
			if e.Addr > lastAddr {
				lastAddr = e.Addr
			}
			got++
		}
	}
	if got == 0 {
		return elems, 0, 0, nil
	}
	return elems, lastAddr, got, nil
}

// decodeDescriptor parses one element descriptor of the given type.
func decodeDescriptor(d []byte, typeCode byte, pVolTag, aVolTag bool) (rawElement, error) {
	if len(d) < 8 {
		return rawElement{}, fmt.Errorf("descriptor too short (%d bytes)", len(d))
	}
	e := rawElement{
		Addr: be16(d[0:2]),
		Full: d[2]&0x01 != 0,
		ASC:  d[4], // additional sense code (PBS field order: flags1,reserved,asc,ascq)
		ASCQ: d[5],
	}
	// Common descriptor header (9 bytes): addr[2], flags1, reserved, asc, ascq,
	// reserved[3]. The type-specific fields follow at offset 9.
	off := 9
	switch typeCode {
	case 1, 2, 3:
		// storage/transport/import-export: flags2[1], src_storage_addr[2]
		if off+3 <= len(d) {
			e.SrcAddr = be16(d[off+1 : off+3])
		}
		off += 3
	case 4:
		// drive (TransferDescriptor): the SValid bit lives in flags2 and the
		// source storage address follows it — at the SAME offsets as storage
		// elements (the id_valid/scsi_bus/reserved fields occupy bytes 6-8,
		// which precede flags2 at byte 9). SValid = flags2 & 0x80.
		if off+3 <= len(d) {
			if d[off]&0x80 != 0 {
				e.SrcAddr = be16(d[off+1 : off+3])
			}
		}
		off += 3
	default:
		return rawElement{}, fmt.Errorf("unknown element type code %d", typeCode)
	}
	// Volume tags (primary then alternate), 36 bytes each when requested.
	if pVolTag && off+scsiVolumeTagLen <= len(d) {
		e.VolTag = scsiASCII(d[off : off+scsiVolumeTagLen])
	}
	// An alternate volume tag (if requested) follows the primary tag, but
	// tape changers do not use it; nothing further is parsed.
	return e, nil
}
