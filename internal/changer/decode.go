//   - note: unlike the subheader, there is no byte-count here in all firmwares,
//
// (barcode) is appended per tag before any per-element trailing data.
package changer

import (
	"errors"
	"fmt"
)

func decodeElementStatusPage(data []byte, start uint16) (elems []rawElement, lastAddr uint16, got uint16, err error) {
	if len(data) < 8 {
		return nil, 0, 0, errors.New("element-status response too short")
	}
	firstReported := be16(data[0:2])
	numAvailable := be16(data[2:4])
	_ = firstReported
	_ = numAvailable

	body := data[8:]
	for len(body) >= 8 {
		typeCode := body[0]
		flags := body[1]
		descLen := be16(body[2:4])
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
	off := 9
	switch typeCode {
	case 1, 2, 3:
		if off+3 <= len(d) {
			e.SrcAddr = be16(d[off+1 : off+3])
		}
		off += 3
	case 4:
		if off+3 <= len(d) {
			if d[off]&0x80 != 0 {
				e.SrcAddr = be16(d[off+1 : off+3])
			}
		}
		off += 3
	default:
		return rawElement{}, fmt.Errorf("unknown element type code %d", typeCode)
	}
	if pVolTag && off+scsiVolumeTagLen <= len(d) {
		e.VolTag = scsiASCII(d[off : off+scsiVolumeTagLen])
	}
	// tape changers do not use it; nothing further is parsed.
	return e, nil
}
