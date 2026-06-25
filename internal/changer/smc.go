// SMC-3 command set and response decoding. Mirrors the CDB construction and
package changer

import (
	"fmt"
)

const (
	cmdModeSense6              = 0x1A
	cmdReadElementStatus       = 0xB8
	cmdMoveMedium              = 0xA5
	cmdInitializeElementStatus = 0x07

	pageElementAddressAssignment = 0x1D

	scsiVolumeTagLen = 36

	allocLenStandard = 0xFFFF

	timeoutDefault   = uint32(5 * 60 * 1000)
	timeoutInventory = uint32(30 * 60 * 1000)
	timeoutMove      = uint32(45 * 60 * 1000)
)

type elementType uint8

const (
	elemTransport elementType = 1
	elemStorage   elementType = 2
	elemImportExp elementType = 3
	elemDrive     elementType = 4
)

func (t elementType) byte1(withVolTag bool) byte {
	b := byte(t)
	if withVolTag {
		b |= 0x10
	}
	return b
}

// addressAssignment mirrors MODE SENSE page 0x1D: the first element address
type addressAssignment struct {
	FirstTransport uint16
	NumTransport   uint16
	FirstStorage   uint16
	NumStorage     uint16
	FirstImportExp uint16
	NumImportExp   uint16
	FirstTransfer  uint16
	NumTransfer    uint16
}

func readAddressAssignment(d *device) (*addressAssignment, error) {
	cdb := []byte{cmdModeSense6, 0x08, pageElementAddressAssignment, 0x00, 0xFF, 0x00}
	data, err := d.scsi(cdb, make([]byte, 255), true, timeoutDefault)
	if err != nil {
		return nil, fmt.Errorf("mode sense element-address page: %w", err)
	}
	if len(data) < 6+2+16 {
		return nil, fmt.Errorf("mode sense 0x1D: short response (%d bytes)", len(data))
	}
	for i := 0; i+18 < len(data); i++ {
		if data[i]&0x3f == pageElementAddressAssignment {
			b := data[i+2:]
			a := &addressAssignment{
				FirstTransport: be16(b[0:2]),
				NumTransport:   be16(b[2:4]),
				FirstStorage:   be16(b[4:6]),
				NumStorage:     be16(b[6:8]),
				FirstImportExp: be16(b[8:10]),
				NumImportExp:   be16(b[10:12]),
				FirstTransfer:  be16(b[12:14]),
				NumTransfer:    be16(b[14:16]),
			}
			return a, nil
		}
	}
	return nil, ErrElementAddressNotFound
}

func readElementStatusCDB(start, count uint16, t elementType, withVolTag bool, allocLen uint32) []byte {
	al := allocLen
	cdb := make([]byte, 12)
	cdb[0] = cmdReadElementStatus
	cdb[1] = t.byte1(withVolTag)
	cdb[2] = byte(start >> 8)
	cdb[3] = byte(start)
	cdb[4] = byte(count >> 8)
	cdb[5] = byte(count)
	cdb[6] = 0
	cdb[7] = byte(al >> 16)
	cdb[8] = byte(al >> 8)
	cdb[9] = byte(al)
	cdb[10] = 0
	cdb[11] = 0
	return cdb
}

func moveMediumCDB(transport, source, dest uint16) []byte {
	return []byte{
		cmdMoveMedium, 0,
		byte(transport >> 8), byte(transport),
		byte(source >> 8), byte(source),
		byte(dest >> 8), byte(dest),
		0,
		0,
	}
}

func initializeElementStatus(d *device) error {
	cdb := []byte{cmdInitializeElementStatus, 0, 0, 0, 0, 0}
	_, err := d.scsi(cdb, nil, false, timeoutInventory)
	return err
}
