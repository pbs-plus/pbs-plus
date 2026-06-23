// and autoloaders  -  directly via the Linux SG_IO ioctl, with no external
// The READ ELEMENT STATUS response framing follows the layout used by
package changer

import (
	"encoding/binary"
	"fmt"
	"os"
	"syscall"
	"unsafe"
)

// sgIO is the Linux SCSI Generic ioctl request header (sg_io_hdr_t).
type sgIO struct {
	InterfaceID  int32 // must be 'S'
	DxferDir     int32
	CmdLen       uint8
	MxSBLen      uint8
	IovecCount   uint16
	DxferLen     uint32
	Dxferp       *byte
	Cmdp         *byte
	Sbp          *byte
	Timeout      uint32
	Flags        uint32
	PackID       int32
	UsrPtr       *byte
	Status       uint8
	MaskedStatus uint8
	MsgStatus    uint8
	SbLenWr      uint8
	HostStatus   uint16
	DriverStatus uint16
	Resid        int32 // residual data length
	Duration     uint32
	Info         uint32
}

const (
	ioctlSGIO    = 0x2285
	ifaceMagic   = 'S'
	dxferNone    = -1
	dxferToDev   = -2
	dxferFromDev = -3
)

// SenseError is returned when a SCSI command completes with CHECK CONDITION.
type SenseError struct {
	Sense []byte
	Key   uint8
	ASC   uint8
	ASCQ  uint8
}

func (e *SenseError) Error() string {
	return fmt.Sprintf("scsi check condition: key=0x%x asc=0x%02x ascq=0x%02x (sense=% x)", e.Key, e.ASC, e.ASCQ, e.Sense)
}

const (
	SenseNoSense        = 0x00
	SenseNotReady       = 0x02
	SenseMediumError    = 0x03
	SenseIllegalRequest = 0x05
	SenseUnitAttention  = 0x06
	SenseDataProtect    = 0x07
	SenseBlankCheck     = 0x08
	SenseAbortedCommand = 0x0b
	SenseVolumeOverflow = 0x0d
)

type device struct {
	f *os.File
}

func openDevice(path string) (*device, error) {
	f, err := os.OpenFile(path, os.O_RDWR, 0)
	if err != nil {
		return nil, err
	}
	return &device{f: f}, nil
}

func (d *device) close() error { return d.f.Close() }
func (d *device) fd() int      { return int(d.f.Fd()) }

// nil (for commands with no data phase). A CHECK CONDITION is returned as a
// the caller; resid is subtracted so only valid bytes are returned.
func (d *device) scsi(cdb []byte, data []byte, fromDevice bool, timeoutMs uint32) ([]byte, error) {
	var sense [64]byte
	var dir int32 = dxferNone
	if fromDevice {
		dir = dxferFromDev
	} else if len(data) > 0 {
		dir = dxferToDev
	}
	req := sgIO{
		InterfaceID: ifaceMagic,
		DxferDir:    dir,
		CmdLen:      uint8(len(cdb)),
		MxSBLen:     uint8(len(sense)),
		DxferLen:    uint32(len(data)),
		Dxferp:      nil,
		Cmdp:        &cdb[0],
		Sbp:         &sense[0],
		Timeout:     timeoutMs,
	}
	if len(data) > 0 {
		req.Dxferp = &data[0]
	}
	_, _, errno := syscall.Syscall(syscall.SYS_IOCTL, uintptr(d.fd()), uintptr(ioctlSGIO), uintptr(unsafe.Pointer(&req)))
	if errno != 0 {
		return nil, fmt.Errorf("sg_io ioctl: %w (status=0x%02x host=%d driver=%d)",
			errno, req.Status, req.HostStatus, req.DriverStatus)
	}
	if req.Status == 0x02 || req.SbLenWr > 0 {
		se := &SenseError{Sense: append([]byte(nil), sense[:req.SbLenWr]...)}
		if len(se.Sense) >= 14 {
			se.Key = se.Sense[2] & 0x0f
			se.ASC = se.Sense[12]
			se.ASCQ = se.Sense[13]
		}
		return nil, se
	}
	used := min(max(len(data)-int(req.Resid), 0), len(data))
	return data[:used], nil
}

func be16(b []byte) uint16 { return binary.BigEndian.Uint16(b) }
func be24(b []byte) uint32 { return uint32(b[0])<<16 | uint32(b[1])<<8 | uint32(b[2]) }

func scsiASCII(b []byte) string {
	for i, c := range b {
		if c == 0 {
			b = b[:i]
			break
		}
	}
	for len(b) > 0 && (b[len(b)-1] == ' ' || b[len(b)-1] == 0) {
		b = b[:len(b)-1]
	}
	return string(b)
}
