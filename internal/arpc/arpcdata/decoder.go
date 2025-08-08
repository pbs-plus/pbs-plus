package arpcdata

import (
	"bytes"
	"compress/zlib"
	"encoding/binary"
	"errors"
	"io"
	"math"
	"sync"
	"time"
	"unsafe"
)

var DecoderPool = sync.Pool{
	New: func() interface{} {
		return &Decoder{}
	},
}

// Decoder reads data from a buffer with zero-copy optimizations
type Decoder struct {
	buf       []byte
	pos       int
	stringBuf []byte
}

func Decompress(compressedData []byte) ([]byte, error) {
	zr, err := zlib.NewReader(bytes.NewReader(compressedData))
	if err != nil {
		return nil, err
	}
	defer zr.Close()

	decompressedData, err := io.ReadAll(zr)
	if err != nil {
		return nil, err
	}
	return decompressedData, nil
}

func NewDecoder(buf []byte) (*Decoder, error) {
	if len(buf) < 4 {
		return nil, errors.New("buffer too small to contain total length")
	}
	totalLength := binary.LittleEndian.Uint32(buf[:4])
	if len(buf) != int(totalLength) {
		return nil, errors.New("total length mismatch: data may be corrupted or incomplete")
	}

	decoder := DecoderPool.Get().(*Decoder)
	decoder.buf = buf
	decoder.pos = 4

	// Initialize string buffer if not present
	if cap(decoder.stringBuf) < 256 {
		decoder.stringBuf = make([]byte, 0, 256)
	}

	return decoder, nil
}

func ReleaseDecoder(d *Decoder) {
	// Keep stringBuf allocated for reuse, just reset other fields
	d.buf = nil
	d.pos = 0
	d.stringBuf = d.stringBuf[:0] // Reset length but keep capacity
	DecoderPool.Put(d)
}

func (d *Decoder) Reset(buf []byte) error {
	if len(buf) < 4 {
		return errors.New("buffer too small to contain total length")
	}
	totalLength := binary.LittleEndian.Uint32(buf[:4])
	if len(buf) != int(totalLength) {
		return errors.New("total length mismatch: data may be corrupted or incomplete")
	}
	d.buf = buf
	d.pos = 4
	d.stringBuf = d.stringBuf[:0]
	return nil
}

func (d *Decoder) checkBounds(size int) error {
	if len(d.buf)-d.pos < size {
		return errors.New("buffer too small")
	}
	return nil
}

func (d *Decoder) ReadByte() (byte, error) {
	if err := d.checkBounds(1); err != nil {
		return 0, err
	}
	value := d.buf[d.pos]
	d.pos++
	return value, nil
}

func (d *Decoder) ReadUint8() (uint8, error) {
	if err := d.checkBounds(1); err != nil {
		return 0, err
	}
	value := d.buf[d.pos]
	d.pos++
	return value, nil
}

func (d *Decoder) ReadUint32() (uint32, error) {
	if err := d.checkBounds(4); err != nil {
		return 0, err
	}
	value := binary.LittleEndian.Uint32(d.buf[d.pos:])
	d.pos += 4
	return value, nil
}

func (d *Decoder) ReadInt64() (int64, error) {
	if err := d.checkBounds(8); err != nil {
		return 0, err
	}
	value := int64(binary.LittleEndian.Uint64(d.buf[d.pos:]))
	d.pos += 8
	return value, nil
}

func (d *Decoder) ReadUint64() (uint64, error) {
	if err := d.checkBounds(8); err != nil {
		return 0, err
	}
	value := binary.LittleEndian.Uint64(d.buf[d.pos:])
	d.pos += 8
	return value, nil
}

func (d *Decoder) ReadFloat32() (float32, error) {
	if err := d.checkBounds(4); err != nil {
		return 0, err
	}
	value := math.Float32frombits(binary.LittleEndian.Uint32(d.buf[d.pos:]))
	d.pos += 4
	return value, nil
}

func (d *Decoder) ReadFloat64() (float64, error) {
	if err := d.checkBounds(8); err != nil {
		return 0, err
	}
	value := math.Float64frombits(binary.LittleEndian.Uint64(d.buf[d.pos:]))
	d.pos += 8
	return value, nil
}

// Zero-copy bytes reading - returns slice pointing to original buffer
func (d *Decoder) ReadBytes() ([]byte, error) {
	length, err := d.ReadUint32()
	if err != nil {
		return nil, err
	}
	if err := d.checkBounds(int(length)); err != nil {
		return nil, err
	}

	// Return slice pointing directly to buffer (zero-copy)
	data := d.buf[d.pos : d.pos+int(length)]
	d.pos += int(length)
	return data, nil
}

// Zero-copy string reading using unsafe conversion
func (d *Decoder) ReadString() (string, error) {
	data, err := d.ReadBytes()
	if err != nil {
		return "", err
	}

	// Zero-copy conversion from []byte to string
	return *(*string)(unsafe.Pointer(&data)), nil
}

func (d *Decoder) ReadBool() (bool, error) {
	if err := d.checkBounds(1); err != nil {
		return false, err
	}
	value := d.buf[d.pos] == 1
	d.pos++
	return value, nil
}

func (d *Decoder) ReadTime() (time.Time, error) {
	nano, err := d.ReadInt64()
	if err != nil {
		return time.Time{}, err
	}
	return time.Unix(0, nano), nil
}

// Zero-copy array reading using unsafe slice header manipulation
func (d *Decoder) ReadInt32Array() ([]int32, error) {
	length, err := d.ReadUint32()
	if err != nil {
		return nil, err
	}

	byteSize := int(length) * 4
	if err := d.checkBounds(byteSize); err != nil {
		return nil, err
	}

	data := d.buf[d.pos : d.pos+byteSize]
	d.pos += byteSize

	// Convert []byte to []int32 without copying
	return *(*[]int32)(unsafe.Pointer(&struct {
		data uintptr
		len  int
		cap  int
	}{
		data: uintptr(unsafe.Pointer(&data[0])),
		len:  int(length),
		cap:  int(length),
	})), nil
}

func (d *Decoder) ReadInt64Array() ([]int64, error) {
	length, err := d.ReadUint32()
	if err != nil {
		return nil, err
	}

	byteSize := int(length) * 8
	if err := d.checkBounds(byteSize); err != nil {
		return nil, err
	}

	data := d.buf[d.pos : d.pos+byteSize]
	d.pos += byteSize

	return *(*[]int64)(unsafe.Pointer(&struct {
		data uintptr
		len  int
		cap  int
	}{
		data: uintptr(unsafe.Pointer(&data[0])),
		len:  int(length),
		cap:  int(length),
	})), nil
}

func (d *Decoder) ReadFloat64Array() ([]float64, error) {
	length, err := d.ReadUint32()
	if err != nil {
		return nil, err
	}

	byteSize := int(length) * 8
	if err := d.checkBounds(byteSize); err != nil {
		return nil, err
	}

	data := d.buf[d.pos : d.pos+byteSize]
	d.pos += byteSize

	return *(*[]float64)(unsafe.Pointer(&struct {
		data uintptr
		len  int
		cap  int
	}{
		data: uintptr(unsafe.Pointer(&data[0])),
		len:  int(length),
		cap:  int(length),
	})), nil
}
