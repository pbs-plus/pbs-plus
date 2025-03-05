package arpcdata

import (
	"encoding/binary"
	"errors"
	"math"
	"time"
)

// Decoder reads data from a buffer
type Decoder struct {
	buf []byte
	pos int
}

// NewDecoder initializes a new Decoder with the given buffer
func NewDecoder(buf []byte) (*Decoder, error) {
	if len(buf) < 4 {
		return nil, errors.New("buffer too small to contain total length")
	}
	totalLength := binary.LittleEndian.Uint32(buf[:4])
	if len(buf) != int(totalLength) {
		return nil, errors.New("total length mismatch: data may be corrupted or incomplete")
	}
	return &Decoder{buf: buf, pos: 4}, nil // Skip the total length field
}

// Reset allows reusing the Decoder with a new buffer
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
	return nil
}

func (d *Decoder) ReadUint32() (uint32, error) {
	if len(d.buf)-d.pos < 4 {
		return 0, errors.New("buffer too small to read uint32")
	}
	value := binary.LittleEndian.Uint32(d.buf[d.pos:])
	d.pos += 4
	return value, nil
}

func (d *Decoder) ReadInt64() (int64, error) {
	if len(d.buf)-d.pos < 8 {
		return 0, errors.New("buffer too small to read int64")
	}
	value := int64(binary.LittleEndian.Uint64(d.buf[d.pos:]))
	d.pos += 8
	return value, nil
}

func (d *Decoder) ReadUint64() (uint64, error) {
	if len(d.buf)-d.pos < 8 {
		return 0, errors.New("buffer too small to read uint64")
	}
	value := binary.LittleEndian.Uint64(d.buf[d.pos:])
	d.pos += 8
	return value, nil
}

func (d *Decoder) ReadFloat32() (float32, error) {
	if len(d.buf)-d.pos < 4 {
		return 0, errors.New("buffer too small to read float32")
	}
	value := math.Float32frombits(binary.LittleEndian.Uint32(d.buf[d.pos:]))
	d.pos += 4
	return value, nil
}

func (d *Decoder) ReadFloat64() (float64, error) {
	if len(d.buf)-d.pos < 8 {
		return 0, errors.New("buffer too small to read float64")
	}
	value := math.Float64frombits(binary.LittleEndian.Uint64(d.buf[d.pos:]))
	d.pos += 8
	return value, nil
}

func (d *Decoder) ReadBytes() ([]byte, error) {
	length, err := d.ReadUint32()
	if err != nil {
		return nil, err
	}
	if len(d.buf)-d.pos < int(length) {
		return nil, errors.New("buffer too small to read bytes")
	}
	data := d.buf[d.pos : d.pos+int(length)]
	d.pos += int(length)
	return data, nil
}

func (d *Decoder) ReadString() (string, error) {
	data, err := d.ReadBytes()
	if err != nil {
		return "", err
	}
	return string(data), nil
}

func (d *Decoder) ReadBool() (bool, error) {
	if len(d.buf)-d.pos < 1 {
		return false, errors.New("buffer too small to read bool")
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

func (d *Decoder) ReadInt32Array() ([]int32, error) {
	length, err := d.ReadUint32()
	if err != nil {
		return nil, err
	}
	if len(d.buf)-d.pos < int(length)*4 {
		return nil, errors.New("buffer too small to read int32 array")
	}
	values := make([]int32, length)
	for i := 0; i < int(length); i++ {
		values[i] = int32(binary.LittleEndian.Uint32(d.buf[d.pos:]))
		d.pos += 4
	}
	return values, nil
}

func (d *Decoder) ReadInt64Array() ([]int64, error) {
	length, err := d.ReadUint32()
	if err != nil {
		return nil, err
	}
	if len(d.buf)-d.pos < int(length)*8 {
		return nil, errors.New("buffer too small to read int64 array")
	}
	values := make([]int64, length)
	for i := 0; i < int(length); i++ {
		values[i] = int64(binary.LittleEndian.Uint64(d.buf[d.pos:]))
		d.pos += 8
	}
	return values, nil
}

func (d *Decoder) ReadFloat64Array() ([]float64, error) {
	length, err := d.ReadUint32()
	if err != nil {
		return nil, err
	}
	if len(d.buf)-d.pos < int(length)*8 {
		return nil, errors.New("buffer too small to read float64 array")
	}
	values := make([]float64, length)
	for i := 0; i < int(length); i++ {
		values[i] = math.Float64frombits(binary.LittleEndian.Uint64(d.buf[d.pos:]))
		d.pos += 8
	}
	return values, nil
}
