package arpcdata

import (
	"encoding/binary"
	"math"
	"time"
)

// Encodable defines the interface for types that can be encoded.
type Encodable interface {
	Encode() ([]byte, error)
	Decode([]byte) error
}

// Encoder writes data to a reusable buffer.
// To minimize allocations, you can pass a sufficiently large buffer using
// NewEncoderWithSize, or let the Encoder grow as needed.
type Encoder struct {
	buf []byte
	pos int
}

// NewEncoderWithSize creates an Encoder with a pre-allocated buffer of the
// specified size. The first 4 bytes are reserved for the total length.
func NewEncoderWithSize(initialSize int) *Encoder {
	initialSize += 4
	buf := make([]byte, initialSize)
	return &Encoder{
		buf: buf,
		pos: 4, // Reserve 4 bytes for total length
	}
}

// NewEncoder creates an Encoder with a default buffer size.
func NewEncoder() *Encoder {
	return NewEncoderWithSize(128)
}

// grow checks if there's enough space for size more bytes.
// It doubles the current buffer repeatedly until it can accommodate the new data.
func (e *Encoder) grow(size int) {
	if len(e.buf)-e.pos < size {
		newSize := len(e.buf) * 2
		for newSize < e.pos+size {
			newSize *= 2
		}
		newBuf := make([]byte, newSize)
		copy(newBuf, e.buf[:e.pos])
		e.buf = newBuf
	}
}

// WriteUint8 writes a uint8 to the buffer.
func (e *Encoder) WriteUint8(value uint8) error {
	e.grow(1) // Ensure there is enough space for 1 byte
	e.buf[e.pos] = value
	e.pos++
	return nil
}

// WriteUint8 writes a uint8 to the buffer.
func (e *Encoder) WriteByte(value byte) error {
	e.grow(1) // Ensure there is enough space for 1 byte
	e.buf[e.pos] = value
	e.pos++
	return nil
}

// WriteUint32 writes a uint32 to the buffer.
func (e *Encoder) WriteUint32(value uint32) error {
	e.grow(4)
	binary.LittleEndian.PutUint32(e.buf[e.pos:], value)
	e.pos += 4
	return nil
}

// WriteInt64 writes an int64 to the buffer.
func (e *Encoder) WriteInt64(value int64) error {
	e.grow(8)
	binary.LittleEndian.PutUint64(e.buf[e.pos:], uint64(value))
	e.pos += 8
	return nil
}

// WriteUint64 writes a uint64 to the buffer.
func (e *Encoder) WriteUint64(value uint64) error {
	e.grow(8)
	binary.LittleEndian.PutUint64(e.buf[e.pos:], value)
	e.pos += 8
	return nil
}

// WriteFloat32 writes a float32 to the buffer.
func (e *Encoder) WriteFloat32(value float32) error {
	e.grow(4)
	binary.LittleEndian.PutUint32(e.buf[e.pos:], math.Float32bits(value))
	e.pos += 4
	return nil
}

// WriteFloat64 writes a float64 to the buffer.
func (e *Encoder) WriteFloat64(value float64) error {
	e.grow(8)
	binary.LittleEndian.PutUint64(e.buf[e.pos:], math.Float64bits(value))
	e.pos += 8
	return nil
}

// WriteBytes writes a length-prefixed byte slice to the buffer.
func (e *Encoder) WriteBytes(data []byte) error {
	if err := e.WriteUint32(uint32(len(data))); err != nil {
		return err
	}
	e.grow(len(data))
	copy(e.buf[e.pos:], data)
	e.pos += len(data)
	return nil
}

// WriteString writes a length-prefixed string to the buffer.
// Note: Converting a string to a []byte will allocate.
// If you need to avoid that allocation as well, consider a different API.
func (e *Encoder) WriteString(value string) error {
	return e.WriteBytes([]byte(value))
}

// WriteBool writes a boolean as a single byte (0 or 1).
func (e *Encoder) WriteBool(value bool) error {
	e.grow(1)
	if value {
		e.buf[e.pos] = 1
	} else {
		e.buf[e.pos] = 0
	}
	e.pos++
	return nil
}

// WriteTime writes a time.Time by encoding its UnixNano (int64).
func (e *Encoder) WriteTime(value time.Time) error {
	return e.WriteInt64(value.UnixNano())
}

// WriteInt32Array writes a length-prefixed array of int32 values.
func (e *Encoder) WriteInt32Array(values []int32) error {
	if err := e.WriteUint32(uint32(len(values))); err != nil {
		return err
	}
	for _, v := range values {
		if err := e.WriteUint32(uint32(v)); err != nil {
			return err
		}
	}
	return nil
}

// WriteInt64Array writes a length-prefixed array of int64 values.
func (e *Encoder) WriteInt64Array(values []int64) error {
	if err := e.WriteUint32(uint32(len(values))); err != nil {
		return err
	}
	for _, v := range values {
		if err := e.WriteInt64(v); err != nil {
			return err
		}
	}
	return nil
}

// WriteFloat64Array writes a length-prefixed array of float64 values.
func (e *Encoder) WriteFloat64Array(values []float64) error {
	if err := e.WriteUint32(uint32(len(values))); err != nil {
		return err
	}
	for _, v := range values {
		if err := e.WriteFloat64(v); err != nil {
			return err
		}
	}
	return nil
}

// Bytes finalizes the encoding by writing the total length at the buffer's start,
// then returns the encoded byte slice.
func (e *Encoder) Bytes() []byte {
	totalLength := uint32(e.pos)
	binary.LittleEndian.PutUint32(e.buf[0:], totalLength)
	return e.buf[:e.pos]
}
