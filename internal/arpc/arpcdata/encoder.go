package arpcdata

import (
	"encoding/binary"
	"math"
	"sync"
	"time"
)

// Encodable defines the interface for types that can be encoded.
type Encodable interface {
	Encode() ([]byte, error)
}

// Buffer pools for reusable buffers of different sizes.
var smallBufferPool = sync.Pool{
	New: func() interface{} {
		return make([]byte, 1024) // 1 KB buffer
	},
}

var mediumBufferPool = sync.Pool{
	New: func() interface{} {
		return make([]byte, 4096) // 4 KB buffer
	},
}

var largeBufferPool = sync.Pool{
	New: func() interface{} {
		return make([]byte, 16384) // 16 KB buffer
	},
}

// getBuffer retrieves a buffer of the appropriate size from the pool.
func getBuffer(size int) []byte {
	switch {
	case size <= 1024:
		return smallBufferPool.Get().([]byte)
	case size <= 4096:
		return mediumBufferPool.Get().([]byte)
	default:
		return largeBufferPool.Get().([]byte)
	}
}

// putBuffer returns a buffer to the appropriate pool based on its capacity.
func putBuffer(buf []byte) {
	switch cap(buf) {
	case 1024:
		smallBufferPool.Put(buf[:0])
	case 4096:
		mediumBufferPool.Put(buf[:0])
	case 16384:
		largeBufferPool.Put(buf[:0])
	}
}

// Encoder writes data to a reusable buffer.
type Encoder struct {
	buf []byte
	pos int
}

// NewEncoder creates a new Encoder instance with a reusable buffer.
func NewEncoder() *Encoder {
	return &Encoder{
		buf: getBuffer(1024), // Start with a small buffer
		pos: 4,               // Reserve 4 bytes for the total length
	}
}

// Release returns the buffer to the pool for reuse.
func (e *Encoder) Release() {
	putBuffer(e.buf) // Return the buffer to the appropriate pool
	e.buf = nil      // Clear the reference
}

// grow ensures the buffer has enough capacity to write additional data.
// It dynamically resizes the buffer using the appropriate pool.
func (e *Encoder) grow(size int) {
	if len(e.buf)-e.pos < size {
		newSize := len(e.buf) * 2
		if newSize < e.pos+size {
			newSize = e.pos + size
		}

		// Get a new buffer from the pool
		newBuf := getBuffer(newSize)
		copy(newBuf, e.buf[:e.pos]) // Copy existing data to the new buffer
		putBuffer(e.buf)            // Return the old buffer to the pool
		e.buf = newBuf
	}
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

// WriteTime writes a time.Time as UnixNano (int64).
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

// Bytes returns the encoded data and writes the total length at the beginning.
func (e *Encoder) Bytes() []byte {
	// Write the total length at the beginning of the buffer
	totalLength := uint32(e.pos)
	binary.LittleEndian.PutUint32(e.buf[0:], totalLength)
	return e.buf[:e.pos]
}
