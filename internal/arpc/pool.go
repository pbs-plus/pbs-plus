package arpc

import (
	"github.com/tinylib/msgp/msgp"
	"github.com/valyala/bytebufferpool"
)

// PooledMsg wraps a []byte that may come from the pool. If
// Pooled is true then the caller must call Release() once done.
type PooledMsg struct {
	Data   []byte
	buffer *bytebufferpool.ByteBuffer
}

// Release returns the underlying buffer to the pool if it was pooled.
func (pm *PooledMsg) Release() {
	if pm.buffer != nil {
		// Return the buffer to the pool.
		bytebufferpool.Put(pm.buffer)
		pm.buffer = nil
	}
}

// Optimized serialization using msgp codegen
func marshalWithPool(v msgp.Marshaler) (*PooledMsg, error) {
	// Get a buffer from the pool.
	buf := bytebufferpool.Get()
	// MarshalMsg appends to the provided slice.
	b, err := v.MarshalMsg(buf.B[:0])
	if err != nil {
		// Return the buffer to the pool on error.
		bytebufferpool.Put(buf)
		return nil, err
	}
	// Update the buffer with the marshaled data.
	buf.B = b
	return &PooledMsg{
		Data:   b,
		buffer: buf,
	}, nil
}
