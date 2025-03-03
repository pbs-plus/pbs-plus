package arpc

import (
	"github.com/tinylib/msgp/msgp"
	"github.com/valyala/bytebufferpool"
)

// Optimized serialization using msgp codegen
func marshalWithPool(v msgp.Marshaler) (*bytebufferpool.ByteBuffer, error) {
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
	return buf, nil
}
