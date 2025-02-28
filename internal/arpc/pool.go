package arpc

import (
	"sync"

	"github.com/tinylib/msgp/msgp"
)

// We use a sync.Pool to return buffers for small MessagePack messages.
var msgpackBufferPool = sync.Pool{
	New: func() interface{} {
		// Start with a reasonable size for most requests.
		return make([]byte, 4096)
	},
}

// PooledMsg wraps a []byte that may come from the pool. If
// Pooled is true then the caller must call Release() once done.
type PooledMsg struct {
	Data   []byte
	pooled bool
}

// Release returns the underlying buffer to the pool if it was pooled.
func (pm *PooledMsg) Release() {
	if pm.pooled {
		// Reset length to full capacity
		msgpackBufferPool.Put(pm.Data[:cap(pm.Data)])
		pm.pooled = false
	}
}

// Optimized serialization using msgp codegen
func marshalWithPool(v msgp.Marshaler) (*PooledMsg, error) {
	// Get a buffer from the pool.
	buf := msgpackBufferPool.Get().([]byte)
	// MarshalMsg appends to the provided slice.
	b, err := v.MarshalMsg(buf[:0])
	if err != nil {
		// Return the buffer to the pool on error.
		msgpackBufferPool.Put(buf)
		return nil, err
	}
	return &PooledMsg{
		Data:   b,
		pooled: true,
	}, nil
}
