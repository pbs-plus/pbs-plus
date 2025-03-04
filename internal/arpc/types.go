//go:generate msgp

package arpc

import "github.com/xtaci/smux"

type BufferMetadata struct {
	BytesAvailable int  `msg:"bytes_available"`
	EOF            bool `msg:"eof"`
}

// Request defines the MessagePack‑encoded request format.
type Request struct {
	Method  []byte `msg:"method"`
	Payload []byte `msg:"payload,allownil"`
}

// Response defines the MessagePack‑encoded response format.
type Response struct {
	Status    int                `msg:"status"`
	Message   []byte             `msg:"message,omitempty"`
	Data      []byte             `msg:"data,allownil,omitempty"`
	RawStream func(*smux.Stream) `msg:"-"`
}

type SerializableError struct {
	ErrorType     []byte `msg:"error_type"`
	Message       []byte `msg:"message"`
	Op            []byte `msg:"op,omitempty"`
	Path          []byte `msg:"path,omitempty"`
	OriginalError error  `msg:"-"`
}

type StringMsg string
type MapStringIntMsg map[string]int
type MapStringUint64Msg map[string]uint64
type MapStringStringMsg map[string]string
