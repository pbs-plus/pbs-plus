package arpc

import (
	"fmt"
	"math/rand"
	"net/http"
	"sync"
	"time"

	"github.com/quic-go/quic-go"
)

var (
	randMu     sync.Mutex
	globalRand = rand.New(rand.NewSource(time.Now().UnixNano()))
)

// safeRandFloat64 returns a random float64 in [0.0, 1.0) in a thread-safe way.
func safeRandFloat64() float64 {
	randMu.Lock()
	defer randMu.Unlock()
	return globalRand.Float64()
}

// writeErrorResponse sends an error response over the stream.
func writeErrorResponse(stream *quic.Stream, status int, err error) {
	resp := Response{
		Status:  status,
		Message: err.Error(),
	}
	data, marshalErr := resp.Encode()
	if marshalErr == nil {
		resp.Data = data
	}
	respBytes, marshalErr := cborEncMode.Marshal(&resp)
	if marshalErr != nil {
		respBytes = []byte(fmt.Sprintf(`{"status":%d,"message":"Internal Server Error: %s"}`, http.StatusInternalServerError, err.Error()))
	}
	_, _ = stream.Write(respBytes)
	stream.CancelWrite(0)
	stream.CancelRead(0)
}
