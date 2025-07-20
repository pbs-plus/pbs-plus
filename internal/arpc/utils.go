package arpc

import (
	"fmt"
	"math/rand"
	"sync"
	"time"

	"github.com/hashicorp/yamux"
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

// getJitteredBackoff returns a backoff duration with a random jitter applied.
func getJitteredBackoff(d time.Duration, jitterFactor float64) time.Duration {
	// Compute jitter in the range [-jitterFactor*d, +jitterFactor*d]
	jitter := time.Duration(float64(d) * jitterFactor * (safeRandFloat64()*2 - 1))
	return d + jitter
}

// writeErrorResponse sends an error response over the stream.
func writeErrorResponse(stream *yamux.Stream, status int, err error) {
	// Wrap the error in a SerializableError
	serErr := WrapError(err)

	// Encode the SerializableError
	errBytes, encodeErr := serErr.Encode()
	if encodeErr != nil {
		// If encoding the error fails, write a generic error response
		stream.Write([]byte(fmt.Sprintf("failed to encode error: %v", encodeErr)))
		return
	}

	// Build the error response
	resp := Response{
		Status:  status,
		Message: err.Error(),
		Data:    errBytes,
	}

	// Encode and write the error response
	respBytes, encodeErr := resp.Encode()
	if encodeErr != nil {
		// If encoding the response fails, write a generic error response
		stream.Write([]byte(fmt.Sprintf("failed to encode response: %v", encodeErr)))
		return
	}

	stream.Write(respBytes)
}
