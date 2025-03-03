package arpc

import (
	"bufio"
	"encoding/binary"
	"io"
	"math/rand"
	"net"
	"sync"
	"time"

	"github.com/sonroyaalmerol/pbs-plus/internal/syslog"
	"github.com/valyala/bytebufferpool"
	"github.com/xtaci/smux"
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

// readMsgpMsgPooled reads a MessagePack‑encoded message from r using our framing protocol.
// It uses a 4‑byte big‑endian length header followed by that many bytes. For messages up to
// 4096 bytes it attempts to use a pooled buffer (avoiding an extra copy in hot paths).
// The caller is responsible for calling Release() on the returned *PooledMsg if pm.pooled is true.
func readMsgpMsgPooled(r io.Reader) (*PooledMsg, error) {
	var lenBuf [4]byte
	if _, err := io.ReadFull(r, lenBuf[:]); err != nil {
		return nil, err
	}
	msgLen := binary.BigEndian.Uint32(lenBuf[:])

	buf := bytebufferpool.Get()
	if cap(buf.B) < int(msgLen) {
		buf.B = make([]byte, msgLen)
	} else {
		buf.B = buf.B[:msgLen]
	}
	if _, err := io.ReadFull(r, buf.B); err != nil {
		bytebufferpool.Put(buf)
		return nil, err
	}
	return &PooledMsg{Data: buf.B, buffer: buf}, nil
}

// writeMsgpMsg writes msg to w with a 4‑byte length header. We combine the header and msg
// into one write using net.Buffers so that we only incur one syscall when possible.
func writeMsgpMsg(w io.Writer, msg []byte) error {
	var lenBuf [4]byte
	binary.BigEndian.PutUint32(lenBuf[:], uint32(len(msg)))
	if bw, ok := w.(*bufio.Writer); ok {
		_, err := bw.Write(lenBuf[:])
		if err != nil {
			return err
		}
		_, err = bw.Write(msg)
		if err != nil {
			return err
		}
		return bw.Flush()
	}
	nb := net.Buffers{lenBuf[:], msg}
	_, err := nb.WriteTo(w)
	return err
}

// writeErrorResponse sends an error response over the stream.
func writeErrorResponse(stream *smux.Stream, status int, err error) {
	serErr := WrapError(err)
	errBytes, mErr := marshalWithPool(serErr)
	if mErr != nil && syslog.L != nil {
		syslog.L.Errorf("[writeErrorResponse] %s", mErr.Error())
	}

	var respData []byte
	if errBytes != nil {
		respData = errBytes.Data
		defer errBytes.Release()
	}

	// Set the error message so that the client can fall back to it,
	// if msgpack decoding fails
	resp := Response{
		Status:  status,
		Message: serErr.Message,
		Data:    respData,
	}

	respBytes, mErr := marshalWithPool(&resp)
	if mErr != nil && syslog.L != nil {
		syslog.L.Errorf("[writeErrorResponse] %s", mErr.Error())
	}
	var respBytesData []byte
	if respBytes != nil {
		respBytesData = respBytes.Data
		defer respBytes.Release()
	}
	wErr := writeMsgpMsg(stream, respBytesData)
	if wErr != nil && syslog.L != nil {
		syslog.L.Errorf("[writeErrorResponse] %s", wErr.Error())
	}
}
