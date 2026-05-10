package arpc

import (
	"encoding/binary"
	"fmt"
	"io"
	"time"

	"github.com/fxamacker/cbor/v2"
	"github.com/pbs-plus/pbs-plus/internal/conf"
	"github.com/xtaci/smux"
)

// ARPCStream abstracts a bidirectional byte stream. Backed by smux.Stream
// (TCP data plane) or quic.Stream (QUIC control plane).
type ARPCStream interface {
	io.Reader
	io.Writer
	io.Closer
	SetDeadline(time.Time) error
}

func writeErrorResponse(stream io.Writer, status int, err error) {
	serErr := WrapError(err)

	errBytes, encodeErr := cbor.Marshal(serErr)
	if encodeErr != nil {
		stream.Write(fmt.Appendf(nil, "failed to encode error: %v", encodeErr))
		return
	}

	resp := Response{
		Status:  status,
		Message: err.Error(),
		Data:    errBytes,
	}

	respBytes, encodeErr := cbor.Marshal(resp)
	if encodeErr != nil {
		stream.Write(fmt.Appendf(nil, "failed to encode response: %v", encodeErr))
		return
	}

	stream.Write(respBytes)
}

func defaultConfig() *smux.Config {
	defaults := smux.DefaultConfig()
	defaults.Version = 2
	defaults.MaxReceiveBuffer = conf.MaxReceiveBuffer
	defaults.MaxStreamBuffer = conf.MaxStreamBuffer
	defaults.MaxFrameSize = 65535

	return defaults
}

func writeVarint(w io.Writer, v uint64) error {
	var buf [binary.MaxVarintLen64]byte
	n := binary.PutUvarint(buf[:], v)
	_, err := w.Write(buf[:n])

	return err

}

func readVarint(r io.Reader) (uint64, error) {
	return binary.ReadUvarint(&byteReader{r: r})
}

type byteReader struct {
	r io.Reader
}

func (b *byteReader) ReadByte() (byte, error) {
	var buf [1]byte
	_, err := b.r.Read(buf[:])
	return buf[0], err
}
