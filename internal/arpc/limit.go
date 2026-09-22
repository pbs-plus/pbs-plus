package arpc

import (
	"errors"
	"io"
)

// ErrMessageTooLarge reports that an aRPC control envelope exceeded its byte limit.
var ErrMessageTooLarge = errors.New("arpc message exceeds size limit")

type messageLimitReader struct {
	reader    io.Reader
	remaining int64
}

func newMessageLimitReader(reader io.Reader, limit int64) io.Reader {
	if limit <= 0 {
		return reader
	}
	return &messageLimitReader{reader: reader, remaining: limit}
}

func (r *messageLimitReader) Read(buffer []byte) (int, error) {
	if r.remaining == 0 {
		return 0, ErrMessageTooLarge
	}
	if int64(len(buffer)) > r.remaining {
		buffer = buffer[:r.remaining]
	}
	n, err := r.reader.Read(buffer)
	r.remaining -= int64(n)
	return n, err
}
