package arpc

import (
	"fmt"
	"io"
	"net/http"
)

func writeErrorResponse(stream io.Writer, status int, err error) {
	serErr := WrapError(err)

	errBytes, encodeErr := serErr.Encode()
	if encodeErr != nil {
		stream.Write([]byte(fmt.Sprintf("failed to encode error: %v", encodeErr)))
		return
	}

	resp := Response{
		Status:  status,
		Message: err.Error(),
		Data:    errBytes,
	}

	respBytes, encodeErr := resp.Encode()
	if encodeErr != nil {
		stream.Write([]byte(fmt.Sprintf("failed to encode response: %v", encodeErr)))
		return
	}

	stream.Write(respBytes)
}

func headerCloneMap(h http.Header) map[string][]string {
	if h == nil {
		return nil
	}
	out := make(map[string][]string, len(h))
	for k, v := range h {
		vv := make([]string, len(v))
		copy(vv, v)
		out[k] = vv
	}
	return out
}
