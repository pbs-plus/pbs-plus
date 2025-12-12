package arpc

import (
	"fmt"
	"net/http"

	"github.com/quic-go/quic-go"
)

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
