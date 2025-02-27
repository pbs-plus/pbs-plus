package arpc

import (
	"net/http"

	"github.com/vmihailenco/msgpack/v5"
)

// buildRequestMsgpack builds a MessagePack‑encoded RPC request.
// It sets the method name, marshals the payload (using msgpack)
// and any extra headers provided.
func buildRequestMsgpack(method string, payload interface{},
	extraHeaders map[string]string) ([]byte, error) {

	// Marshal the payload first so that it is stored as raw bytes.
	p, err := msgpack.Marshal(payload)
	if err != nil {
		return nil, err
	}

	req := Request{
		Method:  method,
		Payload: p,
	}
	if extraHeaders != nil && len(extraHeaders) > 0 {
		headers := http.Header{}
		for key, value := range extraHeaders {
			headers.Set(key, value)
		}
		req.Headers = headers
	}

	return msgpack.Marshal(req)
}
