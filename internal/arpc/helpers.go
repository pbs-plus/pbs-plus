package arpc

import (
	"net/http"

	"github.com/goccy/go-json"
)

// --- JSON Encoding Helpers ---
//

// buildRequestJSON builds a JSON‑encoded RPC request. It sets the method name,
// marshals the payload (using go‑json) and any extra headers, and appends a newline
// as the message delimiter.
func buildRequestJSON(method string, payload interface{}, extraHeaders map[string]string) (
	[]byte, error,
) {
	var rawPayload json.RawMessage
	if p, ok := payload.(json.RawMessage); ok {
		rawPayload = p
	} else {
		b, err := json.Marshal(payload)
		if err != nil {
			return nil, err
		}
		rawPayload = b
	}

	req := Request{
		Method:  method,
		Payload: rawPayload,
	}
	if extraHeaders != nil && len(extraHeaders) > 0 {
		headers := http.Header{}
		for key, value := range extraHeaders {
			headers.Set(key, value)
		}
		req.Headers = headers
	}

	b, err := json.Marshal(req)
	if err != nil {
		return nil, err
	}
	// Append newline as delimiter.
	return append(b, '\n'), nil
}
