package arpc

import (
	"fmt"

	"github.com/goccy/go-json"
	"github.com/valyala/fastjson"
)

//
// --- JSON Encoding Helpers ---
//

// encodeValue converts various Go basic types, maps, slices (or an already
// encoded *fastjson.Value) into a fastjson.Value using the supplied arena.
// This avoids doing a two‑step marshal/parse roundtrip.
// (If your payload type isn’t supported, you’ll have to encode it externally.)
func EncodeValue(v interface{}) *fastjson.Value {
	// If the value is already a *fastjson.Value, return it.
	if fval, ok := v.(*fastjson.Value); ok {
		return fval
	}

	// Handle nil values by returning a JSON null.
	if v == nil {
		var n fastjson.Value
		return &n
	}

	// Marshal the value to JSON bytes.
	b, err := json.Marshal(v)
	if err != nil {
		// If marshaling fails, return a JSON object with an error message.
		errMsg := fmt.Sprintf(`{"error": "failed to marshal value: %v"}`, err)
		b = []byte(errMsg)
	}

	var p fastjson.Parser
	val, err := p.ParseBytes(b)
	if err != nil {
		// If parsing fails, return a JSON object indicating the failure.
		errStr := fmt.Sprintf(`{"error": "failed to parse json: %v"}`, err)
		val, _ = p.Parse(errStr)
	}
	return val
}

// buildRequestJSON builds a JSON‑encoded RPC request using fastjson.
// It writes the method name, the payload (converted via encodeValue),
// any extra headers, and appends a newline as the message delimiter.
func buildRequestJSON(method string, payload interface{}, extraHeaders map[string]string) (
	[]byte, error,
) {
	arena := arenaPool.Get().(*fastjson.Arena)
	defer func() {
		arena.Reset()
		arenaPool.Put(arena)
	}()
	reqObj := arena.NewObject()
	reqObj.Set("method", arena.NewString(method))

	payloadVal := EncodeValue(payload)
	reqObj.Set("payload", payloadVal)

	if extraHeaders != nil && len(extraHeaders) > 0 {
		headersObj := arena.NewObject()
		for key, value := range extraHeaders {
			headersObj.Set(key, arena.NewString(value))
		}
		reqObj.Set("headers", headersObj)
	}

	reqBytes := reqObj.MarshalTo(nil)
	// Append newline as delimiter.
	return append(reqBytes, '\n'), nil
}
