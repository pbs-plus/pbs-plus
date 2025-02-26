package arpc

import (
	json "github.com/goccy/go-json"
	"github.com/valyala/fastjson"
)

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

	// Marshal payload using standard JSON.
	payloadBytes, err := json.Marshal(payload)
	if err != nil {
		return nil, err
	}

	var parser fastjson.Parser
	payloadVal, err := parser.ParseBytes(payloadBytes)
	if err != nil {
		return nil, err
	}
	reqObj.Set("payload", payloadVal)

	// Set any extra headers.
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
