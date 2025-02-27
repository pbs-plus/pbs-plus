package arpc

import (
	"bytes"
	"fmt"

	"github.com/valyala/fastjson"
)

//
// --- JSON Encoding Helpers ---
//

// encodeValue converts various Go basic types, maps, slices (or an already
// encoded *fastjson.Value) into a fastjson.Value using the supplied arena.
// This avoids doing a two‑step marshal/parse roundtrip.
// (If your payload type isn’t supported, you’ll have to encode it externally.)
func encodeValue(arena *fastjson.Arena, v interface{}) (*fastjson.Value, error) {
	if v == nil {
		return arena.NewNull(), nil
	}
	if fv, ok := v.(*fastjson.Value); ok {
		return fv, nil
	}
	switch val := v.(type) {
	case []byte:
		var p fastjson.Parser
		fv, err := p.ParseBytes(val)
		return fv, err
	case string:
		return arena.NewString(val), nil
	case bool:
		if val {
			return arena.NewTrue(), nil
		} else {
			return arena.NewFalse(), nil
		}
	case int:
		return arena.NewNumberInt(val), nil
	case int8:
		return arena.NewNumberInt(int(val)), nil
	case int16:
		return arena.NewNumberInt(int(val)), nil
	case int32:
		return arena.NewNumberInt(int(val)), nil
	case int64:
		return arena.NewNumberInt(int(val)), nil
	case uint:
		return arena.NewNumberInt(int(val)), nil
	case uint8:
		return arena.NewNumberInt(int(val)), nil
	case uint16:
		return arena.NewNumberInt(int(val)), nil
	case uint32:
		return arena.NewNumberInt(int(val)), nil
	case uint64:
		return arena.NewNumberInt(int(val)), nil
	case float32:
		return arena.NewNumberFloat64(float64(val)), nil
	case float64:
		return arena.NewNumberFloat64(float64(val)), nil
	case map[string]interface{}:
		obj := arena.NewObject()
		for k, v2 := range val {
			encoded, err := encodeValue(arena, v2)
			if err != nil {
				return nil, err
			}
			obj.Set(k, encoded)
		}
		return obj, nil
	case []interface{}:
		return encodeArray(arena, val)
	default:
		return nil, fmt.Errorf("unsupported type for JSON encoding: %T", v)
	}
}

// encodeArray creates a JSON array from a []interface{} by encoding each element.
// Because fastjson does not have an efficient programmatic API for arrays, we
// build the JSON string and then re‑parse it.
func encodeArray(arena *fastjson.Arena, arr []interface{}) (*fastjson.Value, error) {
	var buf bytes.Buffer
	buf.WriteByte('[')
	for i, item := range arr {
		if i > 0 {
			buf.WriteByte(',')
		}
		elementVal, err := encodeValue(arena, item)
		if err != nil {
			return nil, err
		}
		buf.Write(elementVal.MarshalTo(nil))
	}
	buf.WriteByte(']')
	var p fastjson.Parser
	fv, err := p.ParseBytes(buf.Bytes())
	return fv, err
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

	payloadVal, err := encodeValue(arena, payload)
	if err != nil {
		return nil, err
	}
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
