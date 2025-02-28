package arpc

// buildRequestMsgpack builds a MessagePack‑encoded RPC request.
// It sets the method name, marshals the payload (using msgpack)
// and any extra headers provided.
func buildRequestMsgpack(method string, payload []byte, extraHeaders map[string]string) ([]byte, error) {
	req := Request{
		Method:  method,
		Payload: payload,
		Headers: extraHeaders,
	}

	return req.MarshalMsg(nil)
}
