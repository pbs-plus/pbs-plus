package pxar

type Request struct {
	_msgpack struct{} `cbor:",toarray"`
	Variant  string
	Data     any
}

type Response map[string]any
