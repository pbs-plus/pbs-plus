package pxar

type readContentReq struct {
	ContentStart uint64 `cbor:"content_start"`
	ContentEnd   uint64 `cbor:"content_end"`
	FileSize     uint64 `cbor:"file_size"`
	Length       int    `cbor:"length"`
}

type readContentAtReq struct {
	HandleID uint64 `cbor:"handle_id"`
	Offset   int64  `cbor:"offset"`
	Length   int    `cbor:"length"`
}

type closeContentReq struct {
	HandleID uint64 `cbor:"handle_id"`
}

type handleIDResp struct {
	HandleID uint64 `cbor:"handle_id"`
}

type lookupByPathReq struct {
	Path string `cbor:"path"`
}

type readDirReq struct {
	EntryEnd uint64 `cbor:"entry_end"`
}

type getAttrReq struct {
	EntryStart uint64 `cbor:"entry_start"`
	EntryEnd   uint64 `cbor:"entry_end"`
}

type readLinkReq struct {
	EntryStart uint64 `cbor:"entry_start"`
	EntryEnd   uint64 `cbor:"entry_end"`
}

type listXAttrsReq struct {
	EntryStart uint64 `cbor:"entry_start"`
	EntryEnd   uint64 `cbor:"entry_end"`
}

type errorReq struct {
	Error string `cbor:"error"`
}
