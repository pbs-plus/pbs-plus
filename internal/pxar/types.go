package pxar

type Request struct {
	_msgpack struct{} `cbor:",toarray"`
	Variant  string
	Data     any
}

type Response map[string]any

type PxarReaderStats struct {
	ByteReadSpeed   float64
	FileAccessSpeed float64
	FilesAccessed   int64
	FoldersAccessed int64
	TotalAccessed   int64
	TotalBytes      uint64
	StatCacheHits   int64
}
