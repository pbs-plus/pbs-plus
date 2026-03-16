package pxar

import "context"

type Reader interface {
	Close() error
	GetRoot(ctx context.Context) (*EntryInfo, error)
	LookupByPath(ctx context.Context, path string) (*EntryInfo, error)
	ReadDir(ctx context.Context, entryEnd uint64) ([]EntryInfo, error)
	GetAttr(ctx context.Context, entryStart, entryEnd uint64) (*EntryInfo, error)
	Read(ctx context.Context, contentStart, contentEnd, offset uint64, size uint) ([]byte, error)
	ReadLink(ctx context.Context, entryStart, entryEnd uint64) ([]byte, error)
	ListXAttrs(ctx context.Context, entryStart, entryEnd uint64) (map[string][]byte, error)
	GetStats() PxarReaderStats
}

type PxarReaderStats struct {
	ByteReadSpeed   float64
	FileAccessSpeed float64
	FilesAccessed   int64
	FoldersAccessed int64
	TotalAccessed   int64
	TotalBytes      uint64
	StatCacheHits   int64
}
