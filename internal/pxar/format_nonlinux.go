//go:build !linux

package pxar

import (
	"context"
	"io"
)

// Stub for non-Linux builds. PxarReader is only available on Linux (PBS server).

// PxarReaderStats is a stub for non-Linux builds.
type PxarReaderStats struct{}

type PxarReader struct{}

func (*PxarReader) Close() error                                                   { return nil }
func (*PxarReader) GetRoot(_ context.Context) (*EntryInfo, error)                  { return nil, nil }
func (*PxarReader) LookupByPath(_ context.Context, _ string) (*EntryInfo, error)   { return nil, nil }
func (*PxarReader) ReadDir(_ context.Context, _ uint64) ([]EntryInfo, error)       { return nil, nil }
func (*PxarReader) GetAttr(_ context.Context, _, _ uint64) (*EntryInfo, error)     { return nil, nil }
func (*PxarReader) ReadLink(_ context.Context, _, _ uint64) ([]byte, error)        { return nil, nil }
func (*PxarReader) Read(_ context.Context, _, _, _ uint64, _ uint) ([]byte, error) { return nil, nil }
func (*PxarReader) ListXAttrs(_ context.Context, _, _ uint64) (map[string][]byte, error) {
	return nil, nil
}
func (*PxarReader) ReadFileContentReader(_ context.Context, _, _ uint64) (io.ReadCloser, error) {
	return nil, nil
}
func (*PxarReader) GetStats() PxarReaderStats { return PxarReaderStats{} }
