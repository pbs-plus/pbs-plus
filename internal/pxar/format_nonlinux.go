//go:build !linux

package pxar

import "context"

// for compatibility only
// local pxarreader can only be from a PBS server host
type PxarReader struct {
}

func (c *PxarReader) Close() error {
	return nil
}

func (c *PxarReader) GetRoot(_ context.Context) (*EntryInfo, error) {
	return nil, nil
}

func (c *PxarReader) LookupByPath(_ context.Context, path string) (*EntryInfo, error) {
	return nil, nil
}

func (c *PxarReader) ReadDir(_ context.Context, entryEnd uint64) ([]EntryInfo, error) {
	return nil, nil
}

func (c *PxarReader) GetAttr(_ context.Context, entryStart, entryEnd uint64) (*EntryInfo, error) {
	return nil, nil
}

func (c *PxarReader) ReadLink(_ context.Context, entryStart, entryEnd uint64) ([]byte, error) {
	return nil, nil
}

func (c *PxarReader) Read(_ context.Context, contentStart, contentEnd, offset uint64, size uint) ([]byte, error) {
	return nil, nil
}

func (c *PxarReader) ListXAttrs(_ context.Context, entryStart, entryEnd uint64) (map[string][]byte, error) {
	return nil, nil
}
