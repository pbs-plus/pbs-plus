//go:build !linux

package pxar

// for compatibility only
// local pxarreader can only be from a PBS server host
type PxarReader struct {
}

func (c *PxarReader) Close() error {
	return nil
}

func (c *PxarReader) GetRoot() (*EntryInfo, error) {
	return nil, nil
}

func (c *PxarReader) LookupByPath(path string) (*EntryInfo, error) {
	return nil, nil
}

func (c *PxarReader) ReadDir(entryEnd uint64) ([]EntryInfo, error) {
	return nil, nil
}

func (c *PxarReader) GetAttr(entryStart, entryEnd uint64) (*EntryInfo, error) {
	return nil, nil
}

func (c *PxarReader) ReadLink(entryStart, entryEnd uint64) ([]byte, error) {
	return nil, nil
}

func (c *PxarReader) Read(contentStart, contentEnd, offset uint64, size uint) ([]byte, error) {
	return nil, nil
}

func (c *PxarReader) ListXAttrs(entryStart, entryEnd uint64) (map[string][]byte, error) {
	return nil, nil
}
