package pxar

import (
	"fmt"
	"os"
	"time"
)

type FileType int

const (
	FileTypeFile FileType = iota
	FileTypeDirectory
	FileTypeSymlink
	FileTypeHardlink
	FileTypeDevice
	FileTypeFifo
	FileTypeSocket
)

type EntryInfo struct {
	FileName        []byte     `cbor:"file_name"`
	FileType        FileType   `cbor:"file_type"`
	EntryRangeStart uint64     `cbor:"entry_range_start"`
	EntryRangeEnd   uint64     `cbor:"entry_range_end"`
	ContentRange    *[2]uint64 `cbor:"content_range"`
	Mode            uint64     `cbor:"mode"`
	UID             uint32     `cbor:"uid"`
	GID             uint32     `cbor:"gid"`
	Size            uint64     `cbor:"size"`
	MtimeSecs       int64      `cbor:"mtime_secs"`
	MtimeNsecs      uint32     `cbor:"mtime_nsecs"`
}

func extractEntryInfo(c *PxarReader, resp Response) (*EntryInfo, error) {
	entryData, ok := resp["Entry"]
	if !ok {
		return nil, fmt.Errorf("unexpected response type")
	}

	entryMap, ok := entryData.(map[any]any)
	if !ok {
		return nil, fmt.Errorf("invalid entry data")
	}

	infoData, ok := entryMap["info"]
	if !ok {
		return nil, fmt.Errorf("missing info field")
	}

	infoBytes, err := c.enc.Marshal(infoData)
	if err != nil {
		return nil, fmt.Errorf("failed to re-encode info: %w", err)
	}

	var info EntryInfo
	if err := c.dec.Unmarshal(infoBytes, &info); err != nil {
		return nil, fmt.Errorf("failed to decode info: %w", err)
	}

	return &info, nil
}

func (e *EntryInfo) IsDir() bool {
	return e.FileType == FileTypeDirectory
}

func (e *EntryInfo) IsFile() bool {
	return e.FileType == FileTypeFile
}

func (e *EntryInfo) IsSymlink() bool {
	return e.FileType == FileTypeSymlink
}

func (e *EntryInfo) Name() string {
	return string(e.FileName)
}

func (e *EntryInfo) ToFileInfo() os.FileInfo {
	return &fileInfo{entry: e}
}

type fileInfo struct {
	entry *EntryInfo
}

func (fi *fileInfo) Name() string {
	return fi.entry.Name()
}

func (fi *fileInfo) Size() int64 {
	return int64(fi.entry.Size)
}

func (fi *fileInfo) Mode() os.FileMode {
	mode := os.FileMode(fi.entry.Mode & 0777)
	switch fi.entry.FileType {
	case FileTypeDirectory:
		mode |= os.ModeDir
	case FileTypeSymlink:
		mode |= os.ModeSymlink
	case FileTypeDevice:
		mode |= os.ModeDevice
	case FileTypeFifo:
		mode |= os.ModeNamedPipe
	case FileTypeSocket:
		mode |= os.ModeSocket
	}
	return mode
}

func (fi *fileInfo) ModTime() time.Time {
	return time.Unix(0, fi.entry.MtimeSecs)
}

func (fi *fileInfo) IsDir() bool {
	return fi.entry.IsDir()
}

func (fi *fileInfo) Sys() any {
	return fi.entry
}
