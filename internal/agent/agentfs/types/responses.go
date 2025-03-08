package types

import (
	"time"

	"github.com/sonroyaalmerol/pbs-plus/internal/arpc/arpcdata"
)

// LseekResp represents the response to a seek request
type LseekResp struct {
	NewOffset int64
}

func (resp *LseekResp) Encode() ([]byte, error) {
	enc := arpcdata.NewEncoder()
	if err := enc.WriteInt64(resp.NewOffset); err != nil {
		return nil, err
	}
	return enc.Bytes(), nil
}

func (resp *LseekResp) Decode(buf []byte) error {
	dec, err := arpcdata.NewDecoder(buf)
	if err != nil {
		return err
	}
	newOffset, err := dec.ReadInt64()
	if err != nil {
		return err
	}
	resp.NewOffset = newOffset
	return nil
}

// AgentFileInfo represents file metadata
type AgentFileInfo struct {
	Name    string
	Size    int64
	Mode    uint32
	ModTime time.Time
	IsDir   bool
	Blocks  uint64
}

func (info *AgentFileInfo) Encode() ([]byte, error) {
	enc := arpcdata.NewEncoderWithSize(len(info.Name) + 8 + 4 + 8 + 1 + 8)
	if err := enc.WriteString(info.Name); err != nil {
		return nil, err
	}
	if err := enc.WriteInt64(info.Size); err != nil {
		return nil, err
	}
	if err := enc.WriteUint32(info.Mode); err != nil {
		return nil, err
	}
	if err := enc.WriteTime(info.ModTime); err != nil {
		return nil, err
	}
	if err := enc.WriteBool(info.IsDir); err != nil {
		return nil, err
	}
	if err := enc.WriteUint64(info.Blocks); err != nil {
		return nil, err
	}
	return enc.Bytes(), nil
}

func (info *AgentFileInfo) Decode(buf []byte) error {
	dec, err := arpcdata.NewDecoder(buf)
	if err != nil {
		return err
	}
	name, err := dec.ReadString()
	if err != nil {
		return err
	}
	info.Name = name
	size, err := dec.ReadInt64()
	if err != nil {
		return err
	}
	info.Size = size
	mode, err := dec.ReadUint32()
	if err != nil {
		return err
	}
	info.Mode = mode
	modTime, err := dec.ReadTime()
	if err != nil {
		return err
	}
	info.ModTime = modTime
	isDir, err := dec.ReadBool()
	if err != nil {
		return err
	}
	info.IsDir = isDir
	blocks, err := dec.ReadUint64()
	if err != nil {
		return err
	}
	info.Blocks = blocks
	return nil
}

// AgentDirEntry represents a directory entry
type AgentDirEntry struct {
	Name string
	Mode uint32
}

func (entry *AgentDirEntry) Encode() ([]byte, error) {
	enc := arpcdata.NewEncoderWithSize(len(entry.Name) + 4)
	if err := enc.WriteString(entry.Name); err != nil {
		return nil, err
	}
	if err := enc.WriteUint32(entry.Mode); err != nil {
		return nil, err
	}
	return enc.Bytes(), nil
}

func (entry *AgentDirEntry) Decode(buf []byte) error {
	dec, err := arpcdata.NewDecoder(buf)
	if err != nil {
		return err
	}
	name, err := dec.ReadString()
	if err != nil {
		return err
	}
	entry.Name = name
	mode, err := dec.ReadUint32()
	if err != nil {
		return err
	}
	entry.Mode = mode
	return nil
}

// StatFS represents filesystem statistics
type StatFS struct {
	Bsize   uint64
	Blocks  uint64
	Bfree   uint64
	Bavail  uint64
	Files   uint64
	Ffree   uint64
	NameLen uint64
}

func (stat *StatFS) Encode() ([]byte, error) {
	enc := arpcdata.NewEncoderWithSize(8 * 7)
	if err := enc.WriteUint64(stat.Bsize); err != nil {
		return nil, err
	}
	if err := enc.WriteUint64(stat.Blocks); err != nil {
		return nil, err
	}
	if err := enc.WriteUint64(stat.Bfree); err != nil {
		return nil, err
	}
	if err := enc.WriteUint64(stat.Bavail); err != nil {
		return nil, err
	}
	if err := enc.WriteUint64(stat.Files); err != nil {
		return nil, err
	}
	if err := enc.WriteUint64(stat.Ffree); err != nil {
		return nil, err
	}
	if err := enc.WriteUint64(stat.NameLen); err != nil {
		return nil, err
	}
	return enc.Bytes(), nil
}

func (stat *StatFS) Decode(buf []byte) error {
	dec, err := arpcdata.NewDecoder(buf)
	if err != nil {
		return err
	}
	bsize, err := dec.ReadUint64()
	if err != nil {
		return err
	}
	stat.Bsize = bsize
	blocks, err := dec.ReadUint64()
	if err != nil {
		return err
	}
	stat.Blocks = blocks
	bfree, err := dec.ReadUint64()
	if err != nil {
		return err
	}
	stat.Bfree = bfree
	bavail, err := dec.ReadUint64()
	if err != nil {
		return err
	}
	stat.Bavail = bavail
	files, err := dec.ReadUint64()
	if err != nil {
		return err
	}
	stat.Files = files
	ffree, err := dec.ReadUint64()
	if err != nil {
		return err
	}
	stat.Ffree = ffree
	nameLen, err := dec.ReadUint64()
	if err != nil {
		return err
	}
	stat.NameLen = nameLen
	return nil
}
