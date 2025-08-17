package types

import (
	"time"

	"github.com/pbs-plus/pbs-plus/internal/arpc"
	"github.com/pbs-plus/pbs-plus/internal/arpc/arpcdata"
)

// AgentDirEntryPlus is a "readdir+attr+xattr" entry.
type AgentDirEntryPlus struct {
	// Identity
	Name string
	Mode uint32 // same as AgentDirEntry.Mode and AgentFileInfo.Mode

	// Stat-like
	Size    int64
	ModTime time.Time
	IsDir   bool
	Blocks  uint64 // compute from AllocationSize and fs block size on server

	// Xattr-like
	CreationTime   int64
	LastAccessTime int64
	LastWriteTime  int64
	FileAttributes map[string]bool

	// Optional ACLs
	HasACL    bool
	Owner     string
	Group     string
	WinACLs   []WinACL
	PosixACLs []PosixACL
}

// Encode serializes AgentDirEntryPlus using the same arpcdata style
// as AgentFileInfo. Order must match Decode.
func (e *AgentDirEntryPlus) Encode() ([]byte, error) {
	enc := arpcdata.NewEncoder()

	if err := enc.WriteString(e.Name); err != nil {
		return nil, err
	}
	if err := enc.WriteUint32(e.Mode); err != nil {
		return nil, err
	}

	if err := enc.WriteInt64(e.Size); err != nil {
		return nil, err
	}
	if err := enc.WriteTime(e.ModTime); err != nil {
		return nil, err
	}
	if err := enc.WriteBool(e.IsDir); err != nil {
		return nil, err
	}
	if err := enc.WriteUint64(e.Blocks); err != nil {
		return nil, err
	}

	if err := enc.WriteInt64(e.CreationTime); err != nil {
		return nil, err
	}
	if err := enc.WriteInt64(e.LastAccessTime); err != nil {
		return nil, err
	}
	if err := enc.WriteInt64(e.LastWriteTime); err != nil {
		return nil, err
	}

	// FileAttributes map
	fa := arpc.MapStringBoolMsg(e.FileAttributes)
	faBytes, err := fa.Encode()
	if err != nil {
		return nil, err
	}
	if err := enc.WriteBytes(faBytes); err != nil {
		return nil, err
	}

	// ACL presence flag
	if err := enc.WriteBool(e.HasACL); err != nil {
		return nil, err
	}

	// Conditionally include ACL payload to avoid paying cost when absent
	if e.HasACL {
		if err := enc.WriteString(e.Owner); err != nil {
			return nil, err
		}
		if err := enc.WriteString(e.Group); err != nil {
			return nil, err
		}

		win := WinACLArray(e.WinACLs)
		winBytes, err := win.Encode()
		if err != nil {
			return nil, err
		}
		if err := enc.WriteBytes(winBytes); err != nil {
			return nil, err
		}

		posix := PosixACLArray(e.PosixACLs)
		posixBytes, err := posix.Encode()
		if err != nil {
			return nil, err
		}
		if err := enc.WriteBytes(posixBytes); err != nil {
			return nil, err
		}
	}

	return enc.Bytes(), nil
}

// Decode deserializes AgentDirEntryPlus. Order must match Encode.
func (e *AgentDirEntryPlus) Decode(buf []byte) error {
	dec, err := arpcdata.NewDecoder(buf)
	if err != nil {
		return err
	}
	defer arpcdata.ReleaseDecoder(dec)

	name, err := dec.ReadString()
	if err != nil {
		return err
	}
	e.Name = name

	mode, err := dec.ReadUint32()
	if err != nil {
		return err
	}
	e.Mode = mode

	size, err := dec.ReadInt64()
	if err != nil {
		return err
	}
	e.Size = size

	modTime, err := dec.ReadTime()
	if err != nil {
		return err
	}
	e.ModTime = modTime

	isDir, err := dec.ReadBool()
	if err != nil {
		return err
	}
	e.IsDir = isDir

	blocks, err := dec.ReadUint64()
	if err != nil {
		return err
	}
	e.Blocks = blocks

	creation, err := dec.ReadInt64()
	if err != nil {
		return err
	}
	e.CreationTime = creation

	atime, err := dec.ReadInt64()
	if err != nil {
		return err
	}
	e.LastAccessTime = atime

	mtime, err := dec.ReadInt64()
	if err != nil {
		return err
	}
	e.LastWriteTime = mtime

	faBytes, err := dec.ReadBytes()
	if err != nil {
		return err
	}
	var fa arpc.MapStringBoolMsg
	if err := fa.Decode(faBytes); err != nil {
		return err
	}
	e.FileAttributes = fa

	hasACL, err := dec.ReadBool()
	if err != nil {
		return err
	}
	e.HasACL = hasACL

	if e.HasACL {
		owner, err := dec.ReadString()
		if err != nil {
			return err
		}
		e.Owner = owner

		group, err := dec.ReadString()
		if err != nil {
			return err
		}
		e.Group = group

		winBytes, err := dec.ReadBytes()
		if err != nil {
			return err
		}
		var win WinACLArray
		if err := win.Decode(winBytes); err != nil {
			return err
		}
		e.WinACLs = win

		posixBytes, err := dec.ReadBytes()
		if err != nil {
			return err
		}
		var posix PosixACLArray
		if err := posix.Decode(posixBytes); err != nil {
			return err
		}
		e.PosixACLs = posix
	} else {
		// Ensure zero values when ACLs are absent
		e.Owner = ""
		e.Group = ""
		e.WinACLs = nil
		e.PosixACLs = nil
	}

	return nil
}

// ReadDirPlusBatch is a slice of AgentDirEntryPlus with Encode/Decode helpers.
// Mirrors your ReadDirEntries pattern.
type ReadDirPlusBatch []AgentDirEntryPlus

func (b *ReadDirPlusBatch) Encode() ([]byte, error) {
	enc := arpcdata.NewEncoder()

	if err := enc.WriteUint32(uint32(len(*b))); err != nil {
		return nil, err
	}

	for _, entry := range *b {
		entryBytes, err := entry.Encode()
		if err != nil {
			return nil, err
		}
		if err := enc.WriteBytes(entryBytes); err != nil {
			return nil, err
		}
	}

	return enc.Bytes(), nil
}

func (b *ReadDirPlusBatch) Decode(buf []byte) error {
	dec, err := arpcdata.NewDecoder(buf)
	if err != nil {
		return err
	}
	defer arpcdata.ReleaseDecoder(dec)

	count, err := dec.ReadUint32()
	if err != nil {
		return err
	}

	*b = make([]AgentDirEntryPlus, count)
	for i := uint32(0); i < count; i++ {
		entryBytes, err := dec.ReadBytes()
		if err != nil {
			return err
		}
		var e AgentDirEntryPlus
		if err := e.Decode(entryBytes); err != nil {
			return err
		}
		(*b)[i] = e
	}
	return nil
}

// Helper: convert DirEntryPlus -> AgentFileInfo when a caller still uses
// the existing attr/xattr APIs. This avoids syscalls by using the merged data.
func (e *AgentDirEntryPlus) ToAgentFileInfo() AgentFileInfo {
	return AgentFileInfo{
		Name:           e.Name,
		Size:           e.Size,
		Mode:           e.Mode,
		ModTime:        e.ModTime,
		IsDir:          e.IsDir,
		Blocks:         e.Blocks,
		CreationTime:   e.CreationTime,
		LastAccessTime: e.LastAccessTime,
		LastWriteTime:  e.LastWriteTime,
		FileAttributes: e.FileAttributes,
		Owner:          e.Owner,
		Group:          e.Group,
		WinACLs:        e.WinACLs,
		PosixACLs:      e.PosixACLs,
	}
}

type ReadDirPlusReq struct {
	HandleID        FileHandleId
	IncludeACLs     bool
	MaxACLsPerBatch uint32 // 0 => server default
}

func (req *ReadDirPlusReq) Encode() ([]byte, error) {
	enc := arpcdata.NewEncoderWithSize(8 + 1 + 4)
	if err := enc.WriteUint64(uint64(req.HandleID)); err != nil {
		return nil, err
	}
	if err := enc.WriteBool(req.IncludeACLs); err != nil {
		return nil, err
	}
	if err := enc.WriteUint32(req.MaxACLsPerBatch); err != nil {
		return nil, err
	}
	return enc.Bytes(), nil
}

func (req *ReadDirPlusReq) Decode(buf []byte) error {
	dec, err := arpcdata.NewDecoder(buf)
	if err != nil {
		return err
	}
	defer arpcdata.ReleaseDecoder(dec)

	h, err := dec.ReadUint64()
	if err != nil {
		return err
	}
	req.HandleID = FileHandleId(h)

	inc, err := dec.ReadBool()
	if err != nil {
		return err
	}
	req.IncludeACLs = inc

	n, err := dec.ReadUint32()
	if err != nil {
		return err
	}
	req.MaxACLsPerBatch = n
	return nil
}
