package types

import (
	"time"

	"github.com/pbs-plus/pbs-plus/internal/arpc"
	"github.com/pbs-plus/pbs-plus/internal/arpc/arpcdata"
)

// LseekResp represents the response to a seek request
type LseekResp struct {
	NewOffset int64
}

func (resp *LseekResp) Encode() ([]byte, error) {
	enc := arpcdata.NewEncoderWithSize(8)
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
	arpcdata.ReleaseDecoder(dec)
	return nil
}

// WinACL represents an Access Control Entry
type WinACL struct {
	SID        []byte
	AccessMask uint32
	Type       uint8
	Flags      uint8
}

// Encode encodes a single WinACL into a byte slice
func (acl *WinACL) Encode() ([]byte, error) {
	enc := arpcdata.NewEncoderWithSize(len(acl.SID) + 4 + 1 + 1)

	if err := enc.WriteBytes(acl.SID); err != nil {
		return nil, err
	}
	if err := enc.WriteUint32(acl.AccessMask); err != nil {
		return nil, err
	}
	if err := enc.WriteUint8(acl.Type); err != nil {
		return nil, err
	}
	if err := enc.WriteUint8(acl.Flags); err != nil {
		return nil, err
	}

	return enc.Bytes(), nil
}

// Decode decodes a byte slice into a single WinACL
func (acl *WinACL) Decode(buf []byte) error {
	dec, err := arpcdata.NewDecoder(buf)
	if err != nil {
		return err
	}

	sid, err := dec.ReadBytes()
	if err != nil {
		return err
	}
	acl.SID = sid

	accessMask, err := dec.ReadUint32()
	if err != nil {
		return err
	}
	acl.AccessMask = accessMask

	aceType, err := dec.ReadUint8()
	if err != nil {
		return err
	}
	acl.Type = aceType

	flags, err := dec.ReadUint8()
	if err != nil {
		return err
	}
	acl.Flags = flags

	arpcdata.ReleaseDecoder(dec)
	return nil
}

type WinACLArray []WinACL

// Encode encodes an array of WinACLs into a byte slice
func (acls *WinACLArray) Encode() ([]byte, error) {
	enc := arpcdata.NewEncoder()

	// Write the number of WinACLs
	if err := enc.WriteUint32(uint32(len(*acls))); err != nil {
		return nil, err
	}

	// Encode each WinACL and append it to the encoder
	for _, acl := range *acls {
		aclBytes, err := acl.Encode()
		if err != nil {
			return nil, err
		}
		if err := enc.WriteBytes(aclBytes); err != nil {
			return nil, err
		}
	}

	return enc.Bytes(), nil
}

// Decode decodes a byte slice into an array of WinACLs
func (acls *WinACLArray) Decode(buf []byte) error {
	dec, err := arpcdata.NewDecoder(buf)
	if err != nil {
		return err
	}

	// Read the number of WinACLs
	count, err := dec.ReadUint32()
	if err != nil {
		return err
	}

	// Decode each WinACL
	*acls = make([]WinACL, count)
	for i := uint32(0); i < count; i++ {
		aclBytes, err := dec.ReadBytes()
		if err != nil {
			return err
		}
		var acl WinACL
		if err := acl.Decode(aclBytes); err != nil {
			return err
		}
		(*acls)[i] = acl
	}

	arpcdata.ReleaseDecoder(dec)
	return nil
}

type PosixACL struct {
	Tag   []byte
	ID    int32
	Perms uint8
}

func (entry *PosixACL) Encode() ([]byte, error) {
	enc := arpcdata.NewEncoderWithSize(len(entry.Tag) + 4 + 1)

	if err := enc.WriteBytes(entry.Tag); err != nil {
		return nil, err
	}
	if err := enc.WriteUint32(uint32(entry.ID)); err != nil {
		return nil, err
	}
	if err := enc.WriteByte(entry.Perms); err != nil {
		return nil, err
	}

	return enc.Bytes(), nil
}

// Decode deserializes a byte slice into a PosixACL.
func (entry *PosixACL) Decode(buf []byte) error {
	dec, err := arpcdata.NewDecoder(buf)
	if err != nil {
		return err
	}

	tag, err := dec.ReadBytes()
	if err != nil {
		return err
	}
	entry.Tag = tag

	id, err := dec.ReadUint32()
	if err != nil {
		return err
	}
	entry.ID = int32(id)

	perms, err := dec.ReadByte()
	if err != nil {
		return err
	}
	entry.Perms = perms

	arpcdata.ReleaseDecoder(dec)
	return nil
}

type PosixACLArray []PosixACL

// Encode encodes an array of WinACLs into a byte slice
func (acls *PosixACLArray) Encode() ([]byte, error) {
	enc := arpcdata.NewEncoder()

	// Write the number of WinACLs
	if err := enc.WriteUint32(uint32(len(*acls))); err != nil {
		return nil, err
	}

	// Encode each WinACL and append it to the encoder
	for _, acl := range *acls {
		aclBytes, err := acl.Encode()
		if err != nil {
			return nil, err
		}
		if err := enc.WriteBytes(aclBytes); err != nil {
			return nil, err
		}
	}

	return enc.Bytes(), nil
}

// Decode decodes a byte slice into an array of WinACLs
func (acls *PosixACLArray) Decode(buf []byte) error {
	dec, err := arpcdata.NewDecoder(buf)
	if err != nil {
		return err
	}

	// Read the number of WinACLs
	count, err := dec.ReadUint32()
	if err != nil {
		return err
	}

	// Decode each WinACL
	*acls = make([]PosixACL, count)
	for i := uint32(0); i < count; i++ {
		aclBytes, err := dec.ReadBytes()
		if err != nil {
			return err
		}
		var acl PosixACL
		if err := acl.Decode(aclBytes); err != nil {
			return err
		}
		(*acls)[i] = acl
	}

	arpcdata.ReleaseDecoder(dec)
	return nil
}

// AgentFileInfo represents file metadata
type AgentFileInfo struct {
	Name           []byte
	Size           int64
	Mode           uint32
	ModTime        time.Time
	IsDir          bool
	Blocks         uint64
	CreationTime   int64
	LastAccessTime int64
	LastWriteTime  int64
	FileAttributes map[string]bool
	Owner          []byte
	Group          []byte
	WinACLs        []WinACL
	PosixACLs      []PosixACL
}

func (info *AgentFileInfo) Encode() ([]byte, error) {
	enc := arpcdata.NewEncoder()

	if err := enc.WriteBytes(info.Name); err != nil {
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

	if err := enc.WriteInt64(info.CreationTime); err != nil {
		return nil, err
	}
	if err := enc.WriteInt64(info.LastAccessTime); err != nil {
		return nil, err
	}
	if err := enc.WriteInt64(info.LastWriteTime); err != nil {
		return nil, err
	}

	fileAttributes := arpc.MapStringBoolMsg(info.FileAttributes)
	fileAttributesBytes, err := fileAttributes.Encode()
	if err != nil {
		return nil, err
	}
	if err := enc.WriteBytes(fileAttributesBytes); err != nil {
		return nil, err
	}

	if err := enc.WriteBytes(info.Owner); err != nil {
		return nil, err
	}
	if err := enc.WriteBytes(info.Group); err != nil {
		return nil, err
	}

	winAcls := WinACLArray(info.WinACLs)
	winAclsBytes, err := winAcls.Encode()
	if err != nil {
		return nil, err
	}
	if err := enc.WriteBytes(winAclsBytes); err != nil {
		return nil, err
	}

	posixAcls := PosixACLArray(info.PosixACLs)
	posixAclsBytes, err := posixAcls.Encode()
	if err != nil {
		return nil, err
	}
	if err := enc.WriteBytes(posixAclsBytes); err != nil {
		return nil, err
	}

	return enc.Bytes(), nil
}

func (info *AgentFileInfo) Decode(buf []byte) error {
	dec, err := arpcdata.NewDecoder(buf)
	if err != nil {
		return err
	}

	name, err := dec.ReadBytes()
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

	creationTime, err := dec.ReadInt64()
	if err != nil {
		return err
	}
	info.CreationTime = creationTime

	lastAccessTime, err := dec.ReadInt64()
	if err != nil {
		return err
	}
	info.LastAccessTime = lastAccessTime

	lastWriteTime, err := dec.ReadInt64()
	if err != nil {
		return err
	}
	info.LastWriteTime = lastWriteTime

	fileAttributesBytes, err := dec.ReadBytes()
	if err != nil {
		return err
	}
	var fileAttributes arpc.MapStringBoolMsg
	if err := fileAttributes.Decode(fileAttributesBytes); err != nil {
		return err
	}
	info.FileAttributes = fileAttributes

	owner, err := dec.ReadBytes()
	if err != nil {
		return err
	}
	info.Owner = owner

	group, err := dec.ReadBytes()
	if err != nil {
		return err
	}
	info.Group = group

	winAclsBytes, err := dec.ReadBytes()
	if err != nil {
		return err
	}
	var winAcls WinACLArray
	if err := winAcls.Decode(winAclsBytes); err != nil {
		return err
	}
	info.WinACLs = winAcls

	posixAclsBytes, err := dec.ReadBytes()
	if err != nil {
		return err
	}
	var posixAcls PosixACLArray
	if err := posixAcls.Decode(posixAclsBytes); err != nil {
		return err
	}
	info.PosixACLs = posixAcls

	arpcdata.ReleaseDecoder(dec)

	return nil
}

// AgentDirEntry represents a directory entry
type AgentDirEntry struct {
	Name []byte
	Mode uint32
}

func (entry *AgentDirEntry) Encode() ([]byte, error) {
	enc := arpcdata.NewEncoderWithSize(len(entry.Name) + 4)
	if err := enc.WriteBytes(entry.Name); err != nil {
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
	name, err := dec.ReadBytes()
	if err != nil {
		return err
	}
	entry.Name = name
	mode, err := dec.ReadUint32()
	if err != nil {
		return err
	}
	entry.Mode = mode
	arpcdata.ReleaseDecoder(dec)
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
	arpcdata.ReleaseDecoder(dec)
	return nil
}
