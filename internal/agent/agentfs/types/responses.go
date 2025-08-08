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
	defer arpcdata.ReleaseDecoder(dec)

	newOffset, err := dec.ReadInt64()
	if err != nil {
		return err
	}
	resp.NewOffset = newOffset

	return nil
}

// WinACL represents an Access Control Entry
type WinACL struct {
	SID        string
	AccessMask uint32
	Type       uint8
	Flags      uint8
}

func (acl *WinACL) Reset() {
	acl.SID = ""
	acl.AccessMask = 0
	acl.Type = 0
	acl.Flags = 0
}

func (acl *WinACL) Encode() ([]byte, error) {
	enc := arpcdata.NewEncoderWithSize(len(acl.SID) + 4 + 1 + 1)
	if err := enc.WriteString(acl.SID); err != nil {
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

func (acl *WinACL) Decode(buf []byte) error {
	dec, err := arpcdata.NewDecoder(buf)
	if err != nil {
		return err
	}
	defer arpcdata.ReleaseDecoder(dec)

	sid, err := dec.ReadString()
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

	return nil
}

type WinACLArray []WinACL

func (acls *WinACLArray) Encode() ([]byte, error) {
	enc := arpcdata.NewEncoder()

	if err := enc.WriteUint32(uint32(len(*acls))); err != nil {
		return nil, err
	}

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

func (acls *WinACLArray) Decode(buf []byte) error {
	dec, err := arpcdata.NewDecoder(buf)
	if err != nil {
		return err
	}
	defer arpcdata.ReleaseDecoder(dec)

	count, err := dec.ReadUint32()
	if err != nil {
		return err
	}

	// Reuse existing slice capacity if possible
	if cap(*acls) >= int(count) {
		*acls = (*acls)[:count]
		// Reset existing entries
		for i := range *acls {
			(*acls)[i].Reset()
		}
	} else {
		*acls = make([]WinACL, count)
	}

	for i := uint32(0); i < count; i++ {
		aclBytes, err := dec.ReadBytes()
		if err != nil {
			return err
		}
		if err := (*acls)[i].Decode(aclBytes); err != nil {
			return err
		}
	}

	return nil
}

type PosixACL struct {
	Tag   string
	ID    int32
	Perms uint8
}

func (entry *PosixACL) Reset() {
	entry.Tag = ""
	entry.ID = 0
	entry.Perms = 0
}

func (entry *PosixACL) Encode() ([]byte, error) {
	enc := arpcdata.NewEncoderWithSize(len(entry.Tag) + 4 + 1)
	if err := enc.WriteString(entry.Tag); err != nil {
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

func (entry *PosixACL) Decode(buf []byte) error {
	dec, err := arpcdata.NewDecoder(buf)
	if err != nil {
		return err
	}
	defer arpcdata.ReleaseDecoder(dec)

	tag, err := dec.ReadString()
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

	return nil
}

type PosixACLArray []PosixACL

func (acls *PosixACLArray) Encode() ([]byte, error) {
	enc := arpcdata.NewEncoder()

	if err := enc.WriteUint32(uint32(len(*acls))); err != nil {
		return nil, err
	}

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

func (acls *PosixACLArray) Decode(buf []byte) error {
	dec, err := arpcdata.NewDecoder(buf)
	if err != nil {
		return err
	}
	defer arpcdata.ReleaseDecoder(dec)

	count, err := dec.ReadUint32()
	if err != nil {
		return err
	}

	// Reuse existing slice capacity if possible
	if cap(*acls) >= int(count) {
		*acls = (*acls)[:count]
		// Reset existing entries
		for i := range *acls {
			(*acls)[i].Reset()
		}
	} else {
		*acls = make([]PosixACL, count)
	}

	for i := uint32(0); i < count; i++ {
		aclBytes, err := dec.ReadBytes()
		if err != nil {
			return err
		}
		if err := (*acls)[i].Decode(aclBytes); err != nil {
			return err
		}
	}

	return nil
}

// AgentFileInfo represents file metadata
type AgentFileInfo struct {
	Name           string
	Size           int64
	Mode           uint32
	ModTime        time.Time
	IsDir          bool
	Blocks         uint64
	CreationTime   int64
	LastAccessTime int64
	LastWriteTime  int64
	FileAttributes map[string]bool
	Owner          string
	Group          string
	WinACLs        []WinACL
	PosixACLs      []PosixACL
}

func (info *AgentFileInfo) Reset() {
	info.Name = ""
	info.Size = 0
	info.Mode = 0
	info.ModTime = time.Time{}
	info.IsDir = false
	info.Blocks = 0
	info.CreationTime = 0
	info.LastAccessTime = 0
	info.LastWriteTime = 0
	if info.FileAttributes != nil {
		for k := range info.FileAttributes {
			delete(info.FileAttributes, k)
		}
	}
	info.Owner = ""
	info.Group = ""
	// Reset slices but keep capacity
	info.WinACLs = info.WinACLs[:0]
	info.PosixACLs = info.PosixACLs[:0]
}

func (info *AgentFileInfo) Encode() ([]byte, error) {
	enc := arpcdata.NewEncoder()

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

	if err := enc.WriteString(info.Owner); err != nil {
		return nil, err
	}
	if err := enc.WriteString(info.Group); err != nil {
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
	defer arpcdata.ReleaseDecoder(dec)

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

	owner, err := dec.ReadString()
	if err != nil {
		return err
	}
	info.Owner = owner

	group, err := dec.ReadString()
	if err != nil {
		return err
	}
	info.Group = group

	winAclsBytes, err := dec.ReadBytes()
	if err != nil {
		return err
	}
	winAcls := WinACLArray(info.WinACLs)
	if err := winAcls.Decode(winAclsBytes); err != nil {
		return err
	}
	info.WinACLs = winAcls

	posixAclsBytes, err := dec.ReadBytes()
	if err != nil {
		return err
	}
	posixAcls := PosixACLArray(info.PosixACLs)
	if err := posixAcls.Decode(posixAclsBytes); err != nil {
		return err
	}
	info.PosixACLs = posixAcls

	return nil
}

// AgentDirEntry represents a directory entry
type AgentDirEntry struct {
	Name string
	Mode uint32
}

func (entry *AgentDirEntry) Reset() {
	entry.Name = ""
	entry.Mode = 0
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
	defer arpcdata.ReleaseDecoder(dec)

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
	defer arpcdata.ReleaseDecoder(dec)

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
