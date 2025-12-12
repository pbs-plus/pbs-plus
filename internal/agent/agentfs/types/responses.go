package types

import (
	"time"
)

// LseekResp represents the response to a seek request
type LseekResp struct {
	NewOffset int64
}

func (resp *LseekResp) Encode() ([]byte, error) {
	return cborEncMode.Marshal(resp)
}

func (resp *LseekResp) Decode(buf []byte) error {
	return cborDecMode.Unmarshal(buf, resp)
}

// WinACL represents an Access Control Entry
type WinACL struct {
	SID        string
	AccessMask uint32
	Type       uint8
	Flags      uint8
}

// Encode encodes a single WinACL into a byte slice
func (acl *WinACL) Encode() ([]byte, error) {
	return cborEncMode.Marshal(acl)
}

// Decode decodes a byte slice into a single WinACL
func (acl *WinACL) Decode(buf []byte) error {
	return cborDecMode.Unmarshal(buf, acl)
}

type WinACLArray []WinACL

// Encode encodes an array of WinACLs into a byte slice
func (acls *WinACLArray) Encode() ([]byte, error) {
	return cborEncMode.Marshal(acls)
}

// Decode decodes a byte slice into an array of WinACLs
func (acls *WinACLArray) Decode(buf []byte) error {
	return cborDecMode.Unmarshal(buf, acls)
}

type PosixACL struct {
	Tag   string
	ID    int32
	Perms uint8
}

func (entry *PosixACL) Encode() ([]byte, error) {
	return cborEncMode.Marshal(entry)
}

// Decode deserializes a byte slice into a PosixACL.
func (entry *PosixACL) Decode(buf []byte) error {
	return cborDecMode.Unmarshal(buf, entry)
}

type PosixACLArray []PosixACL

// Encode encodes an array of WinACLs into a byte slice
func (acls *PosixACLArray) Encode() ([]byte, error) {
	return cborEncMode.Marshal(acls)
}

// Decode decodes a byte slice into an array of WinACLs
func (acls *PosixACLArray) Decode(buf []byte) error {
	return cborDecMode.Unmarshal(buf, acls)
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

func (info *AgentFileInfo) Encode() ([]byte, error) {
	return cborEncMode.Marshal(info)
}

func (info *AgentFileInfo) Decode(buf []byte) error {
	return cborDecMode.Unmarshal(buf, info)
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
	return cborEncMode.Marshal(stat)
}

func (stat *StatFS) Decode(buf []byte) error {
	return cborDecMode.Unmarshal(buf, stat)
}

// ReadDirEntries is a slice of AgentFileInfo
type ReadDirEntries []AgentFileInfo

func (entries *ReadDirEntries) Encode() ([]byte, error) {
	return cborEncMode.Marshal(entries)
}

// DecodeAgentDirEntries decodes a byte slice into an array of AgentFileInfo
func (entries *ReadDirEntries) Decode(buf []byte) error {
	return cborDecMode.Unmarshal(buf, entries)
}
