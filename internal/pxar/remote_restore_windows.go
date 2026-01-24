//go:build windows

package pxar

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"strconv"
	"syscall"
	"unsafe"

	"github.com/pbs-plus/pbs-plus/internal/agent/agentfs/types"
	"golang.org/x/sys/windows"
)

var (
	modAdvapi32          = syscall.NewLazyDLL("advapi32.dll")
	procSetEntriesInAclW = modAdvapi32.NewProc("SetEntriesInAclW")
	procSetSecurityInfo  = modAdvapi32.NewProc("SetSecurityInfo")
)

func remoteApplyMeta(ctx context.Context, client *RemoteClient, file *os.File, e EntryInfo) error {
	defer file.Close()

	h := windows.Handle(file.Fd())

	xattrs, _ := client.ListXAttrs(ctx, e.EntryRangeStart, e.EntryRangeEnd)

	var c, a, w *windows.Filetime

	baseFt := unixToFiletime(e.MtimeSecs)
	w = &baseFt
	a = &baseFt

	if xattrs != nil {
		if d, ok := xattrs["user.creationtime"]; ok {
			if ts, err := strconv.ParseInt(string(d), 10, 64); err == nil {
				ft := unixToFiletime(ts)
				c = &ft
			}
		}
		if d, ok := xattrs["user.lastwritetime"]; ok {
			if ts, err := strconv.ParseInt(string(d), 10, 64); err == nil {
				ft := unixToFiletime(ts)
				w = &ft
			}
		}
		if d, ok := xattrs["user.lastaccesstime"]; ok {
			if ts, err := strconv.ParseInt(string(d), 10, 64); err == nil {
				ft := unixToFiletime(ts)
				a = &ft
			}
		}
	}

	_ = windows.SetFileTime(h, c, a, w)

	if xattrs != nil {
		if d, ok := xattrs["user.fileattributes"]; ok {
			var fa map[string]bool
			if json.Unmarshal(d, &fa) == nil {
				if attr := buildFileAttributes(fa); attr != 0 {
					_ = setFileAttributesByHandle(h, attr)
				}
			}
		}
		restoreWindowsACLsFromHandle(h, xattrs)
	}

	return nil
}

type FILE_BASIC_INFO struct {
	CreationTime   windows.Filetime
	LastAccessTime windows.Filetime
	LastWriteTime  windows.Filetime
	ChangeTime     windows.Filetime
	FileAttributes uint32
}

func setFileAttributesByHandle(h windows.Handle, attrs uint32) error {
	var info FILE_BASIC_INFO

	var existing windows.ByHandleFileInformation
	if err := windows.GetFileInformationByHandle(h, &existing); err != nil {
		return err
	}

	info.CreationTime = existing.CreationTime
	info.LastAccessTime = existing.LastAccessTime
	info.LastWriteTime = existing.LastWriteTime
	info.FileAttributes = attrs

	return windows.SetFileInformationByHandle(h, 0, (*byte)(unsafe.Pointer(&info)), uint32(unsafe.Sizeof(info)))
}

func restoreWindowsACLsFromHandle(h windows.Handle, xattrs map[string][]byte) {
	var secInfo windows.SECURITY_INFORMATION
	var o, g *windows.SID
	var dacl *windows.ACL

	if d, ok := xattrs["user.owner"]; ok {
		if sid, err := windows.StringToSid(string(d)); err == nil {
			o = sid
			secInfo |= windows.OWNER_SECURITY_INFORMATION
		}
	}
	if d, ok := xattrs["user.acls"]; ok {
		var winACLs []types.WinACL
		if json.Unmarshal(d, &winACLs) == nil {
			if a, err := buildDACLFromACEs(winACLs); err == nil {
				dacl = a
				secInfo |= windows.DACL_SECURITY_INFORMATION
			}
		}
	}

	if secInfo != 0 {
		_, _, _ = procSetSecurityInfo.Call(uintptr(h), uintptr(windows.SE_FILE_OBJECT), uintptr(secInfo),
			uintptr(unsafe.Pointer(o)), uintptr(unsafe.Pointer(g)), uintptr(unsafe.Pointer(dacl)), 0)
	}
}

func remoteApplyMetaSymlink(_ context.Context, _ *RemoteClient, _ string, _ EntryInfo) error {
	return nil
}

func unixToFiletime(unixTime int64) windows.Filetime {
	const ticksPerSecond = 10000000
	const epochDifference = 116444736000000000

	ticks := unixTime*ticksPerSecond + epochDifference
	return windows.Filetime{
		LowDateTime:  uint32(ticks & 0xFFFFFFFF),
		HighDateTime: uint32(ticks >> 32),
	}
}

func buildFileAttributes(fa map[string]bool) uint32 {
	var r uint32
	m := map[string]uint32{
		"FILE_ATTRIBUTE_READONLY": windows.FILE_ATTRIBUTE_READONLY, "FILE_ATTRIBUTE_HIDDEN": windows.FILE_ATTRIBUTE_HIDDEN,
		"FILE_ATTRIBUTE_SYSTEM": windows.FILE_ATTRIBUTE_SYSTEM, "FILE_ATTRIBUTE_ARCHIVE": windows.FILE_ATTRIBUTE_ARCHIVE,
	}
	for k, v := range m {
		if fa[k] {
			r |= v
		}
	}
	return r
}

func buildDACLFromACEs(winACLs []types.WinACL) (*windows.ACL, error) {
	if len(winACLs) == 0 {
		return nil, nil
	}

	entries := make([]windows.EXPLICIT_ACCESS, 0, len(winACLs))

	for _, acl := range winACLs {
		sid, err := windows.StringToSid(acl.SID)
		if err != nil {
			continue
		}

		entry := windows.EXPLICIT_ACCESS{
			AccessPermissions: windows.ACCESS_MASK(acl.AccessMask),
			AccessMode:        windows.ACCESS_MODE(acl.Type),
			Inheritance:       uint32(acl.Flags),
			Trustee: windows.TRUSTEE{
				TrusteeForm:  windows.TRUSTEE_IS_SID,
				TrusteeType:  windows.TRUSTEE_IS_UNKNOWN,
				TrusteeValue: windows.TrusteeValueFromSID(sid),
			},
		}
		entries = append(entries, entry)
	}

	if len(entries) == 0 {
		return nil, nil
	}

	var newACL *windows.ACL
	ret, _, err := procSetEntriesInAclW.Call(
		uintptr(len(entries)),
		uintptr(unsafe.Pointer(&entries[0])),
		0, // oldACL
		uintptr(unsafe.Pointer(&newACL)),
	)

	if ret != 0 {
		return nil, fmt.Errorf("SetEntriesInAcl failed: %w", err)
	}

	return newACL, nil
}
