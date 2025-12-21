//go:build windows

package pxar

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"strconv"
	"syscall"
	"time"
	"unsafe"

	"github.com/pbs-plus/pbs-plus/internal/agent/agentfs/types"
	"github.com/pbs-plus/pbs-plus/internal/syslog"
	"golang.org/x/sys/windows"
)

var (
	modAdvapi32               = syscall.NewLazyDLL("advapi32.dll")
	procSetEntriesInAclW      = modAdvapi32.NewProc("SetEntriesInAclW")
	procSetNamedSecurityInfoW = modAdvapi32.NewProc("SetNamedSecurityInfoW")
)

func remoteApplyMeta(ctx context.Context, client *RemoteClient, path string, e EntryInfo) error {
	xattrs, err := client.ListXAttrs(ctx, e.EntryRangeStart, e.EntryRangeEnd)
	if err == nil && len(xattrs) > 0 {
		if err := restoreWindowsXattrs(path, xattrs); err != nil {
			syslog.L.Error(err).WithField("path", path).WithMessage("failed to restore metadata").Write()
		}
	}

	mt := time.Unix(e.MtimeSecs, int64(e.MtimeNsecs))
	_ = os.Chtimes(path, mt, mt)

	return nil
}

func remoteApplyMetaSymlink(_ context.Context, _ *RemoteClient, _ string, _ EntryInfo) error {
	return nil
}

func restoreWindowsXattrs(path string, xattrs map[string][]byte) error {
	pathPtr, err := syscall.UTF16PtrFromString(path)
	if err != nil {
		return err
	}

	handle, err := windows.CreateFile(
		pathPtr,
		windows.FILE_WRITE_ATTRIBUTES|windows.WRITE_OWNER|windows.WRITE_DAC,
		windows.FILE_SHARE_READ|windows.FILE_SHARE_WRITE,
		nil,
		windows.OPEN_EXISTING,
		windows.FILE_FLAG_BACKUP_SEMANTICS, // Needed for directories
		0,
	)
	if err != nil {
		return err
	}
	defer windows.CloseHandle(handle)

	var creationTime, lastAccessTime, lastWriteTime *windows.Filetime

	if data, ok := xattrs["user.creationtime"]; ok {
		if ts, err := strconv.ParseInt(string(data), 10, 64); err == nil {
			ft := unixToFiletime(ts)
			creationTime = &ft
		}
	}

	if data, ok := xattrs["user.lastaccesstime"]; ok {
		if ts, err := strconv.ParseInt(string(data), 10, 64); err == nil {
			ft := unixToFiletime(ts)
			lastAccessTime = &ft
		}
	}

	if data, ok := xattrs["user.lastwritetime"]; ok {
		if ts, err := strconv.ParseInt(string(data), 10, 64); err == nil {
			ft := unixToFiletime(ts)
			lastWriteTime = &ft
		}
	}

	if creationTime != nil || lastAccessTime != nil || lastWriteTime != nil {
		_ = windows.SetFileTime(handle, creationTime, lastAccessTime, lastWriteTime)
	}

	if data, ok := xattrs["user.fileattributes"]; ok {
		var fileAttrs map[string]bool
		if err := json.Unmarshal(data, &fileAttrs); err == nil {
			attrs := buildFileAttributes(fileAttrs)
			if attrs != 0 {
				_ = windows.SetFileAttributes(pathPtr, attrs)
			}
		}
	}

	_ = restoreWindowsACLs(path, xattrs)

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

func buildFileAttributes(attrs map[string]bool) uint32 {
	var result uint32

	if attrs["FILE_ATTRIBUTE_READONLY"] {
		result |= windows.FILE_ATTRIBUTE_READONLY
	}
	if attrs["FILE_ATTRIBUTE_HIDDEN"] {
		result |= windows.FILE_ATTRIBUTE_HIDDEN
	}
	if attrs["FILE_ATTRIBUTE_SYSTEM"] {
		result |= windows.FILE_ATTRIBUTE_SYSTEM
	}
	if attrs["FILE_ATTRIBUTE_ARCHIVE"] {
		result |= windows.FILE_ATTRIBUTE_ARCHIVE
	}
	if attrs["FILE_ATTRIBUTE_TEMPORARY"] {
		result |= windows.FILE_ATTRIBUTE_TEMPORARY
	}
	if attrs["FILE_ATTRIBUTE_COMPRESSED"] {
		result |= windows.FILE_ATTRIBUTE_COMPRESSED
	}
	if attrs["FILE_ATTRIBUTE_ENCRYPTED"] {
		result |= windows.FILE_ATTRIBUTE_ENCRYPTED
	}
	if attrs["FILE_ATTRIBUTE_NOT_CONTENT_INDEXED"] {
		result |= windows.FILE_ATTRIBUTE_NOT_CONTENT_INDEXED
	}
	if attrs["FILE_ATTRIBUTE_SPARSE_FILE"] {
		result |= windows.FILE_ATTRIBUTE_SPARSE_FILE
	}
	if attrs["FILE_ATTRIBUTE_REPARSE_POINT"] {
		result |= windows.FILE_ATTRIBUTE_REPARSE_POINT
	}
	if attrs["FILE_ATTRIBUTE_OFFLINE"] {
		result |= windows.FILE_ATTRIBUTE_OFFLINE
	}
	if attrs["FILE_ATTRIBUTE_INTEGRITY_STREAM"] {
		result |= windows.FILE_ATTRIBUTE_INTEGRITY_STREAM
	}
	if attrs["FILE_ATTRIBUTE_NO_SCRUB_DATA"] {
		result |= windows.FILE_ATTRIBUTE_NO_SCRUB_DATA
	}
	if attrs["FILE_ATTRIBUTE_RECALL_ON_OPEN"] {
		result |= windows.FILE_ATTRIBUTE_RECALL_ON_OPEN
	}
	if attrs["FILE_ATTRIBUTE_RECALL_ON_DATA_ACCESS"] {
		result |= windows.FILE_ATTRIBUTE_RECALL_ON_DATA_ACCESS
	}

	return result
}

func restoreWindowsACLs(path string, xattrs map[string][]byte) error {
	var ownerSID *windows.SID
	var groupSID *windows.SID
	var newDACL *windows.ACL
	var secInfo windows.SECURITY_INFORMATION

	if data, ok := xattrs["user.owner"]; ok && len(data) > 0 {
		sid, err := windows.StringToSid(string(data))
		if err == nil {
			ownerSID = sid
			secInfo |= windows.OWNER_SECURITY_INFORMATION
		}
	}

	if data, ok := xattrs["user.group"]; ok && len(data) > 0 {
		sid, err := windows.StringToSid(string(data))
		if err == nil {
			groupSID = sid
			secInfo |= windows.GROUP_SECURITY_INFORMATION
		}
	}

	if data, ok := xattrs["user.acls"]; ok && len(data) > 0 {
		var winACLs []types.WinACL
		if err := json.Unmarshal(data, &winACLs); err == nil && len(winACLs) > 0 {
			dacl, err := buildDACLFromACEs(winACLs)
			if err == nil && dacl != nil {
				newDACL = dacl
				secInfo |= windows.DACL_SECURITY_INFORMATION
			}
		}
	}

	if secInfo != 0 {
		err := setNamedSecurityInfo(
			path,
			windows.SE_FILE_OBJECT,
			secInfo,
			ownerSID,
			groupSID,
			newDACL,
			nil, // SACL
		)
		if err != nil {
			return fmt.Errorf("failed to set security info: %w", err)
		}
	}

	return nil
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

func setNamedSecurityInfo(
	path string,
	objectType windows.SE_OBJECT_TYPE,
	secInfo windows.SECURITY_INFORMATION,
	owner *windows.SID,
	group *windows.SID,
	dacl *windows.ACL,
	sacl *windows.ACL,
) error {
	pathPtr, err := syscall.UTF16PtrFromString(path)
	if err != nil {
		return err
	}

	var ownerPtr, groupPtr, daclPtr, saclPtr uintptr

	if owner != nil {
		ownerPtr = uintptr(unsafe.Pointer(owner))
	}
	if group != nil {
		groupPtr = uintptr(unsafe.Pointer(group))
	}
	if dacl != nil {
		daclPtr = uintptr(unsafe.Pointer(dacl))
	}
	if sacl != nil {
		saclPtr = uintptr(unsafe.Pointer(sacl))
	}

	ret, _, err := procSetNamedSecurityInfoW.Call(
		uintptr(unsafe.Pointer(pathPtr)),
		uintptr(objectType),
		uintptr(secInfo),
		ownerPtr,
		groupPtr,
		daclPtr,
		saclPtr,
	)

	if ret != 0 {
		return fmt.Errorf("SetNamedSecurityInfo failed: %w", err)
	}

	return nil
}
