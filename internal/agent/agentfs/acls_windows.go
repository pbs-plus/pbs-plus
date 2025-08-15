//go:build windows

package agentfs

import (
	"errors"
	"fmt"
	"unsafe"

	"github.com/pbs-plus/pbs-plus/internal/agent/agentfs/types"
	"golang.org/x/sys/windows"
)

// GetWinACLsHandle retrieves Owner, Group, and DACL ACEs.
func GetWinACLsHandle(h windows.Handle) (owner string, group string, acls []types.WinACL, err error) {
	const si = windows.OWNER_SECURITY_INFORMATION |
		windows.GROUP_SECURITY_INFORMATION |
		windows.DACL_SECURITY_INFORMATION

	sd, err := windows.GetSecurityInfo(h, windows.SE_FILE_OBJECT, si)
	if err != nil {
		return "", "", nil, fmt.Errorf("GetSecurityInfo: %w", err)
	}

	// Owner
	pOwner, _, err := sd.Owner()
	if err != nil && !errors.Is(err, windows.ERROR_OBJECT_NOT_FOUND) {
		return "", "", nil, fmt.Errorf("SECURITY_DESCRIPTOR.Owner: %w", err)
	}
	if pOwner != nil && pOwner.IsValid() {
		if s, e := sidToString(pOwner); e == nil {
			owner = s
		} else {
			return "", "", nil, fmt.Errorf("owner SID stringify: %w", e)
		}
	}

	// Group
	pGroup, _, err := sd.Group()
	if err != nil && !errors.Is(err, windows.ERROR_OBJECT_NOT_FOUND) {
		return "", "", nil, fmt.Errorf("SECURITY_DESCRIPTOR.Group: %w", err)
	}
	if pGroup != nil && pGroup.IsValid() {
		if s, e := sidToString(pGroup); e == nil {
			group = s
		} else {
			return "", "", nil, fmt.Errorf("group SID stringify: %w", e)
		}
	}

	// DACL
	pDacl, _, err := sd.DACL()
	if err != nil {
		if errors.Is(err, windows.ERROR_OBJECT_NOT_FOUND) {
			return owner, group, []types.WinACL{}, nil
		}
		return owner, group, nil, fmt.Errorf("SECURITY_DESCRIPTOR.DACL: %w", err)
	}
	if pDacl == nil {
		return owner, group, []types.WinACL{}, nil
	}

	// Iterate ACEs: GetAce(acl *ACL, aceIndex uint32, pAce **ACCESS_ALLOWED_ACE)
	result := make([]types.WinACL, 0, 8)
	for idx := uint32(0); ; idx++ {
		var acePtr *windows.ACCESS_ALLOWED_ACE
		if err := windows.GetAce(pDacl, idx, &acePtr); err != nil {
			if errors.Is(err, windows.ERROR_NO_MORE_ITEMS) || errors.Is(err, windows.ERROR_INVALID_PARAMETER) {
				break
			}
			return owner, group, result, fmt.Errorf("GetAce(%d): %w", idx, err)
		}
		if acePtr == nil {
			break
		}

		base := unsafe.Pointer(acePtr)
		aceType := *(*byte)(base)                     // offset 0
		aceFlags := *(*byte)(unsafe.Add(base, 1))     // offset 1
		accessMask := *(*uint32)(unsafe.Add(base, 4)) // offset 4

		// Only base ACEs; skip object ACEs
		if aceType != windows.ACCESS_ALLOWED_ACE_TYPE && aceType != windows.ACCESS_DENIED_ACE_TYPE {
			continue
		}

		// For base ACEs, SID starts at offset 8
		sidPtr := (*windows.SID)(unsafe.Add(base, 8))
		if sidPtr == nil || !sidPtr.IsValid() {
			continue
		}
		sidStr, e := sidToString(sidPtr)
		if e != nil {
			continue
		}

		result = append(result, types.WinACL{
			SID:        sidStr,
			AccessMask: accessMask,
			Type:       uint8(aceType),
			Flags:      uint8(aceFlags),
		})
	}

	return owner, group, result, nil
}

// sidToString converts a SID to S-1-... via ConvertSidToStringSid and frees the buffer.
func sidToString(pSid *windows.SID) (string, error) {
	var str *uint16
	if err := windows.ConvertSidToStringSid(pSid, &str); err != nil {
		return "", err
	}
	defer windows.LocalFree(windows.Handle(unsafe.Pointer(str)))
	return windows.UTF16PtrToString(str), nil
}
