//go:build windows

package agentfs

import (
	"fmt"
	"syscall"
	"unsafe"

	"github.com/pbs-plus/pbs-plus/internal/agent/agentfs/types"
	"golang.org/x/sys/windows"
)

var (
	modAdvapi32                    = syscall.NewLazyDLL("advapi32.dll")
	modKernel32                    = syscall.NewLazyDLL("kernel32.dll")
	procGetExplicitEntriesFromACLW = modAdvapi32.NewProc("GetExplicitEntriesFromAclW")
	procLocalFree                  = modKernel32.NewProc("LocalFree")
)

func GetWinACLsHandle(h windows.Handle) (owner string, group string, acls []types.WinACL, err error) {
	const si = windows.OWNER_SECURITY_INFORMATION |
		windows.GROUP_SECURITY_INFORMATION |
		windows.DACL_SECURITY_INFORMATION

	sd, err := windows.GetSecurityInfo(h, windows.SE_FILE_OBJECT, si)
	if err != nil {
		return "", "", nil, fmt.Errorf("GetSecurityInfo: %w", err)
	}

	pOwnerSid, _, _ := sd.Owner()
	pGroupSid, _, _ := sd.Group()

	if pOwnerSid != nil && pOwnerSid.IsValid() {
		owner = pOwnerSid.String()
	} else {
		return "", "", nil, fmt.Errorf("owner SID is nil or invalid")
	}

	if pGroupSid != nil && pGroupSid.IsValid() {
		group = pGroupSid.String()
	} else {
		return owner, "", nil, fmt.Errorf("group SID is nil or invalid")
	}

	pDacl, _, err := sd.DACL()
	if err != nil {
		if err == windows.ERROR_OBJECT_NOT_FOUND {
			return owner, group, []types.WinACL{}, nil
		}
		return owner, group, nil, fmt.Errorf("SECURITY_DESCRIPTOR.DACL: %w", err)
	}
	if pDacl == nil {
		return owner, group, []types.WinACL{}, nil
	}

	entriesPtr, entriesCount, err := GetExplicitEntriesFromACL(pDacl)
	if err != nil {
		return owner, group, []types.WinACL{}, fmt.Errorf("GetExplicitEntriesFromACL: %w", err)
	}
	if entriesPtr == 0 || entriesCount == 0 {
		return owner, group, []types.WinACL{}, nil
	}
	defer FreeExplicitEntries(entriesPtr)

	entries := unsafeEntriesToSlice(entriesPtr, entriesCount)

	result := make([]types.WinACL, 0, entriesCount)
	for _, e := range entries {
		switch e.Trustee.TrusteeForm {
		case windows.TRUSTEE_IS_SID:
			pSid := (*windows.SID)(unsafe.Pointer(e.Trustee.TrusteeValue))
			if pSid == nil || !pSid.IsValid() {
				continue
			}
			result = append(result, types.WinACL{
				SID:        pSid.String(),
				AccessMask: uint32(e.AccessPermissions),
				Type:       uint8(e.AccessMode),
				Flags:      uint8(e.Inheritance),
			})
		default:
			continue
		}
	}

	return owner, group, result, nil
}

func GetExplicitEntriesFromACL(acl *windows.ACL) (uintptr, uint32, error) {
	if acl == nil {
		return 0, 0, fmt.Errorf("input ACL cannot be nil")
	}

	var entriesCount uint32
	var explicitEntriesPtr uintptr

	r0, _, _ := procGetExplicitEntriesFromACLW.Call(
		uintptr(unsafe.Pointer(acl)),
		uintptr(unsafe.Pointer(&entriesCount)),
		uintptr(unsafe.Pointer(&explicitEntriesPtr)),
	)
	// The function returns a Win32 error code. ERROR_SUCCESS (0) means success.
	if windows.Errno(r0) != windows.ERROR_SUCCESS {
		return 0, 0, syscallErr("GetExplicitEntriesFromAclW", windows.Errno(r0))
	}

	if explicitEntriesPtr == 0 && entriesCount > 0 {
		return 0, 0, fmt.Errorf("GetExplicitEntriesFromAclW succeeded but returned null pointer for entries")
	}

	return explicitEntriesPtr, entriesCount, nil
}

func FreeExplicitEntries(explicitEntriesPtr uintptr) error {
	if explicitEntriesPtr == 0 {
		return nil
	}
	r0, _, _ := procLocalFree.Call(explicitEntriesPtr)
	// LocalFree returns NULL on success.
	if r0 != 0 {
		return syscallErr("LocalFree", windows.Errno(r0))
	}
	return nil
}

func unsafeEntriesToSlice(entriesPtr uintptr, count uint32) []windows.EXPLICIT_ACCESS {
	if entriesPtr == 0 || count == 0 {
		return nil
	}
	var slice []windows.EXPLICIT_ACCESS
	hdr := (*struct {
		data unsafe.Pointer
		len  int
		cap  int
	})(unsafe.Pointer(&slice))
	hdr.data = unsafe.Pointer(entriesPtr)
	hdr.len = int(count)
	hdr.cap = int(count)
	return slice
}

func syscallErr(api string, code windows.Errno) error {
	if code == 0 {
		return fmt.Errorf("%s failed", api)
	}
	return fmt.Errorf("%s failed: %s (0x%x)", api, code.Error(), uint32(code))
}
