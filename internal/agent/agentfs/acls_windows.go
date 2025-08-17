//go:build windows

package agentfs

import (
	"errors"
	"fmt"
	"syscall"
	"unsafe"

	"github.com/pbs-plus/pbs-plus/internal/agent/agentfs/types"
	"golang.org/x/sys/windows"
)

var modAdvapi32 = syscall.NewLazyDLL("advapi32.dll")
var modKernel32 = syscall.NewLazyDLL("kernel32.dll")

var (
	procGetExplicitEntriesFromACL = modAdvapi32.NewProc("GetExplicitEntriesFromAclW")
	procLocalFree                 = modKernel32.NewProc("LocalFree")
)

// GetWinACLsHandle retrieves Owner, Group, and DACL ACEs.
func GetWinACLsHandle(h windows.Handle) (owner string, group string, acls []types.WinACL, err error) {
	const si = windows.OWNER_SECURITY_INFORMATION |
		windows.GROUP_SECURITY_INFORMATION |
		windows.DACL_SECURITY_INFORMATION

	// Get SD from the handle
	sd, err := windows.GetSecurityInfo(h, windows.SE_FILE_OBJECT, si)
	if err != nil {
		return "", "", nil, fmt.Errorf("GetSecurityInfo: %w", err)
	}

	pOwnerSid, _, _ := sd.Owner()
	pGroupSid, _, _ := sd.Owner()

	// Use the SIDs returned by MakeAbsoluteSD directly if they are valid.
	if pOwnerSid != nil && pOwnerSid.IsValid() {
		owner = pOwnerSid.String()
	} else {
		// Fallback or handle error if owner SID is expected but missing/invalid
		// For simplicity here, we proceed, but production code might error out.
		// Alternatively, call getOwnerGroupAbsolute as a fallback, but it might be redundant.
		// owner, group, err = getOwnerGroupAbsolute(absoluteSD)
		// if err != nil {
		// 	 return "", "", nil, fmt.Errorf("failed to extract owner/group: %w", err)
		// }
		return "", "", nil, fmt.Errorf("owner SID from MakeAbsoluteSD is nil or invalid")
	}

	if pGroupSid != nil && pGroupSid.IsValid() {
		group = pGroupSid.String()
	} else {
		return owner, "", nil, fmt.Errorf("group SID from MakeAbsoluteSD is nil or invalid")
	}

	// DACL pointer
	pDacl, _, err := sd.DACL()
	if err != nil {
		if errors.Is(err, windows.ERROR_OBJECT_NOT_FOUND) {
			return owner, group, []types.WinACL{}, nil
		}
		return owner, group, nil, fmt.Errorf("SECURITY_DESCRIPTOR.DACL: %w", err)
	}
	if pDacl == nil {
		// NULL DACL => no explicit entries (old code returned empty slice)
		return owner, group, []types.WinACL{}, nil
	}

	// IMPORTANT: Use the same canonicalization path as the old code
	entriesPtr, entriesCount, err := GetExplicitEntriesFromACL(pDacl)
	if err != nil {
		// Keep behavior consistent: on failure, return empty ACLs with error
		return owner, group, []types.WinACL{}, fmt.Errorf("GetExplicitEntriesFromACL: %w", err)
	}
	if entriesPtr == 0 || entriesCount == 0 {
		return owner, group, []types.WinACL{}, nil
	}
	defer FreeExplicitEntries(entriesPtr)

	entries := unsafeEntriesToSlice(entriesPtr, entriesCount)

	result := make([]types.WinACL, 0, entriesCount)
	for _, e := range entries {
		pSid := (*windows.SID)(unsafe.Pointer(e.Trustee.TrusteeValue))
		if pSid == nil || !pSid.IsValid() {
			continue
		}

		sidStr := pSid.String()

		result = append(result, types.WinACL{
			SID:        sidStr,
			AccessMask: uint32(e.AccessPermissions),
			Type:       uint8(e.AccessMode),  // matches path-based mapping
			Flags:      uint8(e.Inheritance), // matches path-based mapping
		})
	}

	return owner, group, result, nil
}

func GetExplicitEntriesFromACL(acl *windows.ACL) (uintptr, uint32, error) {
	if acl == nil {
		// An ACL pointer is required. A nil ACL might represent "no DACL" or "NULL DACL".
		// GetExplicitEntriesFromAcl requires a valid ACL pointer.
		return 0, 0, fmt.Errorf("input ACL cannot be nil")
	}

	var entriesCount uint32
	var explicitEntriesPtr uintptr // Pointer to the array allocated by the API

	ret, _, callErr := procGetExplicitEntriesFromACL.Call(
		uintptr(unsafe.Pointer(acl)),
		uintptr(unsafe.Pointer(&entriesCount)),
		uintptr(unsafe.Pointer(&explicitEntriesPtr)), // Receives pointer to allocated array
	)

	// According to docs, returns ERROR_SUCCESS on success.
	if ret != uintptr(windows.ERROR_SUCCESS) {
		// Check if callErr provides more info, otherwise use the return value.
		if callErr != nil && callErr != windows.ERROR_SUCCESS {
			return 0, 0, fmt.Errorf("GetExplicitEntriesFromACL call failed: %w", callErr)
		}
		// If callErr is success but ret isn't, use ret as the error code.
		return 0, 0, fmt.Errorf("GetExplicitEntriesFromACL call failed with code: %d", ret)
	}

	if explicitEntriesPtr == 0 && entriesCount > 0 {
		// This shouldn't happen if the call succeeded.
		return 0, 0, fmt.Errorf("GetExplicitEntriesFromACL returned success but null pointer for entries")
	}

	// Return the raw pointer and count. Caller is responsible for LocalFree(explicitEntriesPtr).
	return explicitEntriesPtr, entriesCount, nil
}

func FreeExplicitEntries(explicitEntriesPtr uintptr) error {
	if explicitEntriesPtr == 0 {
		return nil // Nothing to free
	}
	ret, _, callErr := procLocalFree.Call(explicitEntriesPtr)
	// LocalFree returns NULL on success. If it returns non-NULL, it's the handle itself, indicating failure.
	if ret != 0 {
		return fmt.Errorf("LocalFree failed: %w", callErr)
	}
	return nil
}

// Helper function to convert raw EXPLICIT_ACCESS pointer and count to a Go slice.
// This is unsafe because the underlying memory is managed by Windows and freed via LocalFree.
// Use only for temporary access immediately after GetExplicitEntriesFromACL and before FreeExplicitEntries.
func unsafeEntriesToSlice(entriesPtr uintptr, count uint32) []windows.EXPLICIT_ACCESS {
	if entriesPtr == 0 || count == 0 {
		return nil
	}
	// Create a slice header pointing to the Windows-allocated memory.
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
