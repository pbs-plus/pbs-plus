//go:build windows

package agentfs

import (
	"errors"
	"fmt"
	"syscall"
	"unicode/utf16"
	"unsafe"

	"github.com/pbs-plus/pbs-plus/internal/agent/agentfs/types"
	"golang.org/x/sys/windows"
)

var modAdvapi32 = syscall.NewLazyDLL("advapi32.dll")
var modKernel32 = syscall.NewLazyDLL("kernel32.dll")

var (
	procGetExplicitEntriesFromACL = modAdvapi32.NewProc("GetExplicitEntriesFromAclW")
	procLocalFree                 = modKernel32.NewProc("LocalFree")
	procMakeAbsoluteSD            = modAdvapi32.NewProc("MakeAbsoluteSD")
	procGetFileSecurity           = modAdvapi32.NewProc("GetFileSecurityW")
	procIsValidSecurityDescriptor = modAdvapi32.NewProc("IsValidSecurityDescriptor")
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
	pGroupSid, _, _ := sd.Group()

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

func GetWinACLsByPath(filePath string) (string, string, []types.WinACL, error) {
	secInfo := windows.OWNER_SECURITY_INFORMATION |
		windows.GROUP_SECURITY_INFORMATION |
		windows.DACL_SECURITY_INFORMATION

	secDesc, err := GetFileSecurityDescriptor(filePath, uint32(secInfo))
	if err != nil {
		return "", "", nil, fmt.Errorf("GetFileSecurityDescriptor failed: %w", err)
	}

	// IsValidSecDescriptor is already called within GetFileSecurityDescriptor in the revised version.
	// If GetFileSecurityDescriptor succeeded, we assume it's valid for now.

	_, pDacl, _, pOwnerSid, pGroupSid, err := MakeAbsoluteSD(secDesc)
	if err != nil {
		// MakeAbsoluteSD might fail even if GetFileSecurityDescriptor succeeded.
		return "", "", nil, fmt.Errorf("MakeAbsoluteSD failed: %w", err)
	}

	// Use the SIDs returned by MakeAbsoluteSD directly if they are valid.
	var owner, group string
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

	// Check if a DACL is present (pDacl will be non-nil if MakeAbsoluteSD found one)
	if pDacl == nil {
		// No DACL present means no explicit ACL entries.
		return owner, group, []types.WinACL{}, nil
	}

	entriesPtr, entriesCount, err := GetExplicitEntriesFromACL(pDacl)
	if err != nil {
		// If GetExplicitEntriesFromACL fails, it might mean an empty but valid ACL,
		// or a real error. Treat failure as potentially no entries, but log/wrap error.
		return owner, group, []types.WinACL{}, fmt.Errorf("GetExplicitEntriesFromACL failed: %w", err)
	}
	if entriesPtr == 0 || entriesCount == 0 {
		// No entries found or pointer is null.
		return owner, group, []types.WinACL{}, nil
	}
	// Ensure the allocated memory is freed when the function returns.
	defer FreeExplicitEntries(entriesPtr)

	// Create a temporary slice header to access the Windows-allocated memory.
	// This is unsafe and the slice is only valid until FreeExplicitEntries is called.
	expEntries := unsafeEntriesToSlice(entriesPtr, entriesCount)

	winAcls := make([]types.WinACL, 0, entriesCount)
	for _, entry := range expEntries {
		// Trustee.TrusteeValue should point to a SID structure in this context.
		pSid := (*windows.SID)(unsafe.Pointer(entry.Trustee.TrusteeValue))
		if pSid == nil { // Check if the cast resulted in a nil pointer
			continue
		}

		if !pSid.IsValid() {
			// Log or handle invalid SID? Skipping for now.
			continue
		}

		sidStr := pSid.String()

		ace := types.WinACL{
			SID:        sidStr,
			AccessMask: uint32(entry.AccessPermissions),
			Type:       uint8(entry.AccessMode),
			Flags:      uint8(entry.Inheritance),
		}
		winAcls = append(winAcls, ace)
	}

	return owner, group, winAcls, nil
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

func MakeAbsoluteSD(selfRelative []byte) ([]byte, *windows.ACL, *windows.ACL, *windows.SID, *windows.SID, error) {
	if len(selfRelative) == 0 {
		return nil, nil, nil, nil, nil, fmt.Errorf("self-relative security descriptor buffer is empty")
	}

	var absSDSize, daclSize, saclSize, ownerSize, primaryGroupSize uint32

	// Call once to obtain sizes.
	ret, _, callErr := procMakeAbsoluteSD.Call(
		uintptr(unsafe.Pointer(&selfRelative[0])),
		0, uintptr(unsafe.Pointer(&absSDSize)),
		0, uintptr(unsafe.Pointer(&daclSize)),
		0, uintptr(unsafe.Pointer(&saclSize)),
		0, uintptr(unsafe.Pointer(&ownerSize)),
		0, uintptr(unsafe.Pointer(&primaryGroupSize)),
	)

	// Expect failure with ERROR_INSUFFICIENT_BUFFER
	if ret != 0 {
		return nil, nil, nil, nil, nil, fmt.Errorf("MakeAbsoluteSD succeeded unexpectedly on size query call")
	}
	if callErr != windows.ERROR_INSUFFICIENT_BUFFER {
		return nil, nil, nil, nil, nil, fmt.Errorf("MakeAbsoluteSD failed getting buffer sizes: %w", callErr)
	}
	if absSDSize == 0 {
		// This indicates a potential issue with the input SD or the call itself.
		return nil, nil, nil, nil, nil, fmt.Errorf("MakeAbsoluteSD reported 0 required size for absolute SD: %w", callErr)
	}

	// Allocate buffers. Note: These buffers will contain the actual data (ACLs, SIDs).
	// The absolute SD buffer will contain pointers *into* these other buffers.
	absSDBuf := make([]byte, absSDSize)
	daclBuf := make([]byte, daclSize)                 // May be size 0
	saclBuf := make([]byte, saclSize)                 // May be size 0
	ownerBuf := make([]byte, ownerSize)               // May be size 0
	primaryGroupBuf := make([]byte, primaryGroupSize) // May be size 0

	// Get pointers for the API call. Pass 0 (null) if size is 0.
	var absSDPtr, daclPtr, saclPtr, ownerPtr, primaryGroupPtr uintptr
	if absSDSize > 0 {
		absSDPtr = uintptr(unsafe.Pointer(&absSDBuf[0]))
	}
	if daclSize > 0 {
		daclPtr = uintptr(unsafe.Pointer(&daclBuf[0]))
	}
	if saclSize > 0 {
		saclPtr = uintptr(unsafe.Pointer(&saclBuf[0]))
	}
	if ownerSize > 0 {
		ownerPtr = uintptr(unsafe.Pointer(&ownerBuf[0]))
	}
	if primaryGroupSize > 0 {
		primaryGroupPtr = uintptr(unsafe.Pointer(&primaryGroupBuf[0]))
	}

	ret, _, callErr = procMakeAbsoluteSD.Call(
		uintptr(unsafe.Pointer(&selfRelative[0])),
		absSDPtr, uintptr(unsafe.Pointer(&absSDSize)),
		daclPtr, uintptr(unsafe.Pointer(&daclSize)),
		saclPtr, uintptr(unsafe.Pointer(&saclSize)),
		ownerPtr, uintptr(unsafe.Pointer(&ownerSize)),
		primaryGroupPtr, uintptr(unsafe.Pointer(&primaryGroupSize)),
	)
	if ret == 0 {
		return nil, nil, nil, nil, nil, fmt.Errorf("MakeAbsoluteSD call failed: %w", callErr)
	}

	// The pointers inside the absolute SD now point to the data in the other buffers.
	// We need to return pointers to the start of those buffers for the caller to use.
	var pDacl *windows.ACL
	var pSacl *windows.ACL
	var pOwner *windows.SID
	var pPrimaryGroup *windows.SID

	if daclSize > 0 {
		pDacl = (*windows.ACL)(unsafe.Pointer(&daclBuf[0]))
	}
	if saclSize > 0 {
		pSacl = (*windows.ACL)(unsafe.Pointer(&saclBuf[0]))
	}
	if ownerSize > 0 {
		pOwner = (*windows.SID)(unsafe.Pointer(&ownerBuf[0]))
	}
	if primaryGroupSize > 0 {
		pPrimaryGroup = (*windows.SID)(unsafe.Pointer(&primaryGroupBuf[0]))
	}

	return absSDBuf, pDacl, pSacl, pOwner, pPrimaryGroup, nil
}

func GetFileSecurityDescriptor(filePath string, secInfo uint32) ([]byte, error) {
	if filePath == "" {
		return nil, fmt.Errorf("filePath cannot be empty")
	}

	pathUtf16 := utf16.Encode([]rune(filePath))
	if len(pathUtf16) == 0 || pathUtf16[len(pathUtf16)-1] != 0 {
		pathUtf16 = append(pathUtf16, 0)
	}

	var bufSize uint32 = 0
	// First call to obtain buffer size.
	ret, _, callErr := procGetFileSecurity.Call(
		uintptr(unsafe.Pointer(&pathUtf16[0])),
		uintptr(secInfo),
		0,
		0,
		uintptr(unsafe.Pointer(&bufSize)),
	)
	// GetFileSecurityW returns 0 on failure. Error code ERROR_INSUFFICIENT_BUFFER is expected here.
	if ret != 0 {
		// This case should ideally not happen if bufSize is 0 initially, but check anyway.
		return nil, fmt.Errorf("GetFileSecurityW succeeded unexpectedly on first call")
	}
	if callErr != windows.ERROR_INSUFFICIENT_BUFFER {
		return nil, fmt.Errorf("GetFileSecurityW failed getting buffer size: %w", callErr)
	}
	if bufSize == 0 {
		// It's possible a file has no security descriptor or access is denied in a way that returns 0 size.
		// Check the error again. If it was INSUFFICIENT_BUFFER, a 0 size is strange.
		return nil, fmt.Errorf("GetFileSecurityW reported 0 buffer size but returned: %w", callErr)
	}

	secDescBuf := make([]byte, bufSize)
	if len(secDescBuf) == 0 {
		// Should not happen if bufSize > 0, but defensive check.
		return nil, fmt.Errorf("failed to allocate buffer for security descriptor")
	}

	ret, _, callErr = procGetFileSecurity.Call(
		uintptr(unsafe.Pointer(&pathUtf16[0])),
		uintptr(secInfo),
		uintptr(unsafe.Pointer(&secDescBuf[0])),
		uintptr(bufSize),
		uintptr(unsafe.Pointer(&bufSize)),
	)
	if ret == 0 {
		return nil, fmt.Errorf("GetFileSecurityW failed: %w", callErr)
	}

	// Optional: Validate the returned descriptor immediately.
	isValid, err := IsValidSecDescriptor(secDescBuf)
	if !isValid {
		// err might be nil if IsValidSecurityDescriptor simply returned false.
		if err != nil {
			return nil, fmt.Errorf("retrieved security descriptor is invalid: %w", err)
		}
		return nil, fmt.Errorf("retrieved security descriptor is invalid")
	}

	return secDescBuf, nil
}

// IsValidSecDescriptor verifies that secDesc is a valid security descriptor.
func IsValidSecDescriptor(secDesc []byte) (bool, error) {
	if len(secDesc) == 0 {
		return false, fmt.Errorf("security descriptor buffer is empty")
	}
	ret, _, callErr := procIsValidSecurityDescriptor.Call(uintptr(unsafe.Pointer(&secDesc[0])))
	if ret == 0 {
		// According to docs, it returns FALSE on failure. GetLastError provides more info.
		// If callErr is ERROR_SUCCESS, it means the descriptor is structurally invalid.
		if callErr == windows.ERROR_SUCCESS {
			return false, nil // Descriptor is invalid, but API call itself didn't fail.
		}
		return false, fmt.Errorf("IsValidSecurityDescriptor call failed: %w", callErr)
	}
	return true, nil
}
