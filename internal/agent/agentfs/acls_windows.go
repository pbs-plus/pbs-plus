//go:build windows

package agentfs

import (
	"fmt"
	"syscall"
	"unicode/utf16"
	"unsafe"

	"golang.org/x/sys/windows"
)

var modAdvapi32 = syscall.NewLazyDLL("advapi32.dll")
var modKernel32 = syscall.NewLazyDLL("kernel32.dll")

var (
	procGetFileSecurity            = modAdvapi32.NewProc("GetFileSecurityW")
	procIsValidSecurityDescriptor  = modAdvapi32.NewProc("IsValidSecurityDescriptor")
	procMakeAbsoluteSD             = modAdvapi32.NewProc("MakeAbsoluteSD")
	procGetSecurityDescriptorDACL  = modAdvapi32.NewProc("GetSecurityDescriptorDacl")
	procGetExplicitEntriesFromACL  = modAdvapi32.NewProc("GetExplicitEntriesFromAclW")
	procGetSecurityDescriptorOwner = modAdvapi32.NewProc("GetSecurityDescriptorOwner")
	procGetSecurityDescriptorGroup = modAdvapi32.NewProc("GetSecurityDescriptorGroup")
	procLocalFree                  = modKernel32.NewProc("LocalFree")
)

// GetFileSecurityDescriptor retrieves a file security descriptor
// (in self-relative format) using GetFileSecurityW.
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

// MakeAbsoluteSD converts a self-relative security descriptor into an absolute one.
// Returns the absolute SD buffer. Caller does not need to free anything.
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

// GetSecurityDescriptorDACL retrieves the DACL from the security descriptor.
// Takes an absolute security descriptor buffer.
func GetSecurityDescriptorDACL(pSecDescriptor []byte) (acl *windows.ACL, present bool, defaulted bool, err error) {
	if len(pSecDescriptor) == 0 {
		return nil, false, false, fmt.Errorf("security descriptor buffer is empty")
	}

	var bPresent int32
	var bDefaulted int32
	var pAcl *windows.ACL // Pointer to ACL within the SD structure

	ret, _, callErr := procGetSecurityDescriptorDACL.Call(
		uintptr(unsafe.Pointer(&pSecDescriptor[0])),
		uintptr(unsafe.Pointer(&bPresent)),
		uintptr(unsafe.Pointer(&pAcl)), // Receives pointer to ACL
		uintptr(unsafe.Pointer(&bDefaulted)),
	)

	if ret == 0 {
		// Function failed.
		return nil, false, false, fmt.Errorf("GetSecurityDescriptorDACL call failed: %w", callErr)
	}

	present = (bPresent != 0)
	defaulted = (bDefaulted != 0)

	// If present is false, pAcl is NULL.
	// If present is true, pAcl *might* be NULL (a NULL DACL means allow all access).
	// The returned pAcl points within the pSecDescriptor structure or associated buffers.
	acl = pAcl
	err = nil // API call succeeded.
	return
}

// GetExplicitEntriesFromACL retrieves the explicit access entries from an ACL.
// The returned slice refers to memory allocated by Windows; DO NOT MODIFY.
// The caller MUST call LocalFree on the returned pointer when done.
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

// FreeExplicitEntries frees the memory allocated by GetExplicitEntriesFromACL.
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

// getOwnerGroupAbsolute extracts the owner and group SIDs (as strings) from an absolute
// security descriptor buffer.
func getOwnerGroupAbsolute(absoluteSD []byte) (string, string, error) {
	if len(absoluteSD) == 0 {
		return "", "", fmt.Errorf("absolute security descriptor buffer is empty")
	}

	// The buffer itself should represent the SECURITY_DESCRIPTOR structure.
	sdPtr := uintptr(unsafe.Pointer(&absoluteSD[0]))

	var pOwnerRaw, pGroupRaw uintptr // Raw pointers to SID structures
	var ownerDefaulted, groupDefaulted int32

	// Get owner SID pointer.
	ret, _, callErr := procGetSecurityDescriptorOwner.Call(
		sdPtr,
		uintptr(unsafe.Pointer(&pOwnerRaw)),
		uintptr(unsafe.Pointer(&ownerDefaulted)),
	)
	if ret == 0 {
		return "", "", fmt.Errorf("GetSecurityDescriptorOwner failed: %w", callErr)
	}
	if pOwnerRaw == 0 {
		// This might happen if the SD has no owner, though unusual.
		return "", "", fmt.Errorf("GetSecurityDescriptorOwner succeeded but returned nil owner SID")
	}

	// Get group SID pointer.
	ret, _, callErr = procGetSecurityDescriptorGroup.Call(
		sdPtr,
		uintptr(unsafe.Pointer(&pGroupRaw)),
		uintptr(unsafe.Pointer(&groupDefaulted)),
	)
	if ret == 0 {
		return "", "", fmt.Errorf("GetSecurityDescriptorGroup failed: %w", callErr)
	}
	if pGroupRaw == 0 {
		// This might happen if the SD has no group.
		return "", "", fmt.Errorf("GetSecurityDescriptorGroup succeeded but returned nil group SID")
	}

	// Convert raw SID pointers to *windows.SID. These point within the absoluteSD structure's associated buffers.
	pOwner := (*windows.SID)(unsafe.Pointer(pOwnerRaw))
	pGroup := (*windows.SID)(unsafe.Pointer(pGroupRaw))

	// Convert SIDs to strings. Need to check validity before calling String().
	if !pOwner.IsValid() {
		return "", "", fmt.Errorf("owner SID is invalid")
	}
	ownerStr := pOwner.String()

	if !pGroup.IsValid() {
		return "", "", fmt.Errorf("group SID is invalid")
	}

	groupStr := pGroup.String()

	return ownerStr, groupStr, nil
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
