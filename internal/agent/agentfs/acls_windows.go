//go:build windows

package agentfs

import (
	"fmt"
	"syscall"
	"unsafe"

	"golang.org/x/sys/windows"
)

var (
	modAdvapi32                    = syscall.NewLazyDLL("advapi32.dll")
	procGetFileSecurity            = modAdvapi32.NewProc("GetFileSecurityW")
	procGetSecurityDescriptorDACL  = modAdvapi32.NewProc("GetSecurityDescriptorDacl")
	procGetExplicitEntriesFromACL  = modAdvapi32.NewProc("GetExplicitEntriesFromAclW")
	procGetSecurityDescriptorOwner = modAdvapi32.NewProc("GetSecurityDescriptorOwner")
	procGetSecurityDescriptorGroup = modAdvapi32.NewProc("GetSecurityDescriptorGroup")
	procIsValidSecurityDescriptor  = modAdvapi32.NewProc("IsValidSecurityDescriptor")
)

// GetFileSecurityDescriptor returns a buffer with the file sec Descriptor
func GetFileSecurityDescriptor(path string, secInfo windows.SECURITY_INFORMATION) ([]uint16, error) {
	//Convert path
	pathPtr, err := syscall.UTF16PtrFromString(path)
	if err != nil {
		return nil, err
	}
	//Initialize size and call for the first time
	var bufferSize uint32 = 0
	r1, _, err := procGetFileSecurity.Call(
		uintptr(unsafe.Pointer(pathPtr)),
		uintptr(secInfo),
		uintptr(0),
		uintptr(bufferSize),
		uintptr(unsafe.Pointer(&bufferSize)),
	)

	if bufferSize == 0 {
		return nil, err
	}

	secDescriptor := make([]uint16, bufferSize)
	r1, _, err = procGetFileSecurity.Call(
		uintptr(unsafe.Pointer(pathPtr)),
		uintptr(secInfo),
		uintptr(unsafe.Pointer(&secDescriptor[0])),
		uintptr(bufferSize),
		uintptr(unsafe.Pointer(&bufferSize)),
	)
	if r1 == 0 {
		return nil, err
	}
	return secDescriptor, nil
}

// IsValidSecDescriptor returns true is the secDescriptor is valid
func IsValidSecDescriptor(secDescriptor []uint16) (bool, error) {
	r1, _, err := procIsValidSecurityDescriptor.Call(
		uintptr(unsafe.Pointer(&secDescriptor[0])),
	)
	if r1 == 0 {
		return false, err
	}
	return true, nil
}

// GetExplicitEntriesFromACL gets a list of explicit entries from an ACL
func GetExplicitEntriesFromACL(acl *windows.ACL) (*[]windows.EXPLICIT_ACCESS, error) {
	var explicitEntries *[]windows.EXPLICIT_ACCESS
	var explicitEntriesSize uint64
	// Get dacl
	r1, _, err := procGetExplicitEntriesFromACL.Call(
		uintptr(unsafe.Pointer(acl)),
		uintptr(unsafe.Pointer(&explicitEntriesSize)),
		uintptr(unsafe.Pointer(&explicitEntries)),
	)
	if r1 != 0 {
		return explicitEntries, err
	}
	return explicitEntries, nil
}

// GetSecurityDescriptorDACL gets an DACL from a security descriptor
func GetSecurityDescriptorDACL(pSecDescriptor []uint16) (*windows.ACL, bool, bool, error) {
	var present bool
	var acl *windows.ACL
	var defaulted bool
	// Get dacl
	r1, _, err := procGetSecurityDescriptorDACL.Call(
		uintptr(unsafe.Pointer(&pSecDescriptor[0])),
		uintptr(unsafe.Pointer(&present)),
		uintptr(unsafe.Pointer(&acl)),
		uintptr(unsafe.Pointer(&defaulted)),
	)
	if r1 == 0 && !present {
		return acl, false, false, err
	}
	return acl, present, defaulted, nil
}

// GetSecurityDescriptorOwner wraps the Windows API call to retrieve the owner SID.
func GetSecurityDescriptorOwner(secDesc []uint16) (*windows.SID, bool, error) {
	var owner *windows.SID
	var defaulted int32 // Nonzero indicates the SID came from the default owner.
	r1, _, err := procGetSecurityDescriptorOwner.Call(
		uintptr(unsafe.Pointer(&secDesc[0])),
		uintptr(unsafe.Pointer(&owner)),
		uintptr(unsafe.Pointer(&defaulted)),
	)
	if r1 == 0 {
		return nil, false, fmt.Errorf("GetSecurityDescriptorOwner failed: %w", err)
	}
	return owner, defaulted != 0, nil
}

// GetSecurityDescriptorGroup wraps the Windows API call to retrieve the group SID.
func GetSecurityDescriptorGroup(secDesc []uint16) (*windows.SID, bool, error) {
	var group *windows.SID
	var defaulted int32 // Nonzero indicates the SID came from the default group.
	r1, _, err := procGetSecurityDescriptorGroup.Call(
		uintptr(unsafe.Pointer(&secDesc[0])),
		uintptr(unsafe.Pointer(&group)),
		uintptr(unsafe.Pointer(&defaulted)),
	)
	if r1 == 0 {
		return nil, false, fmt.Errorf("GetSecurityDescriptorGroup failed: %w", err)
	}
	return group, defaulted != 0, nil
}
