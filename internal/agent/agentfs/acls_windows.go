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
	procMakeAbsoluteSD             = modAdvapi32.NewProc("MakeAbsoluteSD")
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

func MakeAbsoluteSD(selfRelative []uint16) ([]uint16, error) {
	var AsecDesSize uint32
	var AdaclSize uint32
	var AsaclSize uint32
	var AownerSize uint32
	var AprimaryGroupSize uint32
	// Get sizes
	r1, _, err := procMakeAbsoluteSD.Call(
		uintptr(unsafe.Pointer(&selfRelative[0])),
		uintptr(0),
		uintptr(unsafe.Pointer(&AsecDesSize)),
		uintptr(0),
		uintptr(unsafe.Pointer(&AdaclSize)),
		uintptr(0),
		uintptr(unsafe.Pointer(&AsaclSize)),
		uintptr(0),
		uintptr(unsafe.Pointer(&AownerSize)),
		uintptr(0),
		uintptr(unsafe.Pointer(&AprimaryGroupSize)),
	)
	// Check buffer sanity
	if AsecDesSize == 0 {
		return nil, err
	}
	// Make buffers
	AsecDes := make([]uint16, AsecDesSize)
	Adacl := make([]uint16, AdaclSize)
	Asacl := make([]uint16, AsaclSize)
	Aowner := make([]uint16, AownerSize)
	AprimaryGroup := make([]uint16, AprimaryGroupSize)

	// Make Pointers
	var AsecDesPtr *uint16
	var AdaclPtr *uint16
	var AsaclPtr *uint16
	var AownerPtr *uint16
	var AprimaryGroupPtr *uint16

	if AsecDesSize != 0 {
		AsecDesPtr = &AsecDes[0]
	}
	if AdaclSize != 0 {
		AdaclPtr = &Adacl[0]
	}
	if AsaclSize != 0 {
		AsaclPtr = &Asacl[0]
	}
	if AownerSize != 0 {
		AownerPtr = &Aowner[0]
	}
	if AprimaryGroupSize != 0 {
		AprimaryGroupPtr = &AprimaryGroup[0]
	}
	// Final call
	r1, _, err = procMakeAbsoluteSD.Call(
		uintptr(unsafe.Pointer(&selfRelative[0])),
		uintptr(unsafe.Pointer(AsecDesPtr)),
		uintptr(unsafe.Pointer(&AsecDesSize)),
		uintptr(unsafe.Pointer(AdaclPtr)),
		uintptr(unsafe.Pointer(&AdaclSize)),
		uintptr(unsafe.Pointer(AsaclPtr)),
		uintptr(unsafe.Pointer(&AsaclSize)),
		uintptr(unsafe.Pointer(AownerPtr)),
		uintptr(unsafe.Pointer(&AownerSize)),
		uintptr(unsafe.Pointer(AprimaryGroupPtr)),
		uintptr(unsafe.Pointer(&AprimaryGroupSize)),
	)
	if r1 == 0 {
		return nil, err
	}
	return AsecDes, nil
}
