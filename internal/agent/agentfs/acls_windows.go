//go:build windows

package agentfs

import (
	"fmt"
	"syscall"
	"unsafe"

	"golang.org/x/sys/windows"
)

var modAdvapi32 = syscall.NewLazyDLL("advapi32.dll")

var (
	procGetFileSecurity            = modAdvapi32.NewProc("GetFileSecurityW")
	procIsValidSecurityDescriptor  = modAdvapi32.NewProc("IsValidSecurityDescriptor")
	procMakeAbsoluteSD             = modAdvapi32.NewProc("MakeAbsoluteSD")
	procGetSecurityDescriptorDACL  = modAdvapi32.NewProc("GetSecurityDescriptorDacl")
	procGetExplicitEntriesFromACL  = modAdvapi32.NewProc("GetExplicitEntriesFromAclW")
	procGetSecurityDescriptorOwner = modAdvapi32.NewProc("GetSecurityDescriptorOwner")
	procGetSecurityDescriptorGroup = modAdvapi32.NewProc("GetSecurityDescriptorGroup")
)

// GetFileSecurityDescriptor retrieves a file security descriptor
// (in self-relative format) using GetFileSecurityW.
func GetFileSecurityDescriptor(filePath string, secInfo uint32) ([]uint16, error) {
	pathPtr, err := syscall.UTF16PtrFromString(filePath)
	if err != nil {
		return nil, fmt.Errorf("UTF16 conversion error: %v", err)
	}

	var bufSize uint32 = 0
	// First call to obtain buffer size.
	ret, _, err := procGetFileSecurity.Call(
		uintptr(unsafe.Pointer(pathPtr)),
		uintptr(secInfo),
		0,
		0,
		uintptr(unsafe.Pointer(&bufSize)),
	)
	if bufSize == 0 {
		return nil, fmt.Errorf("GetFileSecurityW reported 0 buffer size: %v", err)
	}

	secDesc := make([]uint16, bufSize)
	ret, _, err = procGetFileSecurity.Call(
		uintptr(unsafe.Pointer(pathPtr)),
		uintptr(secInfo),
		uintptr(unsafe.Pointer(&secDesc[0])),
		uintptr(bufSize),
		uintptr(unsafe.Pointer(&bufSize)),
	)
	if ret == 0 {
		return nil, fmt.Errorf("GetFileSecurityW failed: %v", err)
	}
	return secDesc, nil
}

// IsValidSecDescriptor verifies that secDesc is a valid security descriptor.
func IsValidSecDescriptor(secDesc []uint16) (bool, error) {
	ret, _, err := procIsValidSecurityDescriptor.Call(uintptr(unsafe.Pointer(&secDesc[0])))
	if ret == 0 {
		return false, err
	}
	return true, nil
}

// MakeAbsoluteSD converts a self-relative security descriptor into an absolute one.
func MakeAbsoluteSD(selfRelative []uint16) ([]uint16, error) {
	var AsecDesSize, AdaclSize, AsaclSize, AownerSize, AprimaryGroupSize uint32

	// Call once to obtain sizes.
	ret, _, err := procMakeAbsoluteSD.Call(
		uintptr(unsafe.Pointer(&selfRelative[0])),
		0,
		uintptr(unsafe.Pointer(&AsecDesSize)),
		0,
		uintptr(unsafe.Pointer(&AdaclSize)),
		0,
		uintptr(unsafe.Pointer(&AsaclSize)),
		0,
		uintptr(unsafe.Pointer(&AownerSize)),
		0,
		uintptr(unsafe.Pointer(&AprimaryGroupSize)),
	)
	if AsecDesSize == 0 {
		return nil, fmt.Errorf("MakeAbsoluteSD: invalid AsecDesSize: %v", err)
	}

	AsecDes := make([]uint16, AsecDesSize)
	Adacl := make([]uint16, AdaclSize)
	Asacl := make([]uint16, AsaclSize)
	Aowner := make([]uint16, AownerSize)
	AprimaryGroup := make([]uint16, AprimaryGroupSize)

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

	ret, _, err = procMakeAbsoluteSD.Call(
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
	if ret == 0 {
		return nil, fmt.Errorf("MakeAbsoluteSD call failed: %v", err)
	}
	return AsecDes, nil
}

// GetSecurityDescriptorDACL retrieves the DACL from the security descriptor.
func GetSecurityDescriptorDACL(pSecDescriptor []uint16) (*windows.ACL, bool, bool, error) {
	var present int32
	var acl *windows.ACL
	var defaulted int32
	ret, _, err := procGetSecurityDescriptorDACL.Call(
		uintptr(unsafe.Pointer(&pSecDescriptor[0])),
		uintptr(unsafe.Pointer(&present)),
		uintptr(unsafe.Pointer(&acl)),
		uintptr(unsafe.Pointer(&defaulted)),
	)
	// If ret is zero and no DACL is present, return an error.
	if ret == 0 && present == 0 {
		return acl, false, false, fmt.Errorf("GetSecurityDescriptorDACL call failed: %v", err)
	}
	return acl, (present != 0), (defaulted != 0), nil
}

// GetExplicitEntriesFromACL retrieves the explicit access entries from an ACL.
func GetExplicitEntriesFromACL(acl *windows.ACL) (*[]windows.EXPLICIT_ACCESS, error) {
	var explicitEntries *[]windows.EXPLICIT_ACCESS
	var entriesSize uint32
	ret, _, err := procGetExplicitEntriesFromACL.Call(
		uintptr(unsafe.Pointer(acl)),
		uintptr(unsafe.Pointer(&entriesSize)),
		uintptr(unsafe.Pointer(&explicitEntries)),
	)
	// If ret is non-zero, assume success.
	if ret != 0 {
		return explicitEntries, nil
	}
	return explicitEntries, err
}

// getOwnerGroupAbsolute extracts the owner and group SIDs (as strings) from an absolute
// security descriptor.
func getOwnerGroupAbsolute(absoluteSD []uint16) (string, string, error) {
	// Cast the buffer to a SECURITY_DESCRIPTOR.
	sd := (*windows.SECURITY_DESCRIPTOR)(unsafe.Pointer(&absoluteSD[0]))

	// Get owner SID.
	var pOwner *windows.SID
	var ownerDefaulted int32
	ret, _, err := procGetSecurityDescriptorOwner.Call(
		uintptr(unsafe.Pointer(sd)),
		uintptr(unsafe.Pointer(&pOwner)),
		uintptr(unsafe.Pointer(&ownerDefaulted)),
	)
	if ret == 0 {
		return "", "", fmt.Errorf("GetSecurityDescriptorOwner failed: %v", err)
	}
	ownerStr := pOwner.String()
	if ownerStr == "" {
		return "", "", fmt.Errorf("owner SID conversion failed: %v", err)
	}

	// Get group SID.
	var pGroup *windows.SID
	var groupDefaulted int32
	ret, _, err = procGetSecurityDescriptorGroup.Call(
		uintptr(unsafe.Pointer(sd)),
		uintptr(unsafe.Pointer(&pGroup)),
		uintptr(unsafe.Pointer(&groupDefaulted)),
	)
	if ret == 0 {
		return "", "", fmt.Errorf("GetSecurityDescriptorGroup failed: %v", err)
	}
	groupStr := pGroup.String()
	if groupStr == "" {
		return "", "", fmt.Errorf("group SID conversion failed: %v", err)
	}
	return ownerStr, groupStr, nil
}
