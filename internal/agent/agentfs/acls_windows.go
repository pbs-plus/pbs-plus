//go:build windows

package agentfs

import (
	"errors"
	"fmt"
	"reflect"
	"sync"
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

// aclHeader matches the Windows ACL structure layout for validation
type aclHeader struct {
	AclRevision byte
	Sbz1        byte
	AclSize     uint16
	AceCount    uint16
	Sbz2        uint16
}

// GetWinACLsHandle retrieves Owner, Group, and DACL ACEs.
func GetWinACLsHandle(h windows.Handle) (owner string, group string, acls []types.WinACL, err error) {
	// Validate handle before use
	if h == windows.InvalidHandle || h == 0 {
		return "", "", nil, fmt.Errorf("invalid handle")
	}

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

	// Validate owner SID
	if pOwnerSid == nil || !pOwnerSid.IsValid() {
		return "", "", nil, fmt.Errorf("owner SID from security descriptor is nil or invalid")
	}
	owner = pOwnerSid.String()

	// Validate group SID
	if pGroupSid == nil || !pGroupSid.IsValid() {
		return owner, "", nil, fmt.Errorf("group SID from security descriptor is nil or invalid")
	}
	group = pGroupSid.String()

	// DACL pointer
	pDacl, _, err := sd.DACL()
	if err != nil {
		if errors.Is(err, windows.ERROR_OBJECT_NOT_FOUND) {
			return owner, group, []types.WinACL{}, nil
		}
		return owner, group, nil, fmt.Errorf("SECURITY_DESCRIPTOR.DACL: %w", err)
	}
	if pDacl == nil {
		// NULL DACL => no explicit entries
		return owner, group, []types.WinACL{}, nil
	}

	// Validate ACL structure before passing to syscall
	if !isValidACL(pDacl) {
		return owner, group, []types.WinACL{}, fmt.Errorf("ACL structure validation failed")
	}

	// Get explicit entries from ACL
	entriesPtr, entriesCount, err := GetExplicitEntriesFromACL(pDacl)
	if err != nil {
		return owner, group, []types.WinACL{}, fmt.Errorf("GetExplicitEntriesFromACL: %w", err)
	}
	if entriesPtr == 0 || entriesCount == 0 {
		return owner, group, []types.WinACL{}, nil
	}
	defer FreeExplicitEntries(entriesPtr)

	entries, err := safeEntriesToSlice(entriesPtr, entriesCount)
	if err != nil {
		return owner, group, []types.WinACL{}, fmt.Errorf("failed to parse ACL entries: %w", err)
	}

	result := make([]types.WinACL, 0, len(entries))
	for i := range entries {
		e := &entries[i]

		// Validate trustee structure
		if e.Trustee.TrusteeForm != windows.TRUSTEE_IS_SID {
			continue
		}

		pSid := (*windows.SID)(unsafe.Pointer(e.Trustee.TrusteeValue))

		// Comprehensive SID validation
		if pSid == nil {
			continue
		}

		if !isSafePointer(unsafe.Pointer(pSid)) {
			continue
		}

		// Validate SID structure before calling IsValid
		if !isValidSIDPointer(pSid) {
			continue
		}

		if !pSid.IsValid() {
			continue
		}

		sidStr := pSid.String()
		if sidStr == "" {
			continue
		}

		result = append(result, types.WinACL{
			SID:        sidStr,
			AccessMask: uint32(e.AccessPermissions),
			Type:       uint8(e.AccessMode),
			Flags:      uint8(e.Inheritance),
		})
	}

	return owner, group, result, nil
}

func GetExplicitEntriesFromACL(acl *windows.ACL) (uintptr, uint32, error) {
	if acl == nil {
		return 0, 0, fmt.Errorf("input ACL cannot be nil")
	}

	var entriesCount uint32
	var explicitEntriesPtr uintptr

	ret, _, callErr := procGetExplicitEntriesFromACL.Call(
		uintptr(unsafe.Pointer(acl)),
		uintptr(unsafe.Pointer(&entriesCount)),
		uintptr(unsafe.Pointer(&explicitEntriesPtr)),
	)

	if ret != uintptr(windows.ERROR_SUCCESS) {
		if callErr != nil && callErr != windows.ERROR_SUCCESS {
			return 0, 0, fmt.Errorf("GetExplicitEntriesFromACL call failed: %w", callErr)
		}
		return 0, 0, fmt.Errorf("GetExplicitEntriesFromACL call failed with code: %d", ret)
	}

	if explicitEntriesPtr == 0 && entriesCount > 0 {
		return 0, 0, fmt.Errorf("GetExplicitEntriesFromACL returned success but null pointer for entries")
	}

	// Validate count is reasonable
	const maxReasonableACEs = 10000
	if entriesCount > maxReasonableACEs {
		if explicitEntriesPtr != 0 {
			procLocalFree.Call(explicitEntriesPtr)
		}
		return 0, 0, fmt.Errorf("unreasonable ACE count: %d (max: %d)", entriesCount, maxReasonableACEs)
	}

	// Validate pointer is not in freed pointers ring (catches some use-after-free)
	if entriesCount > 0 && freedPointers.contains(explicitEntriesPtr) {
		return 0, 0, fmt.Errorf("pointer already freed")
	}

	return explicitEntriesPtr, entriesCount, nil
}

type freedPointerRing struct {
	mu      sync.RWMutex
	ptrs    map[uintptr]struct{}
	order   []uintptr
	maxSize int
}

var freedPointers = &freedPointerRing{
	ptrs:    make(map[uintptr]struct{}, 1024),
	order:   make([]uintptr, 0, 1024),
	maxSize: 1024,
}

func (r *freedPointerRing) contains(ptr uintptr) bool {
	if ptr == 0 {
		return false
	}

	r.mu.RLock()
	defer r.mu.RUnlock()

	_, exists := r.ptrs[ptr]
	return exists
}

func (r *freedPointerRing) add(ptr uintptr) {
	if ptr == 0 {
		return
	}

	r.mu.Lock()
	defer r.mu.Unlock()

	// If already at capacity, remove oldest entry
	if len(r.order) >= r.maxSize {
		oldest := r.order[0]
		delete(r.ptrs, oldest)
		r.order = r.order[1:]
	}

	// Add new pointer if not already present
	if _, exists := r.ptrs[ptr]; !exists {
		r.ptrs[ptr] = struct{}{}
		r.order = append(r.order, ptr)
	}
}

func FreeExplicitEntries(explicitEntriesPtr uintptr) error {
	if explicitEntriesPtr == 0 {
		return nil
	}

	// Check for double-free
	if freedPointers.contains(explicitEntriesPtr) {
		return fmt.Errorf("detected double-free attempt for pointer 0x%X", explicitEntriesPtr)
	}

	ret, _, callErr := procLocalFree.Call(explicitEntriesPtr)
	if ret != 0 {
		if callErr != nil {
			return fmt.Errorf("LocalFree failed: %w", callErr)
		}
		return fmt.Errorf("LocalFree failed with code: %d", ret)
	}

	freedPointers.add(explicitEntriesPtr)
	return nil
}

// safeEntriesToSlice safely converts raw EXPLICIT_ACCESS pointer and count to a Go slice
// using proper bounds checking and validation
func safeEntriesToSlice(entriesPtr uintptr, count uint32) ([]windows.EXPLICIT_ACCESS, error) {
	if entriesPtr == 0 || count == 0 {
		return nil, nil
	}

	// Validate pointer is in user space
	if !isSafePointer(unsafe.Pointer(entriesPtr)) {
		return nil, fmt.Errorf("entries pointer is not in user space")
	}

	// Validate count to prevent massive allocations
	const maxReasonableACEs = 10000
	if count > maxReasonableACEs {
		return nil, fmt.Errorf("entry count exceeds reasonable limit: %d", count)
	}

	// Calculate required memory size and check for overflow
	entrySize := unsafe.Sizeof(windows.EXPLICIT_ACCESS{})
	if entrySize == 0 {
		return nil, fmt.Errorf("invalid EXPLICIT_ACCESS size")
	}

	totalSize := uintptr(count) * entrySize
	if totalSize/entrySize != uintptr(count) {
		return nil, fmt.Errorf("integer overflow in size calculation")
	}

	// Use reflect.SliceHeader for safer slice construction
	var slice []windows.EXPLICIT_ACCESS
	header := (*reflect.SliceHeader)(unsafe.Pointer(&slice))
	header.Data = entriesPtr
	header.Len = int(count)
	header.Cap = int(count)

	// Validate we can safely access the memory by checking first and last entries
	if len(slice) > 0 {
		// Touch first entry to verify readable
		_ = slice[0].AccessPermissions

		// Touch last entry to verify readable
		if len(slice) > 1 {
			_ = slice[len(slice)-1].AccessPermissions
		}
	}

	return slice, nil
}

// isValidACL performs basic validation on an ACL structure
func isValidACL(acl *windows.ACL) bool {
	if acl == nil {
		return false
	}

	// Validate pointer is in user space
	if !isSafePointer(unsafe.Pointer(acl)) {
		return false
	}

	// Cast to our header structure to access the fields
	header := (*aclHeader)(unsafe.Pointer(acl))

	// Basic sanity checks on ACL structure
	const minACLSize = 8     // sizeof(ACL) structure header
	const maxACLSize = 65535 // Maximum reasonable ACL size

	aclSize := header.AclSize
	if aclSize < minACLSize || aclSize > maxACLSize {
		return false
	}

	// AceCount should be reasonable
	const maxReasonableACEs = 10000
	if header.AceCount > maxReasonableACEs {
		return false
	}

	// ACL revision should be valid (ACL_REVISION = 2 or ACL_REVISION_DS = 4)
	if header.AclRevision != 2 && header.AclRevision != 4 {
		return false
	}

	return true
}

// isValidSIDPointer validates a SID pointer before dereferencing
func isValidSIDPointer(sid *windows.SID) bool {
	if sid == nil {
		return false
	}

	// Validate pointer is in user space
	if !isSafePointer(unsafe.Pointer(sid)) {
		return false
	}

	// A SID has a minimum size (8 bytes: revision + subauth count + authority)
	// We can't fully validate without accessing memory, but we've done basic checks

	return true
}

// isSafePointer performs basic validation that a pointer is in user-space
func isSafePointer(ptr unsafe.Pointer) bool {
	if ptr == nil {
		return false
	}

	addr := uintptr(ptr)

	// Validate pointer is not NULL or near-NULL (catch null pointer arithmetic)
	// Use a smaller threshold that won't reject valid low addresses
	const minValidAddress = 0x1000 // 4KB - first page
	if addr < minValidAddress {
		return false
	}

	// On 64-bit Windows, kernel space starts at 0xFFFF800000000000
	// On 32-bit Windows, kernel space starts at 0x80000000
	const (
		kernel64Start = 0xFFFF800000000000
		kernel32Start = 0x80000000
	)

	// Detect architecture and apply appropriate check
	if unsafe.Sizeof(uintptr(0)) == 8 {
		// 64-bit: user space is below kernel64Start
		// Note: Maximum user-mode address is actually around 0x00007FFFFFFFFFFF
		// but we check against kernel start which is simpler and safe
		return addr < kernel64Start
	} else {
		// 32-bit: user space is below kernel32Start
		return addr < kernel32Start
	}
}
