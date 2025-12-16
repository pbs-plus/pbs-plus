//go:build windows

package agentfs

import (
	"os"
	"path/filepath"
	"testing"
	"unsafe"

	"golang.org/x/sys/windows"
)

// TestGetWinACLsHandle_InvalidHandles tries to crash with invalid handles
func TestGetWinACLsHandle_InvalidHandles(t *testing.T) {
	tests := []struct {
		name   string
		handle windows.Handle
	}{
		{"Zero handle", windows.Handle(0)},
		{"InvalidHandle constant", windows.InvalidHandle},
		{"Arbitrary large handle", windows.Handle(0xDEADBEEF)},
		{"Max uint handle", windows.Handle(^uintptr(0))},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Should not crash, should return error
			owner, group, acls, err := GetWinACLsHandle(tt.handle)
			if err == nil {
				t.Errorf("Expected error for invalid handle, got success: owner=%s, group=%s, acls=%v", owner, group, acls)
			}
		})
	}
}

// TestGetWinACLsHandle_ClosedHandle tries to use a closed handle
func TestGetWinACLsHandle_ClosedHandle(t *testing.T) {
	tmpDir := t.TempDir()
	tmpFile := filepath.Join(tmpDir, "test.txt")

	if err := os.WriteFile(tmpFile, []byte("test"), 0644); err != nil {
		t.Fatalf("Failed to create test file: %v", err)
	}

	// Open and immediately close a handle
	pathPtr, err := windows.UTF16PtrFromString(tmpFile)
	if err != nil {
		t.Fatalf("UTF16PtrFromString failed: %v", err)
	}

	handle, err := windows.CreateFile(
		pathPtr,
		windows.GENERIC_READ,
		windows.FILE_SHARE_READ,
		nil,
		windows.OPEN_EXISTING,
		windows.FILE_ATTRIBUTE_NORMAL,
		0,
	)
	if err != nil {
		t.Fatalf("CreateFile failed: %v", err)
	}

	// Close the handle
	windows.CloseHandle(handle)

	// Try to use the closed handle - should not crash
	_, _, _, err = GetWinACLsHandle(handle)
	if err == nil {
		t.Error("Expected error when using closed handle")
	}
}

// TestGetExplicitEntriesFromACL_NilACL tests nil ACL handling
func TestGetExplicitEntriesFromACL_NilACL(t *testing.T) {
	ptr, count, err := GetExplicitEntriesFromACL(nil)
	if err == nil {
		t.Error("Expected error for nil ACL")
	}
	if ptr != 0 || count != 0 {
		t.Errorf("Expected zero values for nil ACL, got ptr=%v, count=%v", ptr, count)
	}
}

// TestGetExplicitEntriesFromACL_CorruptedACL tries to crash with corrupted ACL
func TestGetExplicitEntriesFromACL_CorruptedACL(t *testing.T) {
	// Create a fake corrupted ACL structure
	type fakeACL struct {
		revision byte
		sbz1     byte
		size     uint16
		aceCount uint16
		sbz2     uint16
	}

	tests := []struct {
		name string
		acl  fakeACL
	}{
		{"Huge size", fakeACL{revision: 2, size: 65535, aceCount: 0}},
		{"Huge ace count", fakeACL{revision: 2, size: 100, aceCount: 50000}},
		{"Zero size", fakeACL{revision: 2, size: 0, aceCount: 0}},
		{"Invalid revision", fakeACL{revision: 99, size: 100, aceCount: 1}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Cast our fake ACL to windows.ACL pointer
			aclPtr := (*windows.ACL)(unsafe.Pointer(&tt.acl))

			// Should not crash due to validation
			if !isValidACL(aclPtr) {
				t.Log("ACL correctly identified as invalid")
				return
			}

			// If it passes validation, try the syscall (should fail gracefully)
			ptr, count, err := GetExplicitEntriesFromACL(aclPtr)
			if ptr != 0 {
				FreeExplicitEntries(ptr) // Clean up if somehow succeeded
			}
			t.Logf("Result: ptr=%v, count=%v, err=%v", ptr, count, err)
		})
	}
}

// TestUnsafeEntriesToSlice_KernelPointers tests kernel space pointer detection
func TestUnsafeEntriesToSlice_KernelPointers(t *testing.T) {
	tests := []struct {
		name string
		ptr  uintptr
	}{
		{"Kernel64 start", 0xFFFF800000000000},
		{"High kernel address", 0xFFFFFFFFFFFFFFF0},
		{"Kernel32 start", 0x80000000},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Should return nil for kernel pointers
			slice := unsafeEntriesToSlice(tt.ptr, 10)
			if slice != nil {
				t.Error("Expected nil for kernel space pointer")
			}
		})
	}
}

// TestUnsafeEntriesToSlice_InvalidPointers tests various invalid pointers
func TestUnsafeEntriesToSlice_InvalidPointers(t *testing.T) {
	tests := []struct {
		name  string
		ptr   uintptr
		count uint32
	}{
		{"Null pointer", 0, 10},
		{"Zero count", 0x1000, 0},
		{"Null and zero", 0, 0},
		{"Low memory", 0x1000, 100},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Should not crash
			slice := unsafeEntriesToSlice(tt.ptr, tt.count)
			t.Logf("Result: %v (len=%d)", slice != nil, len(slice))
		})
	}
}

// TestFreeExplicitEntries_DoubleFreePrevention tests double free scenarios
func TestFreeExplicitEntries_DoubleFreePrevention(t *testing.T) {
	// Allocate some memory via LocalAlloc
	size := uintptr(100)
	ptr, _, _ := modKernel32.NewProc("LocalAlloc").Call(0, size)

	if ptr == 0 {
		t.Skip("LocalAlloc failed, skipping double free test")
	}

	// First free should succeed
	err := FreeExplicitEntries(ptr)
	if err != nil {
		t.Logf("First free error (might be expected): %v", err)
	}

	// Second free should handle gracefully (might error, but shouldn't crash)
	err = FreeExplicitEntries(ptr)
	t.Logf("Second free error: %v", err)
}

// TestFreeExplicitEntries_InvalidPointers tests freeing invalid pointers
func TestFreeExplicitEntries_InvalidPointers(t *testing.T) {
	tests := []struct {
		name string
		ptr  uintptr
	}{
		{"Zero pointer", 0},
		{"Arbitrary pointer", 0xDEADBEEF},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Should not crash
			err := FreeExplicitEntries(tt.ptr)
			t.Logf("Free result: %v", err)
		})
	}
}

// TestGetWinACLsHandle_RealFile tests with a real file to ensure normal operation
func TestGetWinACLsHandle_RealFile(t *testing.T) {
	tmpDir := t.TempDir()
	tmpFile := filepath.Join(tmpDir, "test.txt")

	if err := os.WriteFile(tmpFile, []byte("test"), 0644); err != nil {
		t.Fatalf("Failed to create test file: %v", err)
	}

	pathPtr, err := windows.UTF16PtrFromString(tmpFile)
	if err != nil {
		t.Fatalf("UTF16PtrFromString failed: %v", err)
	}

	handle, err := windows.CreateFile(
		pathPtr,
		windows.GENERIC_READ|windows.READ_CONTROL,
		windows.FILE_SHARE_READ,
		nil,
		windows.OPEN_EXISTING,
		windows.FILE_ATTRIBUTE_NORMAL,
		0,
	)
	if err != nil {
		t.Fatalf("CreateFile failed: %v", err)
	}
	defer windows.CloseHandle(handle)

	owner, group, acls, err := GetWinACLsHandle(handle)
	if err != nil {
		t.Fatalf("GetWinACLsHandle failed: %v", err)
	}

	if owner == "" {
		t.Error("Expected non-empty owner")
	}
	if group == "" {
		t.Error("Expected non-empty group")
	}

	t.Logf("Owner: %s, Group: %s, ACLs: %d", owner, group, len(acls))
}

// TestIsSafePointer_BoundaryConditions tests pointer validation edge cases
func TestIsSafePointer_BoundaryConditions(t *testing.T) {
	tests := []struct {
		name     string
		ptr      unsafe.Pointer
		expected bool
	}{
		{"Nil pointer", nil, false},
		{"Low address", unsafe.Pointer(uintptr(0x1000)), true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := isSafePointer(tt.ptr)
			if result != tt.expected {
				t.Errorf("isSafePointer(%v) = %v, want %v", tt.ptr, result, tt.expected)
			}
		})
	}
}

// TestIsValidACL_EdgeCases tests ACL validation edge cases
func TestIsValidACL_EdgeCases(t *testing.T) {
	tests := []struct {
		name     string
		acl      *windows.ACL
		expected bool
	}{
		{"Nil ACL", nil, false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := isValidACL(tt.acl)
			if result != tt.expected {
				t.Errorf("isValidACL() = %v, want %v", result, tt.expected)
			}
		})
	}
}

// BenchmarkGetWinACLsHandle benchmarks normal operation
func BenchmarkGetWinACLsHandle(b *testing.B) {
	tmpDir := b.TempDir()
	tmpFile := filepath.Join(tmpDir, "bench.txt")

	if err := os.WriteFile(tmpFile, []byte("bench"), 0644); err != nil {
		b.Fatalf("Failed to create test file: %v", err)
	}

	pathPtr, _ := windows.UTF16PtrFromString(tmpFile)
	handle, err := windows.CreateFile(
		pathPtr,
		windows.GENERIC_READ|windows.READ_CONTROL,
		windows.FILE_SHARE_READ,
		nil,
		windows.OPEN_EXISTING,
		windows.FILE_ATTRIBUTE_NORMAL,
		0,
	)
	if err != nil {
		b.Fatalf("CreateFile failed: %v", err)
	}
	defer windows.CloseHandle(handle)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _, _, _ = GetWinACLsHandle(handle)
	}
}
