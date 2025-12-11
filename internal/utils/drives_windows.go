//go:build windows

package utils

import (
	"errors"
	"fmt"
	"runtime"
	"strings"
	"unsafe"

	"golang.org/x/sys/windows"
)

const _IOCTL_STORAGE_QUERY_PROPERTY = uint32(0x2D1400)

type _STORAGE_PROPERTY_ID int32

const (
	_StorageDevicePropertyPROPERTY_ID = 0 // From winioctl.h
	// ... other values exist but are not needed here
)

// STORAGE_QUERY_TYPE enum
type _STORAGE_QUERY_TYPE int32

const (
	_PropertyStandardQuerySTORAGE_QUERY_TYPE = 0 // From winioctl.h
)

// STORAGE_BUS_TYPE enum (subset relevant here)
// Defined in ntddstor.h
type _STORAGE_BUS_TYPE uint32

const (
	_BusTypeUnknown _STORAGE_BUS_TYPE = 0x00
	_BusTypeScsi    _STORAGE_BUS_TYPE = 0x01
	_BusTypeAta     _STORAGE_BUS_TYPE = 0x03
	_BusTypeUsb     _STORAGE_BUS_TYPE = 0x07
	_BusTypeRAID    _STORAGE_BUS_TYPE = 0x08
	_BusTypeSas     _STORAGE_BUS_TYPE = 0x0A
	_BusTypeSata    _STORAGE_BUS_TYPE = 0x0B
	_BusTypeSd      _STORAGE_BUS_TYPE = 0x0C
	_BusTypeMmc     _STORAGE_BUS_TYPE = 0x0D
	_BusTypeNvme    _STORAGE_BUS_TYPE = 0x11
)

// STORAGE_PROPERTY_QUERY structure (matches C struct layout)
// Defined in winioctl.h
type _STORAGE_PROPERTY_QUERY struct {
	PropertyId           _STORAGE_PROPERTY_ID
	QueryType            _STORAGE_QUERY_TYPE
	AdditionalParameters [1]byte // Can be larger if needed for specific queries
}

// STORAGE_DEVICE_DESCRIPTOR structure (matches C struct layout)
// Defined in winioctl.h
type _STORAGE_DEVICE_DESCRIPTOR struct {
	Version               uint32
	Size                  uint32
	DeviceType            byte
	DeviceTypeModifier    byte
	RemovableMedia        byte // Use boolean conversion: (RemovableMedia != 0)
	CommandQueueing       byte // Use boolean conversion: (CommandQueueing != 0)
	VendorIdOffset        uint32
	ProductIdOffset       uint32
	ProductRevisionOffset uint32
	SerialNumberOffset    uint32
	BusType               _STORAGE_BUS_TYPE
	RawPropertiesLength   uint32
	RawDeviceProperties   [1]byte // Placeholder for variable length data
}

// DriveInfo contains detailed information about a drive

// getDriveTypeString returns a human-readable string describing the type of drive
func getDriveTypeString(dt uint32) string {
	switch dt {
	case windows.DRIVE_UNKNOWN:
		return "Unknown"
	case windows.DRIVE_NO_ROOT_DIR:
		return "No Root Directory"
	case windows.DRIVE_REMOVABLE:
		return "Removable"
	case windows.DRIVE_FIXED:
		return "Fixed"
	case windows.DRIVE_REMOTE:
		return "Network"
	case windows.DRIVE_CDROM:
		return "CD-ROM"
	case windows.DRIVE_RAMDISK:
		return "RAM Disk"
	default:
		return "Unknown"
	}
}

// humanizeBytes converts a byte count into a human-readable string with appropriate units (KB, MB, GB, TB)
func humanizeBytes(bytes uint64) string {
	const unit = 1000
	if bytes < unit {
		return fmt.Sprintf("%d B", bytes)
	}
	div, exp := unit, 0
	for n := bytes / unit; n >= unit; n /= unit {
		div *= unit
		exp++
	}
	var unitSymbol string
	switch exp {
	case 0:
		unitSymbol = "KB"
	case 1:
		unitSymbol = "MB"
	case 2:
		unitSymbol = "GB"
	case 3:
		unitSymbol = "TB"
	case 4:
		unitSymbol = "PB"
	default:
		unitSymbol = "??"
	}
	return fmt.Sprintf("%.2f %s", float64(bytes)/float64(div), unitSymbol)
}

// getBusType queries the storage device bus type for a given drive letter.
// Uses manually defined constants and structs.
func getBusType(driveLetter string) (_STORAGE_BUS_TYPE, error) {
	// Need volume path like \\.\C:
	volumePath := fmt.Sprintf("\\\\.\\%s:", driveLetter)
	volumePathUtf16, err := windows.UTF16PtrFromString(volumePath)
	if err != nil {
		return _BusTypeUnknown, fmt.Errorf("failed to create UTF16 pointer for %s: %w", volumePath, err)
	}

	// Get a handle to the volume
	handle, err := windows.CreateFile(
		volumePathUtf16,
		0, // No access needed, just querying properties
		windows.FILE_SHARE_READ|windows.FILE_SHARE_WRITE,
		nil,
		windows.OPEN_EXISTING,
		windows.FILE_ATTRIBUTE_NORMAL, // Use FILE_ATTRIBUTE_NORMAL
		0,
	)
	if err != nil {
		// Common error if drive is not ready (e.g., empty card reader)
		// Or if insufficient permissions (might need admin rights)
		if errno, ok := err.(windows.Errno); ok {
			if errno == windows.ERROR_NOT_READY || errno == windows.ERROR_ACCESS_DENIED || errno == windows.ERROR_SHARING_VIOLATION {
				return _BusTypeUnknown, fmt.Errorf("drive %s not ready, access denied, or sharing violation: %w", driveLetter, err)
			}
		}
		return _BusTypeUnknown, fmt.Errorf("CreateFile failed for %s: %w", volumePath, err)
	}
	defer windows.CloseHandle(handle)

	// Prepare the query
	var propertyQuery _STORAGE_PROPERTY_QUERY
	propertyQuery.PropertyId = _StorageDeviceProperty // Query device properties
	propertyQuery.QueryType = _PropertyStandardQuery  // Standard query

	// Allocate buffer for the result (_STORAGE_DEVICE_DESCRIPTOR)
	// Size needs to be sufficient for _STORAGE_DEVICE_DESCRIPTOR header + potentially vendor/product IDs
	// Using a larger buffer is safer. 1024 should be plenty.
	bufferSize := uint32(1024)
	outBuffer := make([]byte, bufferSize)
	var bytesReturned uint32

	// Perform the query using DeviceIoControl
	err = windows.DeviceIoControl(
		handle,
		_IOCTL_STORAGE_QUERY_PROPERTY, // Use the manually defined IOCTL code
		(*byte)(unsafe.Pointer(&propertyQuery)),
		uint32(unsafe.Sizeof(propertyQuery)),
		&outBuffer[0],
		bufferSize,
		&bytesReturned,
		nil,
	)

	if err != nil {
		// ERROR_INVALID_FUNCTION might occur on some virtual drives or non-standard devices
		if errno, ok := err.(windows.Errno); ok && errno == windows.ERROR_INVALID_FUNCTION {
			return _BusTypeUnknown, fmt.Errorf("DeviceIoControl returned ERROR_INVALID_FUNCTION for %s (unsupported device?): %w", volumePath, err)
		}
		return _BusTypeUnknown, fmt.Errorf("DeviceIoControl failed for %s: %w", volumePath, err)
	}

	// Basic check on returned size
	if bytesReturned < uint32(unsafe.Sizeof(_STORAGE_DEVICE_DESCRIPTOR{})) {
		return _BusTypeUnknown, fmt.Errorf("DeviceIoControl returned insufficient data (%d bytes) for %s", bytesReturned, volumePath)
	}

	// Cast the output buffer to the descriptor structure
	// We only need the header part containing the BusType
	descriptor := (*_STORAGE_DEVICE_DESCRIPTOR)(unsafe.Pointer(&outBuffer[0]))

	// Check if the returned size matches the struct size field
	// (This helps validate we got the expected structure)
	if descriptor.Size == 0 || bytesReturned < descriptor.Size {
		// descriptor.Size might be larger than our initial struct if extra raw data is present
		// but it shouldn't be smaller than the base struct size or zero.
		// fmt.Printf("Warning: Descriptor size mismatch for %s (Expected >= %d, Got %d, Returned %d)\n",
		// 	volumePath, unsafe.Sizeof(_STORAGE_DEVICE_DESCRIPTOR{}), descriptor.Size, bytesReturned)
		// Continue anyway, as BusType is early in the struct.
	}

	return descriptor.BusType, nil
}

// GetLocalDrives returns a slice of DriveInfo containing detailed information about each local drive
func GetLocalDrives() ([]DriveInfo, error) {
	var drives []DriveInfo

	for _, drive := range "ABCDEFGHIJKLMNOPQRSTUVWXYZ" {
		driveLetter := string(drive)
		path := fmt.Sprintf("%s:\\", driveLetter)
		pathUtf16, err := windows.UTF16PtrFromString(path)
		if err != nil {
			continue
		}

		driveType := windows.GetDriveType(pathUtf16)
		if driveType == windows.DRIVE_NO_ROOT_DIR || driveType == windows.DRIVE_UNKNOWN {
			continue
		}

		var (
			volumeNameStr string
			fileSystemStr string
			totalBytes    uint64
			freeBytes     uint64
		)

		// Get Volume Info
		var volumeName [windows.MAX_PATH + 1]uint16
		var fileSystemName [windows.MAX_PATH + 1]uint16
		err = windows.GetVolumeInformation(
			pathUtf16, &volumeName[0], uint32(len(volumeName)),
			nil, nil, nil, &fileSystemName[0], uint32(len(fileSystemName)),
		)
		if err == nil {
			volumeNameStr = windows.UTF16ToString(volumeName[:])
			fileSystemStr = windows.UTF16ToString(fileSystemName[:])
		} else if errno, ok := err.(windows.Errno); ok && (errno == windows.ERROR_NOT_READY || errno == windows.ERROR_GEN_FAILURE || errno == windows.ERROR_ACCESS_DENIED) {
			// Ignore common errors
		} // else { log unexpected errors }

		// Get Disk Space
		var totalFreeBytes uint64
		err = windows.GetDiskFreeSpaceEx(pathUtf16, nil, &totalBytes, &totalFreeBytes)
		if err == nil {
			freeBytes = totalFreeBytes
		} else if errno, ok := err.(windows.Errno); ok && (errno == windows.ERROR_NOT_READY || errno == windows.ERROR_GEN_FAILURE || errno == windows.ERROR_ACCESS_DENIED) {
			// Ignore common errors
		} // else { log unexpected errors }

		usedBytes := uint64(0)
		if totalBytes > freeBytes {
			usedBytes = totalBytes - freeBytes
		}

		totalHuman := humanizeBytes(totalBytes)
		usedHuman := humanizeBytes(usedBytes)
		freeHuman := humanizeBytes(freeBytes)

		// --- Drive Type Classification Logic ---
		driveTypeStr := getDriveTypeString(driveType) // Get base type string

		if driveType == windows.DRIVE_FIXED {
			busType, busErr := getBusType(driveLetter)

			if busErr != nil {
				// Assume "Virtual" for most errors on DRIVE_FIXED,
				// except for specific physical media errors.
				var errno windows.Errno
				isErrno := errors.As(busErr, &errno) // Use errors.As for robust check

				if isErrno && (errno == windows.ERROR_NOT_READY || errno == windows.ERROR_SHARING_VIOLATION) {
					// These errors are less likely for virtual drives, keep as Error
					driveTypeStr = "Fixed (Error)"
					// Optional: Log the specific error for debugging
					// fmt.Printf("Debug: Keeping 'Fixed (Error)' for %s: %v\n", driveLetter, busErr)
				} else {
					// Treat ERROR_INVALID_FUNCTION, ACCESS_DENIED, GEN_FAILURE,
					// and other unexpected errors as likely "Virtual".
					driveTypeStr = "Virtual"
					// Optional: Log the specific error for debugging
					// fmt.Printf("Debug: Classifying as 'Virtual' for %s due to error: %v\n", driveLetter, busErr)
				}
			} else {
				// Bus type query succeeded, refine classification (same as before)
				switch busType {
				case _BusTypeUsb:
					driveTypeStr = "External (USB)"
				case _BusTypeSata, _BusTypeNvme, _BusTypeSas, _BusTypeAta, _BusTypeScsi, _BusTypeRAID:
					driveTypeStr = "Internal (Fixed)"
				case _BusTypeUnknown:
					driveTypeStr = "Virtual" // Heuristic
				default:
					driveTypeStr = "Virtual" // Heuristic
				}
			}
		} else if driveType == windows.DRIVE_REMOVABLE {
			// Refine removable types (same as before)
			busType, busErr := getBusType(driveLetter)
			if busErr == nil {
				switch busType {
				case _BusTypeUsb:
					driveTypeStr = "Removable (USB)"
				case _BusTypeSd:
					driveTypeStr = "Removable (SD)"
				case _BusTypeMmc:
					driveTypeStr = "Removable (MMC)"
				}
			} // Ignore errors for removable
		}
		// --- End Classification Logic ---

		volumeNameStr = strings.TrimRight(volumeNameStr, "\x00")
		fileSystemStr = strings.TrimRight(fileSystemStr, "\x00")

		drives = append(drives, DriveInfo{
			Letter:          driveLetter,
			Type:            driveTypeStr,
			VolumeName:      volumeNameStr,
			FileSystem:      fileSystemStr,
			TotalBytes:      totalBytes,
			UsedBytes:       usedBytes,
			FreeBytes:       freeBytes,
			Total:           totalHuman,
			Used:            usedHuman,
			Free:            freeHuman,
			OperatingSystem: runtime.GOOS,
		})
	}

	return drives, nil
}
