//go:build windows

package agentfs

import (
	"os"
	"time"

	"github.com/pbs-plus/pbs-plus/internal/syslog"
	"golang.org/x/sys/windows"
)

const (
	FILE_ATTRIBUTE_UNPINNED = 0x00100000
	FILE_ATTRIBUTE_PINNED   = 0x00080000
)

const (
	excludedAttrs = windows.FILE_ATTRIBUTE_REPARSE_POINT |
		windows.FILE_ATTRIBUTE_DEVICE |
		windows.FILE_ATTRIBUTE_OFFLINE |
		windows.FILE_ATTRIBUTE_SYSTEM |
		windows.FILE_ATTRIBUTE_VIRTUAL |
		windows.FILE_ATTRIBUTE_RECALL_ON_OPEN |
		windows.FILE_ATTRIBUTE_RECALL_ON_DATA_ACCESS |
		windows.FILE_ATTRIBUTE_ENCRYPTED |
		FILE_ATTRIBUTE_UNPINNED | FILE_ATTRIBUTE_PINNED
)

func windowsFileModeFromHandle(h windows.Handle, fileAttributes uint32) uint32 {
	var m os.FileMode

	// Base permissions from READONLY
	if fileAttributes&windows.FILE_ATTRIBUTE_READONLY != 0 {
		m |= 0444
	} else {
		m |= 0666
	}

	// Directory bit (no reparse-point exclusion needed here since caller excludes them)
	if fileAttributes&windows.FILE_ATTRIBUTE_DIRECTORY != 0 {
		m |= os.ModeDir | 0111
	}

	// Special file types based on GetFileType(handle)
	if h != 0 {
		if ft, err := windows.GetFileType(h); err == nil {
			switch ft {
			case windows.FILE_TYPE_PIPE:
				m |= os.ModeNamedPipe
			case windows.FILE_TYPE_CHAR:
				m |= os.ModeDevice | os.ModeCharDevice
			}
		}
	}

	return uint32(m)
}

// windowsAttributesToFileMode converts Windows file attributes to Go's os.FileMode
func windowsAttributesToFileMode(attrs uint32) uint32 {
	var mode os.FileMode = 0

	if attrs&windows.FILE_ATTRIBUTE_DIRECTORY != 0 {
		mode |= os.ModeDir
	}

	if attrs&windows.FILE_ATTRIBUTE_REPARSE_POINT != 0 {
		mode |= os.ModeSymlink
	}

	if attrs&windows.FILE_ATTRIBUTE_DEVICE != 0 {
		mode |= os.ModeDevice
	}

	if mode == 0 {
		mode |= 0644 // Default permission for files
	} else if mode&os.ModeDir != 0 {
		mode |= 0755 // Default permission for directories
	}

	return uint32(mode)
}

func mapWinError(err error, helper string) error {
	switch err {
	case windows.ERROR_FILE_NOT_FOUND:
		return os.ErrNotExist
	case windows.ERROR_PATH_NOT_FOUND:
		return os.ErrNotExist
	case windows.ERROR_ACCESS_DENIED:
		return os.ErrPermission
	default:
		syslog.L.Error(err).WithMessage("unknown windows error").WithField("helper", helper).Write()
		return &os.PathError{
			Op:   "access",
			Path: "",
			Err:  err,
		}
	}
}

// parseFileAttributes converts Windows file attribute flags into a map.
func parseFileAttributes(attr uint32) map[string]bool {
	attributes := make(map[string]bool)
	// Attributes are defined in golang.org/x/sys/windows.
	if attr&windows.FILE_ATTRIBUTE_READONLY != 0 {
		attributes["readOnly"] = true
	}
	if attr&windows.FILE_ATTRIBUTE_HIDDEN != 0 {
		attributes["hidden"] = true
	}
	if attr&windows.FILE_ATTRIBUTE_SYSTEM != 0 {
		attributes["system"] = true
	}
	if attr&windows.FILE_ATTRIBUTE_DIRECTORY != 0 {
		attributes["directory"] = true
	}
	if attr&windows.FILE_ATTRIBUTE_ARCHIVE != 0 {
		attributes["archive"] = true
	}
	if attr&windows.FILE_ATTRIBUTE_NORMAL != 0 {
		attributes["normal"] = true
	}
	if attr&windows.FILE_ATTRIBUTE_TEMPORARY != 0 {
		attributes["temporary"] = true
	}
	if attr&windows.FILE_ATTRIBUTE_SPARSE_FILE != 0 {
		attributes["sparseFile"] = true
	}
	if attr&windows.FILE_ATTRIBUTE_REPARSE_POINT != 0 {
		attributes["reparsePoint"] = true
	}
	if attr&windows.FILE_ATTRIBUTE_COMPRESSED != 0 {
		attributes["compressed"] = true
	}
	if attr&windows.FILE_ATTRIBUTE_OFFLINE != 0 {
		attributes["offline"] = true
	}
	if attr&windows.FILE_ATTRIBUTE_NOT_CONTENT_INDEXED != 0 {
		attributes["notContentIndexed"] = true
	}
	if attr&windows.FILE_ATTRIBUTE_ENCRYPTED != 0 {
		attributes["encrypted"] = true
	}
	return attributes
}

func filetimeToTime(ft windows.Filetime) time.Time {
	// windows.NsecToFiletime is inverse; use Unix epoch conversion
	return time.Unix(0, ft.Nanoseconds())
}

// filetimeToUnix converts a Windows FILETIME to a Unix timestamp.
// Windows file times are in 100-nanosecond intervals since January 1, 1601.
func filetimeToUnix(ft windows.Filetime) int64 {
	const (
		winToUnixEpochDiff = 116444736000000000 // in 100-nanosecond units
		hundredNano        = 10000000           // 100-ns units per second
	)
	t := (int64(ft.HighDateTime) << 32) | int64(ft.LowDateTime)
	return (t - winToUnixEpochDiff) / hundredNano
}

// Prefer NT path prefix to avoid reparsing and keep long paths working.
func convertToNTPath(path string) string {
	if len(path) >= 4 && path[:4] == "\\??\\" {
		return path
	}
	if len(path) >= 2 && path[1] == ':' {
		return "\\??\\" + path
	}
	return "\\??\\" + path
}

func boolToInt(b bool) uint32 {
	if b {
		return 1
	}
	return 0
}
