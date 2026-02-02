//go:build windows

package agentfs

import (
	"os"
	"path/filepath"
	"strings"
	"syscall"
	"time"

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

	if fileAttributes&windows.FILE_ATTRIBUTE_READONLY != 0 {
		m |= 0444
	} else {
		m |= 0666
	}

	if fileAttributes&windows.FILE_ATTRIBUTE_DIRECTORY != 0 {
		m |= os.ModeDir | 0111
	}

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

func unixNanoFromWin(wt int64) int64 {
	return filetimeToTime(windows.Filetime{
		LowDateTime:  uint32(uint64(wt) & 0xFFFFFFFF),
		HighDateTime: uint32(uint64(wt) >> 32),
	}).UnixNano()
}

func unixFromWin(wt int64) int64 {
	return filetimeToUnix(windows.Filetime{
		LowDateTime:  uint32(uint64(wt) & 0xFFFFFFFF),
		HighDateTime: uint32(uint64(wt) >> 32),
	})
}

func parseFileAttributes(attr uint32) map[string]bool {
	attributes := make(map[string]bool)
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
	return time.Unix(0, ft.Nanoseconds())
}

func filetimeToUnix(ft windows.Filetime) int64 {
	const (
		winToUnixEpochDiff = 116444736000000000
		hundredNano        = 10000000
	)
	t := (int64(ft.HighDateTime) << 32) | int64(ft.LowDateTime)
	return (t - winToUnixEpochDiff) / hundredNano
}

func filetimeSyscallToUnix(ft syscall.Filetime) int64 {
	const (
		winToUnixEpochDiff = 116444736000000000
		hundredNano        = 10000000
	)
	t := (int64(ft.HighDateTime) << 32) | int64(ft.LowDateTime)
	return (t - winToUnixEpochDiff) / hundredNano
}

func toExtendedLengthPath(path string) string {
	if strings.HasPrefix(path, `\\?\`) || strings.HasPrefix(path, `\??\`) {
		return path
	}

	absPath, err := filepath.Abs(path)
	if err != nil {
		absPath = path
	}

	if len(absPath) >= 2 && absPath[1] == ':' {
		return `\\?\` + absPath
	}

	if strings.HasPrefix(absPath, `\\`) {
		return `\\?\UNC\` + absPath[2:]
	}

	return `\\?\` + absPath
}

func boolToInt(b bool) uint32 {
	if b {
		return 1
	}
	return 0
}
