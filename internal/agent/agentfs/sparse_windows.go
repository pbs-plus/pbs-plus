//go:build windows

package agentfs

import (
	"syscall"

	"golang.org/x/sys/windows"
)

func sparseSeek(handle windows.Handle, offset int64, whence int, fileSize int64) (int64, error) {
	var newOffset int64

	// Check if offset is beyond EOF
	if offset >= fileSize {
		return 0, syscall.ENXIO
	}

	// Query allocated ranges
	ranges, err := queryAllocatedRanges(handle, fileSize)
	if err != nil {
		return 0, err
	}

	if whence == SeekData {
		// Find the next data region
		found := false
		for _, r := range ranges {
			if offset >= r.FileOffset && offset < r.FileOffset+r.Length {
				// Already in data
				newOffset = offset
				found = true
				break
			}
			if offset < r.FileOffset {
				// Found next data
				newOffset = r.FileOffset
				found = true
				break
			}
		}
		if !found {
			return 0, syscall.ENXIO
		}
	} else { // SeekHole
		// Find the next hole
		found := false
		for i, r := range ranges {
			if offset < r.FileOffset {
				// Already in a hole
				newOffset = offset
				found = true
				break
			}
			if offset >= r.FileOffset && offset < r.FileOffset+r.Length {
				// In data, seek to the end of this region
				newOffset = r.FileOffset + r.Length
				found = true
				break
			}
			// Check if there's a gap between this range and the next
			if i < len(ranges)-1 && r.FileOffset+r.Length < ranges[i+1].FileOffset {
				if offset < ranges[i+1].FileOffset {
					newOffset = r.FileOffset + r.Length
					found = true
					break
				}
			}
		}
		if !found {
			// After all ranges, everything to EOF is a hole
			newOffset = offset
		}
	}

	return newOffset, nil
}
