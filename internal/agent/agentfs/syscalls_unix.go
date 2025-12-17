//go:build unix

package agentfs

import (
	"syscall"
)

func getAllocGranularity() int {
	// On Linux, the allocation granularity is typically the page size
	pageSize := syscall.Getpagesize()
	return pageSize
}
