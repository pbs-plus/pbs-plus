package agentfs

import "syscall"

func GetAllocGranularity() int {
	// On Linux, the allocation granularity is typically the page size
	pageSize := syscall.Getpagesize()
	return pageSize
}
