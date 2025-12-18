package utils

import (
	"github.com/pbnjay/memory"
)

var MaxConcurrentClients = 16

var IsServer = false

func init() {
	sysMem, err := GetSysMem()
	if err != nil {
		return
	}

	gibs := int(sysMem.Total / (1024 * 1024 * 1024))
	MaxConcurrentClients = gibs
	if MaxConcurrentClients < 16 {
		MaxConcurrentClients = 16
	} else if MaxConcurrentClients > 512 {
		MaxConcurrentClients = 512
	}
}

type SysMem struct {
	Total     uint64 // Total system memory in bytes
	Available uint64 // Available memory in bytes
}

func GetSysMem() (*SysMem, error) {
	return &SysMem{
		Total:     memory.TotalMemory(),
		Available: memory.FreeMemory(),
	}, nil
}
