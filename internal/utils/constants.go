package utils

import (
	"log"

	"github.com/pbnjay/memory"
)

const MaxStreamBuffer = 4 * 1024 * 1024

var MaxConcurrentClients = 16
var MaxReceiveBuffer = MaxStreamBuffer * MaxConcurrentClients

var IsServer = false

func init() {
	sysMem, err := GetSysMem()
	if err != nil {
		return
	}

	ratio := 16
	for MaxReceiveBuffer < MaxStreamBuffer {
		if ratio <= 2 {
			MaxReceiveBuffer = MaxStreamBuffer * 2
			break
		}

		MaxReceiveBuffer = int(sysMem.Available) / ratio
		ratio /= 2
	}

	gibs := int(sysMem.Total / (1024 * 1024 * 1024))
	MaxConcurrentClients = gibs
	if MaxConcurrentClients < 16 {
		MaxConcurrentClients = 16
	} else if MaxConcurrentClients > 512 {
		MaxConcurrentClients = 512
	}

	if IsServer {
		log.Printf("initialized aRPC buffer configurations with MaxReceiveBuffer: %d, MaxStreamBuffer: %d, MaxConcurrentClients: %d", MaxReceiveBuffer, MaxStreamBuffer, MaxConcurrentClients)
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
