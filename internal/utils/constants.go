package utils

import (
	"crypto/tls"
	"log"
	"net"
	"net/http"
	"time"

	"github.com/pbnjay/memory"
)

const MaxStreamBuffer = 4 * 1024 * 1024

var MaxConcurrentClients = 64
var MaxReceiveBuffer = MaxStreamBuffer * MaxConcurrentClients

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

	MaxConcurrentClients = (int(sysMem.Available) / (1024 * 1024 * 1024)) * 2

	log.Printf("initialized aRPC buffer configurations with MaxReceiveBuffer: %d, MaxStreamBuffer: %d, MaxConcurrentClients: %d", MaxReceiveBuffer, MaxStreamBuffer, MaxConcurrentClients)
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

var BaseTransport = &http.Transport{
	MaxIdleConns:        200,              // Max idle connections across all hosts
	MaxIdleConnsPerHost: 20,               // Max idle connections per host
	IdleConnTimeout:     15 * time.Second, // Timeout for idle connections
	DisableKeepAlives:   false,
	DialContext: (&net.Dialer{
		Timeout:   30 * time.Second, // Connection timeout
		KeepAlive: 30 * time.Second, // TCP keep-alive
	}).DialContext,
	TLSHandshakeTimeout:   10 * time.Second,
	TLSClientConfig:       &tls.Config{InsecureSkipVerify: true},
	ExpectContinueTimeout: 1 * time.Second, // Timeout for expect-continue responses
}

var MountTransport = &http.Transport{
	MaxIdleConns:       0,
	DisableKeepAlives:  true,
	DisableCompression: false,
	IdleConnTimeout:    0,
	DialContext: (&net.Dialer{
		Timeout:   5 * time.Minute,
		KeepAlive: 0,
	}).DialContext,
	TLSHandshakeTimeout: 5 * time.Minute,
	TLSClientConfig: &tls.Config{
		InsecureSkipVerify: true,
	},
	ExpectContinueTimeout: 1 * time.Minute,
	WriteBufferSize:       0,
	ReadBufferSize:        0,
	ForceAttemptHTTP2:     false,
}
