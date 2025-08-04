package utils

import (
	"crypto/tls"
	"net"
	"net/http"
	"runtime"
	"time"

	"github.com/pbs-plus/pbs-plus/internal/syslog"
)

const MaxStreamBuffer = 8 * 1024 * 1024

var MaxConcurrentClients = 64
var MaxReceiveBuffer = MaxStreamBuffer * MaxConcurrentClients

func init() {
	sysMem, err := getSysMem()
	if err != nil {
		return
	}

	smuxMemoryBudget := sysMem.Available / 16

	MaxReceiveBuffer = int(smuxMemoryBudget)
	MaxConcurrentClients = int(smuxMemoryBudget) / MaxStreamBuffer

	syslog.L.Info().WithFields(map[string]interface{}{"MaxReceiveBuffer": MaxReceiveBuffer, "MaxStreamBuffer": MaxStreamBuffer, "MaxConcurrentClients": MaxConcurrentClients}).WithMessage("initialized aRPC buffer configurations").Write()
}

type sysMem struct {
	Total     uint64 // Total system memory in bytes
	Available uint64 // Available memory in bytes
	Free      uint64 // Free memory in bytes
}

func getSysMem() (*sysMem, error) {
	var memStats runtime.MemStats
	runtime.ReadMemStats(&memStats)

	estimated := memStats.Sys * 4 // Conservative estimate

	return &sysMem{
		Total:     estimated,
		Available: estimated / 2, // Very conservative
		Free:      estimated / 4,
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
