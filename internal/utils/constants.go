package utils

import (
	"crypto/tls"
	"net"
	"net/http"
	"time"
)

const MAX_CONCURRENT_CLIENTS = 64

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
