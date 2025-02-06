package utils

import (
	"crypto/tls"
	"net"
	"net/http"
	"time"
)

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
	MaxIdleConns:        200,             // Max idle connections across all hosts
	MaxIdleConnsPerHost: 20,              // Max idle connections per host
	IdleConnTimeout:     5 * time.Minute, // Timeout for idle connections
	DisableKeepAlives:   false,
	DialContext: (&net.Dialer{
		Timeout:   10 * time.Minute, // Connection timeout
		KeepAlive: 5 * time.Minute,  // TCP keep-alive
	}).DialContext,
	TLSHandshakeTimeout:   2 * time.Minute,
	TLSClientConfig:       &tls.Config{InsecureSkipVerify: true},
	ExpectContinueTimeout: 2 * time.Minute, // Timeout for expect-continue responses
}
