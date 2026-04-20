//go:build linux

package api

import (
	"net/http"
	"time"
)

var sharedClient = &http.Client{
	Timeout: 30 * time.Second, // Set a timeout for requests
	Transport: &http.Transport{
		MaxIdleConns:        100, // Maximum idle connections
		MaxIdleConnsPerHost: 10,  // Maximum idle connections per host
	},
}
