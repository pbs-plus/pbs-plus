//go:build windows

package syslog

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"os/signal"
	"sync"
	"syscall"

	"github.com/rs/zerolog/log"
	"github.com/sonroyaalmerol/pbs-plus/internal/agent"
)

var (
	logQueue   chan LogEntry
	workerOnce sync.Once
	workerWg   sync.WaitGroup
)

func init() {
	initializeWorkerPool()
	go stopWorkerPool()
}

// initializeWorkerPool sets up a singleton worker pool.
func initializeWorkerPool() {
	workerOnce.Do(func() {
		logQueue = make(chan LogEntry, 100)
		startWorkerPool(5)
	})
}

// startWorkerPool starts the given number of worker goroutines.
func startWorkerPool(workerCount int) {
	workerWg.Add(workerCount)
	for i := 0; i < workerCount; i++ {
		go worker()
	}
}

// worker processes log messages from the logQueue.
func worker() {
	defer workerWg.Done()
	for logMsg := range logQueue {
		sendLogToServer(logMsg)
	}
}

// sendLogToServer marshals and sends the LogEntry as JSON to a remote server.
func sendLogToServer(entry LogEntry) {
	if entry.Err != nil {
		entry.ErrString = entry.Err.Error()
	}

	reqBody, err := json.Marshal(entry)
	if err != nil {
		log.Error().Err(err).Msg("Failed to marshal log entry")
		return
	}

	body, err := agent.ProxmoxHTTPRequest(
		"POST",
		"/api2/json/d2d/agent-log",
		bytes.NewBuffer(reqBody),
		nil,
	)
	if err != nil {
		log.Error().Err(err).Msg("Failed to send log to remote server")
		return
	}
	defer body.Close()
	_, _ = io.Copy(io.Discard, body)
}

// stopWorkerPool gracefully shuts down the worker pool.
func stopWorkerPool() {
	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer cancel()
	<-ctx.Done()
	close(logQueue)
	workerWg.Wait()
}
