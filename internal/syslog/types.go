package syslog

import (
	"sync"

	"github.com/rs/zerolog"
)

type Logger struct {
	mu   sync.RWMutex
	zlog *zerolog.Logger
}

// LogEntry represents a structured log entry.
type LogEntry struct {
	Level     string                 `json:"level"`
	Message   string                 `json:"message"`
	Hostname  string                 `json:"hostname,omitempty"`
	Err       error                  `json:"-"`
	ErrString string                 `json:"error,omitempty"`
	Fields    map[string]interface{} `json:"fields,omitempty"`
	logger    *Logger                `json:"-"`
}
