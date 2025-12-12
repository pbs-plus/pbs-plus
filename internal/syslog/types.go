package syslog

import (
	"sync"

	"github.com/rs/zerolog"
)

type Logger struct {
	mu       sync.RWMutex
	zlog     *zerolog.Logger
	hostname string
	Server   bool
	disabled bool
}

// LogEntry represents a structured log entry.
type LogEntry struct {
	Level     string                 `json:"level"`
	Message   string                 `json:"message"`
	Hostname  string                 `json:"hostname,omitempty"`
	JobID     string                 `json:"job_id,omitempty"`
	Err       error                  `json:"-"`
	ErrString string                 `json:"error,omitempty"`
	Fields    map[string]any `json:"fields,omitempty"`
	logger    *Logger                `json:"-"`
}
