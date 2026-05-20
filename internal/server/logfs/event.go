//go:build linux

package logfs

import (
	"strings"
	"sync"
)

// Event represents a filesystem-level event from the PBS task log directory.
type Event struct {
	// Kind classifies the event.
	Kind EventKind

	// Path is the relative path within the log directory that triggered
	// the event (e.g. "tasks/active" or "tasks/ab/UPID:...").
	Path string

	// Data is the raw bytes that were written (for Write events).
	Data []byte
}

// EventKind classifies a filesystem event.
type EventKind int

const (
	// EventWrite is emitted when data is written to a file.
	EventWrite EventKind = iota

	// EventCreate is emitted when a new file is created.
	EventCreate

	// EventRename is emitted when a file is renamed.
	EventRename

	// EventUnlink is emitted when a file is removed.
	EventUnlink
)

func (k EventKind) String() string {
	switch k {
	case EventWrite:
		return "write"
	case EventCreate:
		return "create"
	case EventRename:
		return "rename"
	case EventUnlink:
		return "unlink"
	default:
		return "unknown"
	}
}

// Subscriber receives events from the event bus.
type Subscriber func(Event)

// EventBus distributes filesystem events to subscribers.
// Events are delivered in order per path but concurrently across paths.
type EventBus struct {
	mu          sync.RWMutex
	subscribers map[string][]Subscriber // path prefix -> subscribers
	allSubs     []Subscriber            // subscribers for all events
}

// NewEventBus creates a new EventBus.
func NewEventBus() *EventBus {
	return &EventBus{
		subscribers: make(map[string][]Subscriber),
	}
}

// Subscribe registers a subscriber for events matching the given path prefix.
// The prefix is matched against the relative path within the log directory
// (e.g. "tasks/active" or "tasks/ab/").
func (b *EventBus) Subscribe(prefix string, sub Subscriber) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.subscribers[prefix] = append(b.subscribers[prefix], sub)
}

// SubscribeAll registers a subscriber for all events.
func (b *EventBus) SubscribeAll(sub Subscriber) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.allSubs = append(b.allSubs, sub)
}

// Emit sends an event to all matching subscribers.
func (b *EventBus) Emit(ev Event) {
	b.mu.RLock()
	defer b.mu.RUnlock()

	for _, sub := range b.allSubs {
		sub(ev)
	}

	for prefix, subs := range b.subscribers {
		if strings.HasPrefix(ev.Path, prefix) {
			for _, sub := range subs {
				sub(ev)
			}
		}
	}
}
