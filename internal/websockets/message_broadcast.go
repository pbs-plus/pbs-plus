package websockets

import (
	"context"
	"sync"
)

type BroadcastServer interface {
	Subscribe() <-chan Message
	CancelSubscription(<-chan Message)
	Close()
	Broadcast(msg Message) error
}

type broadcastServer struct {
	sync.RWMutex
	listeners      map[chan Message]struct{}
	addListener    chan chan Message
	removeListener chan (<-chan Message)
	cancel         context.CancelFunc
	done           chan struct{}
}

func NewBroadcastServer(ctx context.Context) BroadcastServer {
	ctxLocal, ctxCancel := context.WithCancel(ctx)

	service := &broadcastServer{
		listeners:      make(map[chan Message]struct{}),
		addListener:    make(chan chan Message),
		removeListener: make(chan (<-chan Message)),
		cancel:         ctxCancel,
		done:           make(chan struct{}),
	}

	go service.serve(ctxLocal)
	return service
}

func (s *broadcastServer) Subscribe() <-chan Message {
	newListener := make(chan Message, 32) // Increased buffer for better performance
	s.addListener <- newListener
	return newListener
}

func (s *broadcastServer) CancelSubscription(channel <-chan Message) {
	s.Lock()
	for ch := range s.listeners {
		if ch == channel {
			delete(s.listeners, ch)
			close(ch)
			break
		}
	}
	s.Unlock()
}

func (s *broadcastServer) Close() {
	s.cancel()
	<-s.done // Wait for serve goroutine to finish
}

func (s *broadcastServer) Broadcast(msg Message) error {
	s.RLock()
	deadListeners := make([]chan Message, 0)

	// Attempt to send to all listeners
	for listener := range s.listeners {
		select {
		case listener <- msg:
			// Message sent successfully
		default:
			// Channel is full or stuck
			deadListeners = append(deadListeners, listener)
		}
	}
	s.RUnlock()

	// Clean up dead listeners
	if len(deadListeners) > 0 {
		s.Lock()
		for _, listener := range deadListeners {
			delete(s.listeners, listener)
			close(listener)
		}
		s.Unlock()
	}

	return nil
}

func (s *broadcastServer) serve(ctx context.Context) {
	defer func() {
		s.Lock()
		for listener := range s.listeners {
			close(listener)
		}
		clear(s.listeners)
		s.Unlock()
		close(s.done)
	}()

	for {
		select {
		case <-ctx.Done():
			return

		case newListener := <-s.addListener:
			s.Lock()
			s.listeners[newListener] = struct{}{}
			s.Unlock()
		}
	}
}
