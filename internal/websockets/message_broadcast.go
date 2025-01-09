package websockets

import (
	"context"
	"sync"
	"time"
)

type BroadcastServer interface {
	Subscribe() <-chan Message
	CancelSubscription(<-chan Message)
	Close()
}

type broadcastServer struct {
	source         <-chan Message
	listeners      sync.Map
	addListener    chan chan Message
	removeListener chan chan Message
	cancel         context.CancelFunc
}

const sendTimeout = 100 * time.Millisecond

func (s *broadcastServer) Subscribe() <-chan Message {
	newListener := make(chan Message, 10) // Buffered channel
	s.addListener <- newListener
	return newListener
}

func (s *broadcastServer) CancelSubscription(channel <-chan Message) {
	s.removeListener <- channel
}

func (s *broadcastServer) Close() {
	s.cancel()
}

func (s *broadcastServer) serve(ctx context.Context) {
	defer func() {
		// Close all listeners when shutting down
		s.listeners.Range(func(key, _ interface{}) bool {
			if listener, ok := key.(chan Message); ok {
				close(listener)
			}
			return true
		})
	}()

	for {
		select {
		case <-ctx.Done():
			return
		case newListener := <-s.addListener:
			s.listeners.Store(newListener, struct{}{})
		case listenerToRemove := <-s.removeListener:
			if _, exists := s.listeners.LoadAndDelete(listenerToRemove); exists {
				close(listenerToRemove)
			}
		case val, ok := <-s.source:
			if !ok {
				return
			}
			// Broadcast the message to all listeners
			s.listeners.Range(func(key, _ interface{}) bool {
				listener, ok := key.(chan Message)
				if !ok {
					return true
				}
				select {
				case listener <- val:
					// Message sent successfully
				case <-time.After(sendTimeout):
					// Listener is stuck, remove it
					s.listeners.Delete(listener)
					close(listener)
				case <-ctx.Done():
					return false
				}
				return true
			})
		}
	}
}

func NewBroadcastServer(ctx context.Context, source <-chan Message) BroadcastServer {
	ctxLocal, ctxCancel := context.WithCancel(ctx)
	service := &broadcastServer{
		source:         source,
		addListener:    make(chan chan Message, 10),  // Buffered to avoid blocking
		removeListener: make(chan chan Message, 10), // Buffered to avoid blocking
		cancel:         ctxCancel,
	}
	go service.serve(ctxLocal)
	return service
}
