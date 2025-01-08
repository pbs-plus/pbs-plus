package websockets

import "context"

type BroadcastServer interface {
	Subscribe() <-chan Message
	CancelSubscription(<-chan Message)
	Close()
}

type broadcastServer struct {
	source         <-chan Message
	listeners      []chan Message
	addListener    chan chan Message
	removeListener chan (<-chan Message)
	cancel         context.CancelFunc
}

func (s *broadcastServer) Subscribe() <-chan Message {
	newListener := make(chan Message)
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
		for _, listener := range s.listeners {
			if listener != nil {
				close(listener)
			}
		}
	}()

	for {
		select {
		case <-ctx.Done():
			return
		case newListener := <-s.addListener:
			s.listeners = append(s.listeners, newListener)
		case listenerToRemove := <-s.removeListener:
			for i, ch := range s.listeners {
				if ch == listenerToRemove {
					s.listeners[i] = s.listeners[len(s.listeners)-1]
					s.listeners = s.listeners[:len(s.listeners)-1]
					close(ch)
					break
				}
			}
		case val, ok := <-s.source:
			if !ok {
				return
			}
			for _, listener := range s.listeners {
				if listener != nil {
					select {
					case listener <- val:
					case <-ctx.Done():
						return
					}
				}
			}
		}
	}
}

func NewBroadcastServer(ctx context.Context, source <-chan Message) BroadcastServer {
	ctxLocal, ctxCancel := context.WithCancel(ctx)

	service := &broadcastServer{
		source:         source,
		listeners:      make([]chan Message, 0),
		addListener:    make(chan chan Message),
		removeListener: make(chan (<-chan Message)),
		cancel:         ctxCancel,
	}
	go service.serve(ctxLocal)
	return service
}
