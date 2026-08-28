package rpcserver

import (
	"context"
	"fmt"
	"net"
	"net/rpc"
	"os"
)

func Run(ctx context.Context, socketPath, serviceName string, receiver any) error {
	if err := os.RemoveAll(socketPath); err != nil {
		return fmt.Errorf("remove RPC socket %s: %w", socketPath, err)
	}

	listener, err := net.Listen("unix", socketPath)
	if err != nil {
		return fmt.Errorf("listen on RPC socket %s: %w", socketPath, err)
	}

	server := rpc.NewServer()
	if err := server.RegisterName(serviceName, receiver); err != nil {
		_ = listener.Close()
		_ = os.Remove(socketPath)
		return fmt.Errorf("register RPC service %s: %w", serviceName, err)
	}

	acceptErr := make(chan error, 1)
	go func() {
		for {
			conn, err := listener.Accept()
			if err != nil {
				acceptErr <- err
				return
			}
			go server.ServeConn(conn)
		}
	}()

	select {
	case <-ctx.Done():
		if err := listener.Close(); err != nil {
			return fmt.Errorf("close RPC socket %s: %w", socketPath, err)
		}
		<-acceptErr
	case err := <-acceptErr:
		if ctx.Err() == nil {
			return fmt.Errorf("RPC server stopped: %w", err)
		}
	}

	if err := os.Remove(socketPath); err != nil && !os.IsNotExist(err) {
		return fmt.Errorf("remove RPC socket %s: %w", socketPath, err)
	}

	return nil
}
