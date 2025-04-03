//go:build linux
// +build linux

package rpclocker

import (
	"fmt"
	"net"
	"net/rpc"
	"time"

	"github.com/pbs-plus/pbs-plus/internal/store/constants"
)

// LockerClient provides a client interface to the LockerRPC service.
type LockerClient struct {
	client *rpc.Client
}

// NewLockerClient creates a new client connected to the RPC lock server.
func NewLockerClient() (*LockerClient, error) {
	conn, err := net.DialTimeout("unix", constants.LockSocketPath, 2*time.Second)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to lock server at %s: %w", constants.LockSocketPath, err)
	}
	return &LockerClient{client: rpc.NewClient(conn)}, nil
}

// Close closes the connection to the RPC server.
func (c *LockerClient) Close() error {
	if c.client != nil {
		return c.client.Close()
	}
	return nil
}

// Lock acquires a lock for the given key via RPC.
func (c *LockerClient) Lock(key string) error {
	args := &Args{Key: key}
	var reply Reply

	err := c.client.Call("LockerRPC.Lock", args, &reply)
	if err != nil {
		return fmt.Errorf("rpc Lock failed for key '%s': %w", key, err)
	}
	if !reply.Success {
		return fmt.Errorf("lock operation failed server-side for key '%s'", key)
	}
	return nil
}

// TryLock attempts to acquire a lock for the given key via RPC.
// Returns true if the lock was acquired, false otherwise.
func (c *LockerClient) TryLock(key string) (bool, error) {
	args := &Args{Key: key}
	var reply Reply
	err := c.client.Call("LockerRPC.TryLock", args, &reply)
	if err != nil {
		return false, fmt.Errorf("rpc TryLock failed for key '%s': %w", key, err)
	}
	return reply.Success, nil
}

// Unlock releases the lock for the given key via RPC.
func (c *LockerClient) Unlock(key string) error {
	args := &Args{Key: key}
	var reply Reply
	err := c.client.Call("LockerRPC.Unlock", args, &reply)
	if err != nil {
		if err.Error() == fmt.Sprintf("key '%s' not found or never locked", key) {
			return err
		}
		return fmt.Errorf("rpc Unlock failed for key '%s': %w", key, err)
	}
	if !reply.Success {
		// Should correspond to the "key not found" case handled above by checking err
		return fmt.Errorf("unlock operation failed server-side for key '%s'", key)
	}
	return nil
}
