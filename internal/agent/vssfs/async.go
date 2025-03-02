//go:build windows

package vssfs

import (
	"context"
	"fmt"
	"sync"
	"time"

	"golang.org/x/sys/windows"
)

// overlappedPool reuses OVERLAPPED structures.
var overlappedPool = sync.Pool{
	New: func() interface{} {
		return new(windows.Overlapped)
	},
}

// Get a new OVERLAPPED instance.
func getOverlapped() *windows.Overlapped {
	return overlappedPool.Get().(*windows.Overlapped)
}

// Return an OVERLAPPED instance to the pool.
func putOverlapped(ov *windows.Overlapped) {
	*ov = windows.Overlapped{}
	overlappedPool.Put(ov)
}

type Completion struct {
	BytesTransferred uint32
	Key              uintptr
	Overlapped       *windows.Overlapped
	Err              error
}

// IOCP encapsulates a completion port.
type IOCP struct {
	Port windows.Handle
	// You might deliver completions through a channel.
	Completions chan Completion
}

// NewIOCP creates a new IOCP.
func NewIOCP() (*IOCP, error) {
	port, err := windows.CreateIoCompletionPort(windows.InvalidHandle, 0, 0, 0)
	if err != nil {
		return nil, fmt.Errorf("CreateIoCompletionPort error: %w", err)
	}
	return &IOCP{
		Port:        port,
		Completions: make(chan Completion, 128),
	}, nil
}

// AssociateHandle associates a file handle with the IOCP.
func (iocp *IOCP) AssociateHandle(handle windows.Handle, key uintptr) error {
	// Passing our IOCP port ensures that all completions are queued to our port.
	_, err := windows.CreateIoCompletionPort(handle, iocp.Port, key, 0)
	if err != nil {
		return fmt.Errorf("AssociateHandle error: %w", err)
	}
	return nil
}

// Loop waits for I/O completions and sends them to the Completions channel.
func (iocp *IOCP) Loop(ctx context.Context) {
	go func() {
		defer close(iocp.Completions)
		for {
			var bytesTransferred uint32
			var completionKey uintptr
			var ov *windows.Overlapped
			err := windows.GetQueuedCompletionStatus(iocp.Port, &bytesTransferred, &completionKey, &ov, windows.INFINITE)
			comp := Completion{
				BytesTransferred: bytesTransferred,
				Key:              completionKey,
				Overlapped:       ov,
				Err:              err,
			}
			select {
			case iocp.Completions <- comp:
			case <-ctx.Done():
				return
			}
		}
	}()
}

func asyncReadFile(handle windows.Handle, buf []byte, offset int64, iocp *IOCP, timeout time.Duration) (uint32, error) {
	ov := getOverlapped()
	// Set the offset in the OVERLAPPED structure.
	ov.Offset = uint32(offset)
	ov.OffsetHigh = uint32(offset >> 32)

	// Associate this operation with a key (you could use a pointer to a context object)
	const key uintptr = 1
	if err := iocp.AssociateHandle(handle, key); err != nil {
		putOverlapped(ov)
		return 0, err
	}

	var bytesRead uint32
	err := windows.ReadFile(handle, buf, &bytesRead, ov)
	if err != nil && err != windows.ERROR_IO_PENDING {
		putOverlapped(ov)
		return 0, fmt.Errorf("ReadFile error: %w", err)
	}

	// Wait for completion to be delivered via IOCP.
	timeoutCh := time.After(timeout)
	for {
		select {
		case comp := <-iocp.Completions:
			// Check if the completion matches our key and overlapped pointer.
			if comp.Key == key && comp.Overlapped == ov {
				putOverlapped(ov)
				if comp.Err != nil {
					return 0, fmt.Errorf("IOCP completed with error: %w", comp.Err)
				}
				return comp.BytesTransferred, nil
			} else {
				// Handle completion for another operation (or push it back to a central queue)
				// For this simple func, ignore.
			}
		case <-timeoutCh:
			putOverlapped(ov)
			return 0, fmt.Errorf("async read timed out")
		}
	}
}
