//go:build linux

package arpcfs

import (
	"errors"
	"fmt"
	"io"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/sonroyaalmerol/pbs-plus/internal/agent/vssfs"
	"github.com/sonroyaalmerol/pbs-plus/internal/syslog"
)

var bufferPool = sync.Pool{
	New: func() interface{} {
		// Default size - adjust based on your needs
		return make([]byte, 32*1024) // 32KB default buffer
	},
}

func (f *ARPCFile) Close() error {
	if f.isClosed.Load() {
		return nil
	}

	if f.fs.session == nil {
		syslog.L.Error("RPC failed: aRPC session is nil")
		return os.ErrInvalid
	}

	req := vssfs.CloseReq{HandleID: f.handleID}
	reqBytes, err := req.MarshalMsg(nil)
	if err != nil {
		return os.ErrInvalid
	}

	_, err = f.fs.session.CallMsgWithTimeout(10*time.Second, f.jobId+"/Close", reqBytes)
	if err != nil {
		syslog.L.Errorf("Write RPC failed (%s): %v", f.name, err)
		return err
	}
	f.isClosed.Store(true)

	return nil
}

func (f *ARPCFile) ReadAt(p []byte, off int64) (int, error) {
	if f.isClosed.Load() {
		return 0, os.ErrInvalid
	}

	if f.fs.session == nil {
		return 0, os.ErrInvalid
	}

	if len(p) == 0 {
		return 0, nil
	}

	const (
		maxChunkSize = 1 << 20 // 1MB chunks
		minChunkSize = 1 << 16 // 64KB minimum
		maxRetries   = 3
		retryDelay   = 100 * time.Millisecond
	)

	totalBytesRead := 0
	remaining := len(p)
	retryCount := 0

	// Get optimal chunk size
	chunkSize := maxChunkSize
	if remaining < maxChunkSize*4 {
		chunkSize = max(minChunkSize, remaining/4)
	}

	reqBuf := bufferPool.Get().([]byte)
	defer bufferPool.Put(reqBuf)

	for remaining > 0 {
		if err := f.fs.ctx.Err(); err != nil {
			return totalBytesRead, err
		}

		currentChunk := min(remaining, chunkSize)
		req := vssfs.ReadAtReq{
			HandleID: f.handleID,
			Offset:   off + int64(totalBytesRead),
			Length:   currentChunk,
		}

		reqBytes, err := req.MarshalMsg(reqBuf[:0])
		if err != nil {
			return totalBytesRead, os.ErrInvalid
		}

		// Try to read with retries
		bytesRead, err := func() (int, error) {
			for retryCount < maxRetries {
				if retryCount > 0 {
					// Exponential backoff
					delay := retryDelay * time.Duration(1<<uint(retryCount-1))
					time.Sleep(delay)
				}

				n, err := f.fs.session.CallMsgWithBuffer(
					f.fs.ctx,
					f.jobId+"/ReadAt",
					reqBytes,
					p[totalBytesRead:totalBytesRead+currentChunk],
				)

				if err == nil || err == io.EOF {
					retryCount = 0 // Reset retry count on success
					return n, err
				}

				if !isRetryableError(err) {
					return n, err // Don't retry non-retryable errors
				}

				retryCount++
			}
			return 0, fmt.Errorf("max retries (%d) exceeded", maxRetries)
		}()

		// Update progress
		if bytesRead > 0 {
			totalBytesRead += bytesRead
			remaining -= bytesRead
			go atomic.AddInt64(&f.fs.totalBytes, int64(bytesRead))
		}

		// Handle errors
		if err != nil {
			if err == io.EOF && totalBytesRead > 0 {
				return totalBytesRead, nil
			}
			return totalBytesRead, err
		}

		if bytesRead == 0 {
			break
		}
	}

	if totalBytesRead > 0 {
		return totalBytesRead, nil
	}
	return 0, io.EOF
}

// Helper to identify retryable errors
func isRetryableError(err error) bool {
	if err == nil {
		return false
	}

	// Unwrap the error since most are wrapped with fmt.Errorf
	err = errors.Unwrap(err)
	if err == nil {
		return false
	}

	// Check specific error strings/types that occur in CallMsgWithBuffer
	switch {
	case errors.Is(err, io.EOF):
		// EOF during response read or length prefix read is retryable
		return true
	case strings.Contains(err.Error(), "failed to open stream"):
		// Stream opening failures are retryable
		return true
	case strings.Contains(err.Error(), "failed to read response"):
		// Response reading failures are retryable
		return true
	case strings.Contains(err.Error(), "failed to read length prefix"):
		// Length prefix reading failures are retryable
		return true
	case strings.Contains(err.Error(), "read error after"):
		// Data reading failures are retryable
		return true
	}

	// Non-retryable errors:
	// - "failed to marshal request" (marshaling errors are not network-related)
	// - "failed to unmarshal response" (unmarshaling errors indicate protocol issues)
	// - "RPC error: status" (application-level errors)

	return false
}
