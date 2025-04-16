package types

import (
	"bytes"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/pbs-plus/pbs-plus/internal/arpc/arpcdata"
)

func TestEncodeDecodeConcurrency(t *testing.T) {
	t.Run("LseekResp", func(t *testing.T) {
		original := &LseekResp{NewOffset: 12345}
		validateEncodeDecodeConcurrency(t, original, func() arpcdata.Encodable {
			return &LseekResp{}
		})
	})

	t.Run("AgentFileInfo", func(t *testing.T) {
		original := &AgentFileInfo{
			Name:    "testfile.txt",
			Size:    1024,
			Mode:    0644,
			ModTime: time.Now(),
			IsDir:   false,
			Blocks:  8,
		}
		validateEncodeDecodeConcurrency(t, original, func() arpcdata.Encodable {
			return &AgentFileInfo{}
		})
	})

	t.Run("AgentDirEntry", func(t *testing.T) {
		original := &AgentDirEntry{
			Name: "subdir",
			Mode: 0755,
		}
		validateEncodeDecodeConcurrency(t, original, func() arpcdata.Encodable {
			return &AgentDirEntry{}
		})
	})

	t.Run("StatFS", func(t *testing.T) {
		original := &StatFS{
			Bsize:   4096,
			Blocks:  100000,
			Bfree:   50000,
			Bavail:  40000,
			Files:   10000,
			Ffree:   5000,
			NameLen: 255,
		}
		validateEncodeDecodeConcurrency(t, original, func() arpcdata.Encodable {
			return &StatFS{}
		})
	})

	t.Run("OpenFileReq", func(t *testing.T) {
		original := &OpenFileReq{
			Path: "/path/to/file",
			Flag: 2,
			Perm: 0644,
		}
		validateEncodeDecodeConcurrency(t, original, func() arpcdata.Encodable {
			return &OpenFileReq{}
		})
	})

	t.Run("StatReq", func(t *testing.T) {
		original := &StatReq{Path: "/path/to/stat"}
		validateEncodeDecodeConcurrency(t, original, func() arpcdata.Encodable {
			return &StatReq{}
		})
	})

	t.Run("ReadDirReq", func(t *testing.T) {
		original := &ReadDirReq{
			HandleID: FileHandleId(12345),
		}
		validateEncodeDecodeConcurrency(t, original, func() arpcdata.Encodable {
			return &ReadDirReq{}
		})
	})

	t.Run("ReadReq", func(t *testing.T) {
		original := &ReadReq{
			HandleID: FileHandleId(12345),
			Length:   4096,
		}
		validateEncodeDecodeConcurrency(t, original, func() arpcdata.Encodable {
			return &ReadReq{}
		})
	})

	t.Run("ReadAtReq", func(t *testing.T) {
		original := &ReadAtReq{
			HandleID: FileHandleId(12345),
			Offset:   1024,
			Length:   4096,
		}
		validateEncodeDecodeConcurrency(t, original, func() arpcdata.Encodable {
			return &ReadAtReq{}
		})
	})

	t.Run("CloseReq", func(t *testing.T) {
		original := &CloseReq{HandleID: FileHandleId(12345)}
		validateEncodeDecodeConcurrency(t, original, func() arpcdata.Encodable {
			return &CloseReq{}
		})
	})

	t.Run("BackupReq", func(t *testing.T) {
		original := &BackupReq{
			JobId: "job123",
			Drive: "/dev/sda1",
		}
		validateEncodeDecodeConcurrency(t, original, func() arpcdata.Encodable {
			return &BackupReq{}
		})
	})

	t.Run("LseekReq", func(t *testing.T) {
		original := &LseekReq{
			HandleID: FileHandleId(12345),
			Offset:   1024,
			Whence:   1,
		}
		validateEncodeDecodeConcurrency(t, original, func() arpcdata.Encodable {
			return &LseekReq{}
		})
	})

	t.Run("ReadDirEntries", func(t *testing.T) {
		original := ReadDirEntries{
			Entries: []AgentDirEntry{
				{Name: "file1.txt", Mode: 0644},
				{Name: "file2.txt", Mode: 0755},
			},
		}
		validateEncodeDecodeConcurrency(t, &original, func() arpcdata.Encodable {
			return &ReadDirEntries{}
		})
	})
}

// validateEncodeDecodeConcurrency tests encoding and decoding concurrently.
func validateEncodeDecodeConcurrency(t *testing.T, original arpcdata.Encodable, newDecoded func() arpcdata.Encodable) {
	const numGoroutines = 100
	var wg sync.WaitGroup

	// Channel to collect errors from goroutines.
	errCh := make(chan error, numGoroutines)

	// Run multiple goroutines to encode and decode concurrently.
	for i := 0; i < numGoroutines; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()

			// Encode the original object.
			encoded, err := original.Encode()
			if err != nil {
				errCh <- err
				return
			}

			// Create a new decoded instance for this goroutine.
			decoded := newDecoded()

			// Decode into the new object.
			if err := decoded.Decode(encoded); err != nil {
				errCh <- err
				return
			}

			// Compare the original and decoded objects.
			if !deepCompare(original, decoded) {
				errCh <- fmt.Errorf("original and decoded objects do not match.\nOriginal: %+v\nDecoded: %+v", original, decoded)
			}
		}()
	}

	// Wait for all goroutines to finish.
	wg.Wait()
	close(errCh)

	// Check for errors.
	for err := range errCh {
		if err != nil {
			t.Fatalf("concurrent encode/decode failed: %v", err)
		}
	}
}

// deepCompare performs a deep comparison of two Encodable objects.
func deepCompare(a, b arpcdata.Encodable) bool {
	// Perform a type-specific comparison for known types.
	switch objA := a.(type) {
	case *ReadDirEntries:
		objB, ok := b.(*ReadDirEntries)
		if !ok {
			return false
		}
		if len(objA.Entries) != len(objB.Entries) {
			return false
		}
		for i := range objA.Entries {
			if objA.Entries[i] != objB.Entries[i] {
				return false
			}
		}
		return true
	}

	// Fallback: Compare the encoded byte slices.
	encodedA, errA := a.Encode()
	encodedB, errB := b.Encode()

	if errA != nil || errB != nil {
		return false
	}

	return bytes.Equal(encodedA, encodedB)
}
