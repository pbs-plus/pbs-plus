package binarystream

import (
	"encoding/binary"
	"fmt"
	"io"
	"sync"

	"github.com/xtaci/smux"
)

// BufferPool groups a fixed-size buffer and an associated sync.Pool.
type BufferPool struct {
	Size int
	Pool *sync.Pool
}

// Define a handful of pools with different buffer sizes.
var bufferPools = []BufferPool{
	{
		Size: 4096,
		Pool: &sync.Pool{
			New: func() interface{} {
				return make([]byte, 4096)
			},
		},
	},
	{
		Size: 16384,
		Pool: &sync.Pool{
			New: func() interface{} {
				return make([]byte, 16384)
			},
		},
	},
	{
		Size: 32768,
		Pool: &sync.Pool{
			New: func() interface{} {
				return make([]byte, 32768)
			},
		},
	},
}

// selectBufferPool returns a pool and its size based on the requested total
// length. The heuristic is: pick the smallest pool whose capacity is at least
// the requested length; if none qualifies, use the largest pool.
func selectBufferPool(totalLength int) (pool *sync.Pool, poolSize int) {
	for _, bp := range bufferPools {
		if totalLength <= bp.Size {
			return bp.Pool, bp.Size
		}
	}
	// Default to the largest pool.
	last := bufferPools[len(bufferPools)-1]
	return last.Pool, last.Size
}

// SendDataFromReader reads up to 'length' bytes from the provided io.Reader
// in chunks. For each chunk it writes a 4-byte little-endian prefix (the actual
// size of that chunk) to the smux stream, followed immediately by the chunk data.
// After sending all chunks it writes a sentinel (0) and then the final total.
func SendDataFromReader(r io.Reader, length int, stream *smux.Stream) error {
	if stream == nil {
		return fmt.Errorf("stream is nil")
	}

	// If length is zero, write the sentinel and a final total of 0 to signal an empty result.
	if length == 0 || r == nil {
		if err := binary.Write(stream, binary.LittleEndian, uint32(0)); err != nil {
			return fmt.Errorf("failed to write sentinel: %w", err)
		}
		if err := binary.Write(stream, binary.LittleEndian, uint32(0)); err != nil {
			return fmt.Errorf("failed to write final total: %w", err)
		}
		return nil
	}

	// Choose a buffer pool based on the expected total length.
	pool, poolSize := selectBufferPool(length)
	chunkBuf := pool.Get().([]byte)
	// Make sure we use the entire capacity of the buffer.
	chunkBuf = chunkBuf[:poolSize]
	defer pool.Put(chunkBuf)

	totalRead := 0

	for totalRead < length {
		remaining := length - totalRead
		readSize := min(len(chunkBuf), remaining)
		if readSize <= 0 {
			break
		}

		n, err := r.Read(chunkBuf[:readSize])
		if err != nil && err != io.EOF {
			return fmt.Errorf("read error: %w", err)
		}
		if n == 0 {
			break
		}

		// Write the chunk's size prefix (32-bit little-endian).
		if err := binary.Write(stream, binary.LittleEndian, uint32(n)); err != nil {
			return fmt.Errorf("failed to write chunk size prefix: %w", err)
		}

		// Write the actual chunk data.
		if _, err := stream.Write(chunkBuf[:n]); err != nil {
			return fmt.Errorf("failed to write chunk data: %w", err)
		}

		totalRead += n
	}

	// Write sentinel (0) to signal there are no more chunks.
	if err := binary.Write(stream, binary.LittleEndian, uint32(0)); err != nil {
		return fmt.Errorf("failed to write sentinel: %w", err)
	}

	// Write the final total number of bytes sent.
	if err := binary.Write(stream, binary.LittleEndian, uint32(totalRead)); err != nil {
		return fmt.Errorf("failed to write final total: %w", err)
	}

	return nil
}

// ReceiveData reads data from the smux stream into the provided buffer.
// It expects each chunk to be preceded by its 4-byte size. A chunk size of 0
// signals that data transfer has finished; it is then followed by a final total
// which is compared to the accumulated data.
func ReceiveData(stream *smux.Stream, buffer []byte) (int, error) {
	totalRead := 0

	for {
		var chunkSize uint32
		if err := binary.Read(stream, binary.LittleEndian, &chunkSize); err != nil {
			return totalRead, fmt.Errorf("failed to read chunk size: %w", err)
		}

		// A chunk size of zero signals the end.
		if chunkSize == 0 {
			var finalTotal uint32
			if err := binary.Read(stream, binary.LittleEndian, &finalTotal); err != nil {
				return totalRead, fmt.Errorf("failed to read final total: %w", err)
			}
			if int(finalTotal) != totalRead {
				return totalRead, fmt.Errorf("data length mismatch: expected %d bytes, got %d",
					finalTotal, totalRead)
			}
			break
		}

		// Ensure the provided buffer is large enough.
		if totalRead+int(chunkSize) > len(buffer) {
			return totalRead, fmt.Errorf("buffer overflow: need %d bytes, have %d",
				totalRead+int(chunkSize), len(buffer))
		}

		n, err := io.ReadFull(stream, buffer[totalRead:totalRead+int(chunkSize)])
		totalRead += n
		if err != nil {
			return totalRead, fmt.Errorf("failed to read chunk data: %w", err)
		}
	}

	return totalRead, nil
}
