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

// ReceiveData reads data from the smux stream, using the buffer pools and growing as needed.
func ReceiveData(stream *smux.Stream) ([]byte, int, error) {
	if stream == nil {
		return nil, 0, fmt.Errorf("stream is nil")
	}

	currentPoolIndex := 0
	pool, poolSize := bufferPools[currentPoolIndex].Pool, bufferPools[currentPoolIndex].Size
	buffer := pool.Get().([]byte)
	buffer = buffer[:poolSize]

	defer func() {
		if buffer != nil {
			pool.Put(buffer)
		}
	}()

	totalRead := 0
	var result []byte

	for {
		var chunkSize uint32
		if err := binary.Read(stream, binary.LittleEndian, &chunkSize); err != nil {
			return result, totalRead, fmt.Errorf("failed to read chunk size: %w", err)
		}

		if chunkSize == 0 {
			var finalTotal uint32
			if err := binary.Read(stream, binary.LittleEndian, &finalTotal); err != nil {
				return result, totalRead, fmt.Errorf("failed to read final total: %w", err)
			}
			if int(finalTotal) != totalRead {
				return result, totalRead, fmt.Errorf(
					"data length mismatch: expected %d bytes, got %d",
					finalTotal,
					totalRead,
				)
			}
			break
		}

		neededSize := totalRead + int(chunkSize)
		if neededSize > poolSize {
			newPoolIndex := currentPoolIndex
			for i := currentPoolIndex + 1; i < len(bufferPools); i++ {
				if bufferPools[i].Size >= neededSize {
					newPoolIndex = i
					break
				}
			}

			if newPoolIndex != currentPoolIndex {
				newPool := bufferPools[newPoolIndex].Pool
				newBuffer := newPool.Get().([]byte)
				newBuffer = newBuffer[:bufferPools[newPoolIndex].Size]

				copy(newBuffer, buffer)

				pool.Put(buffer)

				buffer = newBuffer
				pool = newPool
				poolSize = bufferPools[newPoolIndex].Size
				currentPoolIndex = newPoolIndex
			} else {
				if result == nil {
					result = make([]byte, neededSize)
					copy(result, buffer[:totalRead])
				} else {
					newResult := make([]byte, neededSize)
					copy(newResult, result)
					result = newResult
				}

				pool.Put(buffer)
				buffer = nil
			}
		}

		if buffer != nil {
			n, err := io.ReadFull(stream, buffer[totalRead:totalRead+int(chunkSize)])
			totalRead += n
			if err != nil {
				return result, totalRead, fmt.Errorf("failed to read chunk data: %w", err)
			}
		} else {
			n, err := io.ReadFull(stream, result[totalRead:totalRead+int(chunkSize)])
			totalRead += n
			if err != nil {
				return result, totalRead, fmt.Errorf("failed to read chunk data: %w", err)
			}
		}
	}

	if buffer != nil {
		result = make([]byte, totalRead)
		copy(result, buffer[:totalRead])
	} else if len(result) != totalRead {
		newResult := make([]byte, totalRead)
		copy(newResult, result)
		result = newResult
	}

	return result, totalRead, nil
}
