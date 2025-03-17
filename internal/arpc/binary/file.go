package binarystream

import (
	"encoding/binary"
	"fmt"
	"io"
	"sync"

	"github.com/xtaci/smux"
)

var bufferPool = sync.Pool{
	New: func() interface{} {
		buf := make([]byte, 65536)
		return buf
	},
}

func SendDataFromReader(r io.Reader, length int, stream *smux.Stream) error {
	if stream == nil {
		return fmt.Errorf("stream is nil")
	}

	var header [4]byte

	// If nothing to send, write sentinel values.
	if length == 0 || r == nil {
		// Write 0 as the sentinel chunk size.
		binary.LittleEndian.PutUint32(header[:], 0)
		if _, err := stream.Write(header[:]); err != nil {
			return fmt.Errorf("failed to write sentinel: %w", err)
		}
		// Write 0 as the final total.
		binary.LittleEndian.PutUint32(header[:], 0)
		if _, err := stream.Write(header[:]); err != nil {
			return fmt.Errorf("failed to write final total: %w", err)
		}
		return nil
	}

	buf := bufferPool.Get().([]byte)
	defer bufferPool.Put(buf)

	totalRead := 0
	for totalRead < length {
		readSize := min(len(buf), length-totalRead)
		n, err := r.Read(buf[:readSize])
		if err != nil && err != io.EOF {
			return fmt.Errorf("read error: %w", err)
		}
		if n == 0 {
			break
		}

		// Write the chunk size header.
		binary.LittleEndian.PutUint32(header[:], uint32(n))
		if _, err := stream.Write(header[:]); err != nil {
			return fmt.Errorf("failed to write chunk size: %w", err)
		}

		// Write the actual chunk data.
		if _, err := stream.Write(buf[:n]); err != nil {
			return fmt.Errorf("failed to write chunk data: %w", err)
		}

		totalRead += n
	}

	// Write the sentinel
	binary.LittleEndian.PutUint32(header[:], 0)
	if _, err := stream.Write(header[:]); err != nil {
		return fmt.Errorf("failed to write sentinel: %w", err)
	}
	// Write the final total.
	binary.LittleEndian.PutUint32(header[:], uint32(totalRead))
	if _, err := stream.Write(header[:]); err != nil {
		return fmt.Errorf("failed to write final total: %w", err)
	}

	return nil
}

func ReceiveData(stream *smux.Stream, buffer []byte) (int, error) {
	var header [4]byte
	totalRead := 0

	for {
		// Read the 4-byte chunk size header.
		if _, err := io.ReadFull(stream, header[:]); err != nil {
			return totalRead, fmt.Errorf("failed to read chunk size: %w", err)
		}
		chunkSize := binary.LittleEndian.Uint32(header[:])

		// If header is zero, then finish.
		if chunkSize == 0 {
			// Read the final total.
			if _, err := io.ReadFull(stream, header[:]); err != nil {
				return totalRead, fmt.Errorf("failed to read final total: %w", err)
			}
			finalTotal := binary.LittleEndian.Uint32(header[:])
			if int(finalTotal) != totalRead {
				return totalRead, fmt.Errorf(
					"data length mismatch: expected %d, got %d",
					finalTotal, totalRead,
				)
			}
			break
		}

		// Check buffer size to avoid overflow.
		if totalRead+int(chunkSize) > len(buffer) {
			return totalRead, fmt.Errorf(
				"buffer overflow: need %d, have %d",
				totalRead+int(chunkSize), len(buffer),
			)
		}

		// Read the chunk data directly.
		n, err := io.ReadFull(stream, buffer[totalRead:totalRead+int(chunkSize)])
		if err != nil {
			return totalRead, fmt.Errorf("failed to read chunk data: %w", err)
		}
		totalRead += n
	}

	return totalRead, nil
}
