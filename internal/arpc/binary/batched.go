package binarystream

import (
	"encoding/binary"
	"fmt"
	"io"

	"github.com/pbs-plus/pbs-plus/internal/utils"
	"github.com/xtaci/smux"
)

func SendDataFromReader(r io.Reader, length int, stream *smux.Stream) error {
	if stream == nil {
		return fmt.Errorf("stream is nil")
	}
	if r == nil || length == 0 {
		if err := binary.Write(stream, binary.LittleEndian, uint32(0)); err != nil {
			return fmt.Errorf("failed to write batch count: %w", err)
		}
		return nil
	}

	// Calculate number of chunks
	chunkSize := utils.MaxStreamBuffer
	numChunks := (length + chunkSize - 1) / chunkSize

	// Write batch count
	if err := binary.Write(stream, binary.LittleEndian, uint32(numChunks)); err != nil {
		return fmt.Errorf("failed to write batch count: %w", err)
	}

	// Write all chunk sizes
	sizes := make([]uint32, numChunks)
	remaining := length
	for i := 0; i < numChunks; i++ {
		thisChunk := chunkSize
		if remaining < chunkSize {
			thisChunk = remaining
		}
		sizes[i] = uint32(thisChunk)
		remaining -= thisChunk
	}
	for _, sz := range sizes {
		if err := binary.Write(stream, binary.LittleEndian, sz); err != nil {
			return fmt.Errorf("failed to write chunk size: %w", err)
		}
	}

	// Write all chunk data
	remaining = length
	for i := 0; i < numChunks; i++ {
		toRead := int(sizes[i])
		buf := bufferPool.Get().([]byte)[:toRead]
		n, err := io.ReadFull(r, buf)
		if err != nil && err != io.EOF && err != io.ErrUnexpectedEOF {
			bufferPool.Put(buf)
			return fmt.Errorf("read error: %w", err)
		}
		written := 0
		for written < n {
			nw, err := stream.Write(buf[written:n])
			if err != nil {
				bufferPool.Put(buf)
				return fmt.Errorf("failed to write chunk data: %w", err)
			}
			written += nw
		}
		bufferPool.Put(buf)
		remaining -= n
	}
	return nil
}

func ReceiveData(stream *smux.Stream) ([]byte, int, error) {
	var N uint32
	if err := binary.Read(stream, binary.LittleEndian, &N); err != nil {
		return nil, 0, fmt.Errorf("failed to read batch count: %w", err)
	}
	if N == 0 {
		return nil, 0, nil
	}
	sizes := make([]uint32, N)
	total := 0
	for i := range sizes {
		if err := binary.Read(stream, binary.LittleEndian, &sizes[i]); err != nil {
			return nil, 0, fmt.Errorf("failed to read chunk size: %w", err)
		}
		total += int(sizes[i])
	}
	buf := make([]byte, total)
	if cap(buf) < total {
		buf = make([]byte, total)
	} else {
		buf = buf[:total]
	}
	offset := 0
	for i := 0; i < int(N); i++ {
		n, err := io.ReadFull(stream, buf[offset:offset+int(sizes[i])])
		if err != nil {
			return buf[:offset+n], offset + n, fmt.Errorf("failed to read chunk data: %w", err)
		}
		offset += n
	}

	return buf, offset, nil
}
