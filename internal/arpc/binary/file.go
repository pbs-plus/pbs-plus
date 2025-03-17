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

	if length == 0 || r == nil {
		if err := binary.Write(stream, binary.LittleEndian, uint32(0)); err != nil {
			return fmt.Errorf("failed to write sentinel: %w", err)
		}
		if err := binary.Write(stream, binary.LittleEndian, uint32(0)); err != nil {
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

		if err := binary.Write(stream, binary.LittleEndian, uint32(n)); err != nil {
			return fmt.Errorf("failed to write chunk size: %w", err)
		}

		if _, err := stream.Write(buf[:n]); err != nil {
			return fmt.Errorf("failed to write chunk data: %w", err)
		}

		totalRead += n
	}

	if err := binary.Write(stream, binary.LittleEndian, uint32(0)); err != nil {
		return fmt.Errorf("failed to write sentinel: %w", err)
	}
	if err := binary.Write(stream, binary.LittleEndian, uint32(totalRead)); err != nil {
		return fmt.Errorf("failed to write final total: %w", err)
	}

	return nil
}

func ReceiveData(stream *smux.Stream, buffer []byte) (int, error) {
	totalRead := 0

	for {
		var chunkSize uint32
		if err := binary.Read(stream, binary.LittleEndian, &chunkSize); err != nil {
			return totalRead, fmt.Errorf("failed to read chunk size: %w", err)
		}

		if chunkSize == 0 {
			var finalTotal uint32
			if err := binary.Read(stream, binary.LittleEndian, &finalTotal); err != nil {
				return totalRead, fmt.Errorf("failed to read final total: %w", err)
			}
			if int(finalTotal) != totalRead {
				return totalRead, fmt.Errorf("data length mismatch: expected %d, got %d", finalTotal, totalRead)
			}
			break
		}

		if totalRead+int(chunkSize) > len(buffer) {
			return totalRead, fmt.Errorf("buffer overflow: need %d, have %d", totalRead+int(chunkSize), len(buffer))
		}

		n, err := io.ReadFull(stream, buffer[totalRead:totalRead+int(chunkSize)])
		if err != nil {
			return totalRead, fmt.Errorf("failed to read chunk data: %w", err)
		}
		totalRead += n
	}

	return totalRead, nil
}
