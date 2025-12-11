package binarystream

import (
	"encoding/binary"
	"fmt"
	"io"
	"sync"

	"github.com/pbs-plus/pbs-plus/internal/utils"
	"github.com/xtaci/smux"
)

var bufferPool = &sync.Pool{
	New: func() interface{} {
		return make([]byte, utils.MaxStreamBuffer)
	},
}

func SendDataFromReader(r io.Reader, length int, stream *smux.Stream) error {
	if stream == nil {
		return fmt.Errorf("stream is nil")
	}

	if err := binary.Write(stream, binary.LittleEndian, uint64(length)); err != nil {
		return fmt.Errorf("failed to write total length prefix: %w", err)
	}

	if length == 0 || r == nil {
		if err := binary.Write(stream, binary.LittleEndian, uint32(0)); err != nil {
			return fmt.Errorf("failed to write sentinel for zero length: %w", err)
		}
		return nil
	}

	chunkBuf := bufferPool.Get().([]byte)
	chunkBuf = chunkBuf[:utils.MaxStreamBuffer]
	defer bufferPool.Put(chunkBuf)

	totalSent := 0

	for totalSent < length {
		remaining := length - totalSent
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

		if err := binary.Write(stream, binary.LittleEndian, uint32(n)); err != nil {
			return fmt.Errorf("failed to write chunk size prefix: %w", err)
		}

		written := 0
		for written < n {
			nw, err := stream.Write(chunkBuf[written:n])
			if err != nil {
				return fmt.Errorf("failed to write chunk data: %w", err)
			}
			written += nw
		}

		totalSent += n
	}

	if err := binary.Write(stream, binary.LittleEndian, uint32(0)); err != nil {
		return fmt.Errorf("failed to write sentinel: %w", err)
	}

	return nil
}

func ReceiveDataInto(stream *smux.Stream, dst []byte) (int, error) {
	var totalLength uint64
	if err := binary.Read(stream, binary.LittleEndian, &totalLength); err != nil {
		if err == io.EOF || err == io.ErrUnexpectedEOF {
			return 0, fmt.Errorf("failed to read total length prefix (EOF/UnexpectedEOF): %w", err)
		}
		return 0, fmt.Errorf("failed to read total length prefix: %w", err)
	}

	const maxLength = 1 << 30
	if totalLength > maxLength {
		return 0, fmt.Errorf("declared total length %d exceeds maximum allowed %d", totalLength, maxLength)
	}

	if int(totalLength) > len(dst) {
		return 0, fmt.Errorf("destination buffer too small: need %d bytes, have %d", totalLength, len(dst))
	}

	totalRead := 0

	for {
		var chunkSize uint32
		if err := binary.Read(stream, binary.LittleEndian, &chunkSize); err != nil {
			if totalLength == 0 && totalRead == 0 && (err == io.EOF || err == io.ErrUnexpectedEOF) {
				return totalRead, nil
			}
			if err == io.EOF || err == io.ErrUnexpectedEOF {
				return totalRead, fmt.Errorf("failed to read chunk size (EOF/UnexpectedEOF before completion): expected %d, got %d: %w", totalLength, totalRead, err)
			}
			return totalRead, fmt.Errorf("failed to read chunk size: %w", err)
		}

		if chunkSize == 0 {
			if uint64(totalRead) != totalLength {
				return totalRead, fmt.Errorf("incomplete payload: declared %d, received %d", totalLength, totalRead)
			}
			break
		}

		chunkLen := int(chunkSize)
		expectedEnd := totalRead + chunkLen

		if expectedEnd > int(totalLength) {
			_, _ = io.CopyN(io.Discard, stream, int64(chunkLen))
			return totalRead, fmt.Errorf("received chunk overflows declared total length: total %d, current %d, chunk %d", totalLength, totalRead, chunkLen)
		}

		n, err := io.ReadFull(stream, dst[totalRead:expectedEnd])
		totalRead += n
		if err != nil {
			if err == io.ErrUnexpectedEOF {
				err = fmt.Errorf("unexpected EOF reading chunk data: expected %d bytes for chunk, got %d: %w", chunkLen, n, err)
			} else {
				err = fmt.Errorf("failed to read chunk data: %w", err)
			}
			_ = stream.Close()
			return totalRead, err
		}
	}

	return totalRead, nil
}
