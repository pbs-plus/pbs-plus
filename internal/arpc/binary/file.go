package binarystream

import (
	"encoding/binary"
	"fmt"
	"io"
	"net"
	"sync"

	"github.com/pbs-plus/pbs-plus/internal/utils"
)

var bufferPool = &sync.Pool{
	New: func() interface{} {
		return make([]byte, utils.MaxStreamBuffer)
	},
}

// SendDataFromReader sends data from a reader over a connection with length prefixes
func SendDataFromReader(r io.Reader, length int, conn net.Conn) error {
	if conn == nil {
		return fmt.Errorf("connection is nil")
	}

	// Write the total expected length prefix (64-bit little-endian)
	if err := binary.Write(conn, binary.LittleEndian, uint64(length)); err != nil {
		return fmt.Errorf("failed to write total length prefix: %w", err)
	}

	// If length is zero, write the sentinel and return
	if length == 0 || r == nil {
		if err := binary.Write(conn, binary.LittleEndian, uint32(0)); err != nil {
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
			_ = binary.Write(conn, binary.LittleEndian, uint32(0))
			return fmt.Errorf("read error: %w", err)
		}
		if n == 0 {
			break
		}

		// Write the chunk's size prefix (32-bit little-endian)
		if err := binary.Write(conn, binary.LittleEndian, uint32(n)); err != nil {
			return fmt.Errorf("failed to write chunk size prefix: %w", err)
		}

		// Write the actual chunk data
		written := 0
		for written < n {
			nw, err := conn.Write(chunkBuf[written:n])
			if err != nil {
				_ = binary.Write(conn, binary.LittleEndian, uint32(0))
				return fmt.Errorf("failed to write chunk data: %w", err)
			}
			written += nw
		}

		totalSent += n
	}

	// Write sentinel (0) to signal there are no more chunks
	if err := binary.Write(conn, binary.LittleEndian, uint32(0)); err != nil {
		return fmt.Errorf("failed to write sentinel: %w", err)
	}

	return nil
}

// ReceiveDataInto receives chunked data from a connection into a destination buffer
func ReceiveDataInto(conn net.Conn, dst []byte) (int, error) {
	var totalLength uint64
	if err := binary.Read(conn, binary.LittleEndian, &totalLength); err != nil {
		if err == io.EOF || err == io.ErrUnexpectedEOF {
			return 0, fmt.Errorf("failed to read total length prefix (EOF/UnexpectedEOF): %w", err)
		}
		return 0, fmt.Errorf("failed to read total length prefix: %w", err)
	}

	// Add a reasonable maximum length check to prevent OOM attacks
	const maxLength = 1 << 30 // 1 GiB limit, adjust as needed
	if totalLength > maxLength {
		return 0, fmt.Errorf(
			"declared total length %d exceeds maximum allowed %d",
			totalLength,
			maxLength,
		)
	}

	// Check if destination buffer is large enough
	if int(totalLength) > len(dst) {
		return 0, fmt.Errorf(
			"destination buffer too small: need %d bytes, have %d",
			totalLength,
			len(dst),
		)
	}

	totalRead := 0

	for {
		var chunkSize uint32
		if err := binary.Read(conn, binary.LittleEndian, &chunkSize); err != nil {
			if totalLength == 0 && totalRead == 0 && (err == io.EOF || err == io.ErrUnexpectedEOF) {
				return totalRead, nil
			}
			if err == io.EOF || err == io.ErrUnexpectedEOF {
				return totalRead, fmt.Errorf(
					"failed to read chunk size (EOF/UnexpectedEOF before completion): expected %d, got %d: %w",
					totalLength,
					totalRead,
					err,
				)
			}
			return totalRead, fmt.Errorf("failed to read chunk size: %w", err)
		}

		if chunkSize == 0 {
			// Sentinel found, break the loop
			break
		}

		chunkLen := int(chunkSize)
		expectedEnd := totalRead + chunkLen

		if expectedEnd > int(totalLength) {
			_, _ = io.CopyN(io.Discard, conn, int64(chunkLen))
			return totalRead, fmt.Errorf(
				"received chunk overflows declared total length: total %d, current %d, chunk %d",
				totalLength,
				totalRead,
				chunkLen,
			)
		}

		// Read directly into the destination buffer
		n, err := io.ReadFull(conn, dst[totalRead:expectedEnd])
		totalRead += n
		if err != nil {
			if err == io.ErrUnexpectedEOF {
				err = fmt.Errorf(
					"unexpected EOF reading chunk data: expected %d bytes for chunk, got %d: %w",
					chunkLen,
					n,
					err,
				)
			} else {
				err = fmt.Errorf("failed to read chunk data: %w", err)
			}
			return totalRead, err
		}
	}

	return totalRead, nil
}
