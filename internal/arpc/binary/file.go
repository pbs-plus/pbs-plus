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

func OldSendDataFromReader(r io.Reader, length int, stream *smux.Stream) error {
	if stream == nil {
		return fmt.Errorf("stream is nil")
	}

	// Write the total expected length prefix (64-bit little-endian).
	if err := binary.Write(stream, binary.LittleEndian, uint64(length)); err != nil {
		return fmt.Errorf("failed to write total length prefix: %w", err)
	}

	// If length is zero, write the sentinel and return.
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
			break // Should not happen if length > 0 initially, but safe check
		}

		n, err := r.Read(chunkBuf[:readSize])
		if err != nil && err != io.EOF {
			_ = binary.Write(stream, binary.LittleEndian, uint32(0))
			return fmt.Errorf("read error: %w", err)
		}
		if n == 0 {
			// EOF reached or reader behaving unexpectedly.
			break
		}

		// Write the chunk's size prefix (32-bit little-endian).
		if err := binary.Write(stream, binary.LittleEndian, uint32(n)); err != nil {
			return fmt.Errorf("failed to write chunk size prefix: %w", err)
		}

		// Write the actual chunk data.
		written := 0
		for written < n {
			nw, err := stream.Write(chunkBuf[written:n])
			if err != nil {
				_ = binary.Write(stream, binary.LittleEndian, uint32(0))
				return fmt.Errorf("failed to write chunk data: %w", err)
			}
			written += nw
		}

		totalSent += n
	}

	// Write sentinel (0) to signal there are no more chunks.
	if err := binary.Write(stream, binary.LittleEndian, uint32(0)); err != nil {
		return fmt.Errorf("failed to write sentinel: %w", err)
	}

	return nil
}

func OldReceiveData(stream *smux.Stream) ([]byte, int, error) {
	var totalLength uint64
	if err := binary.Read(stream, binary.LittleEndian, &totalLength); err != nil {
		// Check for EOF specifically, might indicate clean closure before data
		if err == io.EOF || err == io.ErrUnexpectedEOF {
			return nil, 0, fmt.Errorf(
				"failed to read total length prefix (EOF/UnexpectedEOF): %w",
				err,
			)
		}
		return nil, 0, fmt.Errorf("failed to read total length prefix: %w", err)
	}

	// Add a reasonable maximum length check to prevent OOM attacks
	const maxLength = 1 << 30 // 1 GiB limit, adjust as needed
	if totalLength > maxLength {
		return nil, 0, fmt.Errorf(
			"declared total length %d exceeds maximum allowed %d",
			totalLength,
			maxLength,
		)
	}

	buffer := make([]byte, int(totalLength))
	totalRead := 0

	for {
		var chunkSize uint32
		if err := binary.Read(stream, binary.LittleEndian, &chunkSize); err != nil {
			// If we expected 0 bytes total and read 0 bytes, EOF after total length is okay.
			if totalLength == 0 && totalRead == 0 && (err == io.EOF || err == io.ErrUnexpectedEOF) {
				// This case means totalLength=0 was sent, and then the stream closed before the sentinel(0)
				// This might be acceptable depending on the sender logic, but indicates an incomplete transmission
				// according to the current protocol (missing sentinel).
				return buffer, totalRead, nil
			}
			// If EOF happens *before* reading the expected amount, it's an error.
			if err == io.EOF || err == io.ErrUnexpectedEOF {
				return buffer, totalRead, fmt.Errorf(
					"failed to read chunk size (EOF/UnexpectedEOF before completion): expected %d, got %d: %w",
					totalLength,
					totalRead,
					err,
				)
			}
			return buffer, totalRead, fmt.Errorf("failed to read chunk size: %w", err)
		}

		if chunkSize == 0 {
			// Sentinel found, break the loop to perform final check.
			break
		}

		chunkLen := int(chunkSize)
		expectedEnd := totalRead + chunkLen

		if expectedEnd > int(totalLength) {
			// Read the remaining unexpected data to clear the stream? Or just error out?
			// Erroring out is safer as the stream state is inconsistent.
			// Drain the specific chunk that overflows?
			_, _ = io.CopyN(io.Discard, stream, int64(chunkLen))
			return buffer, totalRead, fmt.Errorf(
				"received chunk overflows declared total length: total %d, current %d, chunk %d",
				totalLength,
				totalRead,
				chunkLen,
			)
		}

		// Ensure buffer slice is correct (should be guaranteed by allocation/reslicing)
		if expectedEnd > cap(buffer) {
			// This should not happen if allocation logic is correct
			return buffer, totalRead, fmt.Errorf(
				"internal buffer error: required capacity %d, have %d",
				expectedEnd,
				cap(buffer),
			)
		}
		// No need to reslice buffer here, as it was allocated/sliced to totalLength initially

		n, err := io.ReadFull(stream, buffer[totalRead:expectedEnd])
		// 'n' is implicitly added to totalRead outside the ReadFull call in this structure
		totalRead += n // Keep track even if ReadFull returns an error partially
		if err != nil {
			// If ReadFull fails (e.g., EOF before chunk completion), report it
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
			return buffer, totalRead, err // Return partially read buffer and error
		}
		// totalRead is now expectedEnd after successful ReadFull
	}

	return buffer[:totalRead], totalRead, nil
}
