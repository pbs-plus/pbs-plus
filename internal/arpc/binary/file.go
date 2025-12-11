package binarystream

import (
	"encoding/binary"
	"fmt"
	"io"
	"sync"

	"github.com/pbs-plus/pbs-plus/internal/syslog"
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
		err := fmt.Errorf("stream is nil")
		syslog.L.Error(err).WithMessage("SendDataFromReader: nil stream").Write()
		return err
	}

	syslog.L.Debug().WithMessage("SendDataFromReader: start").
		WithField("length", length).
		Write()

	if err := binary.Write(stream, binary.LittleEndian, uint64(length)); err != nil {
		err = fmt.Errorf("failed to write total length prefix: %w", err)
		syslog.L.Error(err).WithMessage("SendDataFromReader: total length write failed").Write()
		return err
	}

	if length == 0 || r == nil {
		if r == nil {
			syslog.L.Warn().WithMessage("SendDataFromReader: reader is nil, sending zero-length").Write()
		}
		if err := binary.Write(stream, binary.LittleEndian, uint32(0)); err != nil {
			err = fmt.Errorf("failed to write sentinel for zero length: %w", err)
			syslog.L.Error(err).WithMessage("SendDataFromReader: zero-length sentinel write failed").Write()
			return err
		}
		syslog.L.Debug().WithMessage("SendDataFromReader: completed zero-length send").Write()
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
			syslog.L.Warn().WithMessage("SendDataFromReader: non-positive readSize encountered").Write()
			break
		}

		n, err := r.Read(chunkBuf[:readSize])
		if err != nil && err != io.EOF {
			_ = binary.Write(stream, binary.LittleEndian, uint32(0))
			err = fmt.Errorf("read error: %w", err)
			syslog.L.Error(err).WithMessage("SendDataFromReader: read failed").WithField("sent_bytes", totalSent).Write()
			return err
		}
		if n == 0 {
			syslog.L.Debug().WithMessage("SendDataFromReader: EOF or zero bytes read, ending").WithField("sent_bytes", totalSent).Write()
			break
		}

		if err := binary.Write(stream, binary.LittleEndian, uint32(n)); err != nil {
			err = fmt.Errorf("failed to write chunk size prefix: %w", err)
			syslog.L.Error(err).WithMessage("SendDataFromReader: chunk size write failed").Write()
			return err
		}

		written := 0
		for written < n {
			nw, err := stream.Write(chunkBuf[written:n])
			if err != nil {
				_ = binary.Write(stream, binary.LittleEndian, uint32(0))
				err = fmt.Errorf("failed to write chunk data: %w", err)
				syslog.L.Error(err).WithMessage("SendDataFromReader: chunk write failed").WithField("written", written).WithField("chunk", n).Write()
				return err
			}
			written += nw
		}

		totalSent += n
	}

	if err := binary.Write(stream, binary.LittleEndian, uint32(0)); err != nil {
		err = fmt.Errorf("failed to write sentinel: %w", err)
		syslog.L.Error(err).WithMessage("SendDataFromReader: sentinel write failed").Write()
		return err
	}

	syslog.L.Debug().WithMessage("SendDataFromReader: completed").
		WithField("total_sent", totalSent).
		Write()
	return nil
}

func ReceiveDataInto(stream *smux.Stream, dst []byte) (int, error) {
	var totalLength uint64
	if err := binary.Read(stream, binary.LittleEndian, &totalLength); err != nil {
		if err == io.EOF || err == io.ErrUnexpectedEOF {
			wErr := fmt.Errorf("failed to read total length prefix (EOF/UnexpectedEOF): %w", err)
			syslog.L.Error(wErr).WithMessage("ReceiveDataInto: total length read EOF").Write()
			return 0, wErr
		}
		wErr := fmt.Errorf("failed to read total length prefix: %w", err)
		syslog.L.Error(wErr).WithMessage("ReceiveDataInto: total length read failed").Write()
		return 0, wErr
	}

	syslog.L.Debug().WithMessage("ReceiveDataInto: start").
		WithField("declared_length", totalLength).
		WithField("dst_capacity", len(dst)).
		Write()

	const maxLength = 1 << 30
	if totalLength > maxLength {
		err := fmt.Errorf("declared total length %d exceeds maximum allowed %d", totalLength, maxLength)
		syslog.L.Error(err).WithMessage("ReceiveDataInto: length exceeds limit").Write()
		return 0, err
	}

	if int(totalLength) > len(dst) {
		err := fmt.Errorf("destination buffer too small: need %d bytes, have %d", totalLength, len(dst))
		syslog.L.Error(err).WithMessage("ReceiveDataInto: destination too small").Write()
		return 0, err
	}

	totalRead := 0

	for {
		var chunkSize uint32
		if err := binary.Read(stream, binary.LittleEndian, &chunkSize); err != nil {
			if totalLength == 0 && totalRead == 0 && (err == io.EOF || err == io.ErrUnexpectedEOF) {
				syslog.L.Debug().WithMessage("ReceiveDataInto: zero-length with early EOF, accepting").Write()
				return totalRead, nil
			}
			if err == io.EOF || err == io.ErrUnexpectedEOF {
				wErr := fmt.Errorf(
					"failed to read chunk size (EOF/UnexpectedEOF before completion): expected %d, got %d: %w",
					totalLength,
					totalRead,
					err,
				)
				syslog.L.Error(wErr).WithMessage("ReceiveDataInto: chunk size read EOF").Write()
				return totalRead, wErr
			}
			wErr := fmt.Errorf("failed to read chunk size: %w", err)
			syslog.L.Error(wErr).WithMessage("ReceiveDataInto: chunk size read failed").Write()
			return totalRead, wErr
		}

		if chunkSize == 0 {
			syslog.L.Debug().WithMessage("ReceiveDataInto: sentinel received, finishing").
				WithField("total_read", totalRead).
				Write()
			break
		}

		chunkLen := int(chunkSize)
		expectedEnd := totalRead + chunkLen

		if expectedEnd > int(totalLength) {
			_, _ = io.CopyN(io.Discard, stream, int64(chunkLen))
			err := fmt.Errorf(
				"received chunk overflows declared total length: total %d, current %d, chunk %d",
				totalLength,
				totalRead,
				chunkLen,
			)
			syslog.L.Error(err).WithMessage("ReceiveDataInto: chunk overflow").Write()
			return totalRead, err
		}

		n, err := io.ReadFull(stream, dst[totalRead:expectedEnd])
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
			syslog.L.Error(err).WithMessage("ReceiveDataInto: chunk data read failed").
				WithField("read_in_chunk", n).
				WithField("expected_chunk", chunkLen).
				Write()
			return totalRead, err
		}
	}

	syslog.L.Debug().WithMessage("ReceiveDataInto: completed").
		WithField("total_read", totalRead).
		Write()
	return totalRead, nil
}
