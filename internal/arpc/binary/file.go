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
	New: func() any {
		return make([]byte, utils.MaxStreamBuffer)
	},
}

const (
	magicV1   uint32 = 0x4E465353
	version   uint16 = 1
	maxLength        = 1 << 30
)

func writeFull(w io.Writer, buf []byte) error {
	total := 0
	for total < len(buf) {
		n, err := w.Write(buf[total:])
		total += n
		if err != nil {
			return err
		}
		if n == 0 && total < len(buf) {
			return io.ErrShortWrite
		}
	}
	return nil
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

	var hdr [14]byte
	binary.LittleEndian.PutUint32(hdr[0:4], magicV1)
	binary.LittleEndian.PutUint16(hdr[4:6], version)
	binary.LittleEndian.PutUint64(hdr[6:14], uint64(length))
	if err := writeFull(stream, hdr[:]); err != nil {
		err = fmt.Errorf("failed to write header: %w", err)
		syslog.L.Error(err).WithMessage("SendDataFromReader: header write failed").Write()
		return err
	}

	if length == 0 || r == nil {
		if r == nil {
			syslog.L.Warn().WithMessage("SendDataFromReader: reader is nil, sending zero-length").Write()
		}
		syslog.L.Debug().WithMessage("SendDataFromReader: completed zero-length send").Write()
		return nil
	}

	buf := bufferPool.Get().([]byte)
	defer bufferPool.Put(buf)

	remaining := length

	for remaining > 0 {
		readSize := min(len(buf), remaining)
		n, err := r.Read(buf[:readSize])
		if err != nil && err != io.EOF {
			err = fmt.Errorf("read error: %w", err)
			syslog.L.Error(err).WithMessage("SendDataFromReader: read failed").Write()
			return err
		}
		if n == 0 {
			break
		}
		if err := writeFull(stream, buf[:n]); err != nil {
			err = fmt.Errorf("failed to write data: %w", err)
			syslog.L.Error(err).WithMessage("SendDataFromReader: data write failed").Write()
			return err
		}
		remaining -= n
	}

	if remaining != 0 {
		err := fmt.Errorf("source ended before sending declared length: remaining=%d", remaining)
		syslog.L.Error(err).WithMessage("SendDataFromReader: premature EOF").Write()
		return err
	}

	syslog.L.Debug().WithMessage("SendDataFromReader: completed").
		WithField("total_sent", length).
		Write()
	return nil
}

func ReceiveDataInto(stream *smux.Stream, dst []byte) (int, error) {
	var hdr [14]byte
	if _, err := io.ReadFull(stream, hdr[:]); err != nil {
		wErr := fmt.Errorf("failed to read header: %w", err)
		syslog.L.Error(wErr).WithMessage("ReceiveDataInto: header read failed").Write()
		return 0, wErr
	}

	if binary.LittleEndian.Uint32(hdr[0:4]) != magicV1 {
		err := fmt.Errorf("invalid magic")
		syslog.L.Error(err).WithMessage("ReceiveDataInto: invalid magic").Write()
		return 0, err
	}
	if binary.LittleEndian.Uint16(hdr[4:6]) != version {
		err := fmt.Errorf("unsupported version")
		syslog.L.Error(err).WithMessage("ReceiveDataInto: unsupported version").Write()
		return 0, err
	}

	totalLength := binary.LittleEndian.Uint64(hdr[6:14])

	syslog.L.Debug().WithMessage("ReceiveDataInto: start").
		WithField("declared_length", totalLength).
		WithField("dst_capacity", len(dst)).
		Write()

	if totalLength > maxLength {
		err := fmt.Errorf("declared total length %d exceeds maximum allowed %d", totalLength, maxLength)
		syslog.L.Error(err).WithMessage("ReceiveDataInto: length exceeds limit").Write()
		return 0, err
	}

	totalRead := 0
	dstCap := len(dst)
	toRead := int(totalLength)
	if toRead > dstCap {
		toRead = dstCap
	}

	buf := bufferPool.Get().([]byte)
	defer bufferPool.Put(buf)

	for totalRead < toRead {
		want := min(len(buf), toRead-totalRead)
		if _, err := io.ReadFull(stream, buf[:want]); err != nil {
			e := fmt.Errorf("failed to read payload: %w", err)
			syslog.L.Error(e).WithMessage("ReceiveDataInto: payload read failed").Write()
			return totalRead, e
		}
		copy(dst[totalRead:totalRead+want], buf[:want])
		totalRead += want
	}

	remain := int(totalLength) - totalRead
	for remain > 0 {
		want := min(len(buf), remain)
		if _, err := io.ReadFull(stream, buf[:want]); err != nil {
			e := fmt.Errorf("failed to drain payload remainder: %w", err)
			syslog.L.Error(e).WithMessage("ReceiveDataInto: payload drain failed").Write()
			return totalRead, e
		}
		remain -= want
	}

	syslog.L.Debug().WithMessage("ReceiveDataInto: completed").
		WithField("total_read", totalRead).
		Write()
	return totalRead, nil
}
