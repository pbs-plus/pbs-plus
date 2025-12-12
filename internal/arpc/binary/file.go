package binarystream

import (
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"io"
	"sync"

	"github.com/pbs-plus/pbs-plus/internal/syslog"
	"github.com/pbs-plus/pbs-plus/internal/utils"
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

func writeFull(w io.Writer, b []byte) error {
	for len(b) > 0 {
		n, err := w.Write(b)
		if err != nil {
			return err
		}
		b = b[n:]
	}
	return nil
}

func readFull(r io.Reader, b []byte) error {
	_, err := io.ReadFull(r, b)
	return err
}

func SendDataFromReader(r io.Reader, length int, stream io.Writer) error {
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
		var zeroCRC [4]byte
		if err := writeFull(stream, zeroCRC[:]); err != nil {
			err = fmt.Errorf("failed to write zero-length crc: %w", err)
			syslog.L.Error(err).WithMessage("SendDataFromReader: zero-length crc write failed").Write()
			return err
		}
		syslog.L.Debug().WithMessage("SendDataFromReader: completed zero-length send").Write()
		return nil
	}

	buf := bufferPool.Get().([]byte)
	defer bufferPool.Put(buf)

	remaining := length
	crc := crc32.NewIEEE()

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
		_, _ = crc.Write(buf[:n])
		remaining -= n
	}

	if remaining != 0 {
		err := fmt.Errorf("source ended before sending declared length: remaining=%d", remaining)
		syslog.L.Error(err).WithMessage("SendDataFromReader: premature EOF").Write()
		return err
	}

	var crcBuf [4]byte
	binary.LittleEndian.PutUint32(crcBuf[:], crc.Sum32())
	if err := writeFull(stream, crcBuf[:]); err != nil {
		err = fmt.Errorf("failed to write final crc: %w", err)
		syslog.L.Error(err).WithMessage("SendDataFromReader: crc write failed").Write()
		return err
	}

	syslog.L.Debug().WithMessage("SendDataFromReader: completed").
		WithField("total_sent", length).
		Write()
	return nil
}

func ReceiveDataInto(stream io.Reader, dst []byte) (int, error) {
	var hdr [14]byte
	if err := readFull(stream, hdr[:]); err != nil {
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

	crc := crc32.NewIEEE()

	for totalRead < toRead {
		want := min(len(buf), toRead-totalRead)
		if err := readFull(stream, buf[:want]); err != nil {
			e := fmt.Errorf("failed to read payload: %w", err)
			syslog.L.Error(e).WithMessage("ReceiveDataInto: payload read failed").Write()
			return totalRead, e
		}
		copy(dst[totalRead:totalRead+want], buf[:want])
		_, _ = crc.Write(buf[:want])
		totalRead += want
	}

	remain := int(totalLength) - totalRead
	for remain > 0 {
		want := min(len(buf), remain)
		if err := readFull(stream, buf[:want]); err != nil {
			e := fmt.Errorf("failed to drain payload remainder: %w", err)
			syslog.L.Error(e).WithMessage("ReceiveDataInto: payload drain failed").Write()
			return totalRead, e
		}
		_, _ = crc.Write(buf[:want])
		remain -= want
	}

	var recvCRC [4]byte
	if err := readFull(stream, recvCRC[:]); err != nil {
		e := fmt.Errorf("failed to read final crc: %w", err)
		syslog.L.Error(e).WithMessage("ReceiveDataInto: crc read failed").Write()
		return totalRead, e
	}

	if binary.LittleEndian.Uint32(recvCRC[:]) != crc.Sum32() {
		err := fmt.Errorf("crc mismatch for payload")
		syslog.L.Error(err).WithMessage("ReceiveDataInto: crc mismatch").Write()
		return totalRead, err
	}

	syslog.L.Debug().WithMessage("ReceiveDataInto: completed").
		WithField("total_read", totalRead).
		Write()
	return totalRead, nil
}
