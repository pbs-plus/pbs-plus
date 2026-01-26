package binarystream

import (
	"encoding/binary"
	"fmt"
	"io"

	"github.com/pbs-plus/pbs-plus/internal/syslog"
	"github.com/xtaci/smux"
)

const (
	magicV1   uint32 = 0x4E465353
	version   uint16 = 1
	maxLength        = 1 << 30
)

func SendDataFromReader(r io.Reader, length int, stream *smux.Stream) error {
	if stream == nil {
		err := fmt.Errorf("stream is nil")
		syslog.L.Error(err).WithMessage("SendDataFromReader: nil stream").Write()
		return err
	}

	if length < 0 {
		err := fmt.Errorf("length must be non-negative, got %d", length)
		syslog.L.Error(err).WithMessage("SendDataFromReader: negative length").Write()
		return err
	}

	if length > maxLength {
		err := fmt.Errorf("length %d exceeds maximum allowed %d", length, maxLength)
		syslog.L.Error(err).WithMessage("SendDataFromReader: length exceeds limit").Write()
		return err
	}

	syslog.L.Debug().WithMessage("SendDataFromReader: start").
		WithField("length", length).
		Write()

	var hdr [14]byte
	binary.LittleEndian.PutUint32(hdr[0:4], magicV1)
	binary.LittleEndian.PutUint16(hdr[4:6], version)
	binary.LittleEndian.PutUint64(hdr[6:14], uint64(length))

	if _, err := stream.Write(hdr[:]); err != nil {
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

	n, err := io.CopyN(stream, r, int64(length))
	if err != nil {
		err = fmt.Errorf("data transfer error: %w", err)
		syslog.L.Error(err).WithMessage("SendDataFromReader: data write failed").Write()
		return err
	}

	syslog.L.Debug().WithMessage("SendDataFromReader: completed").
		WithField("total_sent", n).
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

	if totalLength > uint64(len(dst)) {
		toRead := len(dst)

		if toRead > 0 {
			if _, err := io.ReadFull(stream, dst); err != nil {
				e := fmt.Errorf("failed to read payload: %w", err)
				syslog.L.Error(e).WithMessage("ReceiveDataInto: payload read failed").Write()
				return 0, e
			}
		}

		remaining := int64(totalLength) - int64(toRead)
		if remaining > 0 {
			if _, err := io.CopyN(io.Discard, stream, remaining); err != nil {
				e := fmt.Errorf("failed to drain payload remainder: %w", err)
				syslog.L.Error(e).WithMessage("ReceiveDataInto: payload drain failed").Write()
				return toRead, e
			}
		}

		syslog.L.Debug().WithMessage("ReceiveDataInto: completed").
			WithField("total_read", toRead).
			Write()
		return toRead, nil
	}

	toRead := int(totalLength)

	if toRead > 0 {
		if _, err := io.ReadFull(stream, dst[:toRead]); err != nil {
			e := fmt.Errorf("failed to read payload: %w", err)
			syslog.L.Error(e).WithMessage("ReceiveDataInto: payload read failed").Write()
			return 0, e
		}
	}

	syslog.L.Debug().WithMessage("ReceiveDataInto: completed").
		WithField("total_read", toRead).
		Write()
	return toRead, nil
}
