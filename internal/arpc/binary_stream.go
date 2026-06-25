package arpc

import (
	"encoding/binary"
	"fmt"
	"io"

	"github.com/pbs-plus/pbs-plus/internal/log"
)

const (
	magicV1   uint32 = 0x4E465353
	version   uint16 = 1
	maxLength        = 1 << 30
)

func SendDataFromReader(r io.Reader, length int, stream ARPCStream) error {
	if stream == nil {
		err := fmt.Errorf("stream is nil")
		log.Error(err, "SendDataFromReader: nil stream")
		return err
	}

	if length < 0 {
		err := fmt.Errorf("length must be non-negative, got %d", length)
		log.Error(err, "SendDataFromReader: negative length")
		return err
	}

	if length > maxLength {
		err := fmt.Errorf("length %d exceeds maximum allowed %d", length, maxLength)
		log.Error(err, "SendDataFromReader: length exceeds limit")
		return err
	}

	var hdr [14]byte
	binary.LittleEndian.PutUint32(hdr[0:4], magicV1)
	binary.LittleEndian.PutUint16(hdr[4:6], version)
	binary.LittleEndian.PutUint64(hdr[6:14], uint64(length))

	if _, err := stream.Write(hdr[:]); err != nil {
		err = fmt.Errorf("failed to write header: %w", err)
		log.Error(err, "SendDataFromReader: header write failed")
		return err
	}

	if length == 0 || r == nil {
		if r == nil {
			log.Warn("SendDataFromReader: reader is nil, sending zero-length")
		}
		return nil
	}

	if _, err := io.CopyN(stream, r, int64(length)); err != nil {
		err = fmt.Errorf("data transfer error: %w", err)
		log.Error(err, "SendDataFromReader: data write failed")
		return err
	}

	return nil
}

func ReceiveDataInto(stream ARPCStream, dst []byte) (int, error) {
	var hdr [14]byte
	if _, err := io.ReadFull(stream, hdr[:]); err != nil {
		wErr := fmt.Errorf("failed to read header: %w", err)
		log.Error(wErr, "ReceiveDataInto: header read failed")
		return 0, wErr
	}

	if binary.LittleEndian.Uint32(hdr[0:4]) != magicV1 {
		err := fmt.Errorf("invalid magic")
		log.Error(err, "ReceiveDataInto: invalid magic")
		return 0, err
	}
	if binary.LittleEndian.Uint16(hdr[4:6]) != version {
		err := fmt.Errorf("unsupported version")
		log.Error(err, "ReceiveDataInto: unsupported version")
		return 0, err
	}

	totalLength := binary.LittleEndian.Uint64(hdr[6:14])

	if totalLength > maxLength {
		err := fmt.Errorf("declared total length %d exceeds maximum allowed %d", totalLength, maxLength)
		log.Error(err, "ReceiveDataInto: length exceeds limit")
		return 0, err
	}

	if totalLength > uint64(len(dst)) {
		toRead := len(dst)

		if toRead > 0 {
			if _, err := io.ReadFull(stream, dst); err != nil {
				e := fmt.Errorf("failed to read payload: %w", err)
				log.Error(e, "ReceiveDataInto: payload read failed")
				return 0, e
			}
		}

		remaining := int64(totalLength) - int64(toRead)
		if remaining > 0 {
			if _, err := io.CopyN(io.Discard, stream, remaining); err != nil {
				e := fmt.Errorf("failed to drain payload remainder: %w", err)
				log.Error(e, "ReceiveDataInto: payload drain failed")
				return toRead, e
			}
		}

		return toRead, nil
	}

	toRead := int(totalLength)

	if toRead > 0 {
		if _, err := io.ReadFull(stream, dst[:toRead]); err != nil {
			e := fmt.Errorf("failed to read payload: %w", err)
			log.Error(e, "ReceiveDataInto: payload read failed")
			return 0, e
		}
	}

	return toRead, nil
}
