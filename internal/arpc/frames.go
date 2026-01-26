package arpc

import (
	"errors"
	"fmt"
	"io"
	"net/http"

	"github.com/fxamacker/cbor/v2"
	"github.com/xtaci/smux"
)

const (
	maxHeaderSize       = 8192
	maxHeaderCount      = 50
	maxRejectionSize    = 4096
	maxSingleHeaderSize = 2048
)

type RejectionFrame struct {
	Message string `cbor:"message"`
	Code    int    `cbor:"code"`
}

func (r RejectionFrame) Error() string {
	return fmt.Sprintf("connection rejected: %s (code %d)", r.Message, r.Code)
}

func writeRejectionFrame(s *smux.Stream, rejection RejectionFrame) error {
	// Send marker byte to indicate rejection (0x00 = rejection, 0x01 = success)
	if _, err := s.Write([]byte{0x00}); err != nil {
		return err
	}

	data, err := cbor.Marshal(rejection)
	if err != nil {
		return err
	}

	if len(data) > maxRejectionSize {
		return fmt.Errorf("rejection frame too large: %d > %d", len(data), maxRejectionSize)
	}

	if err := writeVarint(s, uint64(len(data))); err != nil {
		return err
	}

	_, err = s.Write(data)
	return err
}

func readRejectionFrame(s *smux.Stream) (RejectionFrame, error) {
	var rejection RejectionFrame

	length, err := readVarint(s)
	if err != nil {
		return rejection, err
	}

	if length > maxRejectionSize {
		return rejection, fmt.Errorf("rejection frame too large: %d > %d", length, maxRejectionSize)
	}

	dataLen := int(length)
	data := make([]byte, dataLen)
	if _, err := io.ReadFull(s, data); err != nil {
		return rejection, err
	}

	err = cbor.Unmarshal(data, &rejection)
	return rejection, err
}

func writeHeadersSuccess(s *smux.Stream) error {
	_, err := s.Write([]byte{0x01})
	return err
}

func readHandshakeResponse(s *smux.Stream) error {
	var marker [1]byte
	if _, err := io.ReadFull(s, marker[:]); err != nil {
		return fmt.Errorf("failed to read handshake response: %w", err)
	}

	switch marker[0] {
	case 0x00: // Rejection
		rejection, err := readRejectionFrame(s)
		if err != nil {
			return fmt.Errorf("connection rejected (failed to parse rejection: %w)", err)
		}
		return rejection
	case 0x01: // Success
		return nil
	default:
		return fmt.Errorf("invalid handshake response marker: 0x%02X", marker[0])
	}
}

func validateHeaders(headers http.Header) error {
	if len(headers) > maxHeaderCount {
		return fmt.Errorf("too many headers: %d > %d", len(headers), maxHeaderCount)
	}

	totalSize := 0
	for k, vals := range headers {
		totalSize += len(k)
		for _, v := range vals {
			totalSize += len(v)
		}
		if totalSize > maxHeaderSize {
			return errors.New("headers exceed size limit")
		}
	}
	return nil
}

func writeHeadersFrame(s *smux.Stream, hdr http.Header) error {
	if err := validateHeaders(hdr); err != nil {
		return err
	}

	if err := writeVarint(s, uint64(len(hdr))); err != nil {
		return err
	}
	for k, vals := range hdr {
		kb := []byte(http.CanonicalHeaderKey(k))
		if err := writeVarint(s, uint64(len(kb))); err != nil {
			return err
		}
		if _, err := s.Write(kb); err != nil {
			return err
		}
		if err := writeVarint(s, uint64(len(vals))); err != nil {
			return err
		}
		for _, v := range vals {
			vb := []byte(v)
			if err := writeVarint(s, uint64(len(vb))); err != nil {
				return err
			}
			if _, err := s.Write(vb); err != nil {
				return err
			}
		}
	}
	return nil
}

func readHeadersFrame(s *smux.Stream) (http.Header, error) {
	h := http.Header{}
	n, err := readVarint(s)
	if err != nil {
		return nil, err
	}

	if n > maxHeaderCount {
		return nil, fmt.Errorf("too many headers: %d > %d", n, maxHeaderCount)
	}

	totalSize := 0

	for i := uint64(0); i < n; i++ {
		kl, err := readVarint(s)
		if err != nil {
			return nil, err
		}

		if kl > maxSingleHeaderSize {
			return nil, fmt.Errorf("header key too large: %d > %d", kl, maxSingleHeaderSize)
		}

		keyLen := int(kl)
		kb := make([]byte, keyLen)
		if _, err := io.ReadFull(s, kb); err != nil {
			return nil, err
		}

		totalSize += keyLen
		if totalSize > maxHeaderSize {
			return nil, errors.New("headers exceed size limit")
		}

		vn, err := readVarint(s)
		if err != nil {
			return nil, err
		}

		if vn > maxHeaderCount {
			return nil, fmt.Errorf("too many values for header: %d > %d", vn, maxHeaderCount)
		}

		for j := uint64(0); j < vn; j++ {
			vl, err := readVarint(s)
			if err != nil {
				return nil, err
			}

			if vl > maxSingleHeaderSize {
				return nil, fmt.Errorf("header value too large: %d > %d", vl, maxSingleHeaderSize)
			}

			valLen := int(vl)
			vb := make([]byte, valLen)
			if _, err := io.ReadFull(s, vb); err != nil {
				return nil, err
			}

			totalSize += valLen
			if totalSize > maxHeaderSize {
				return nil, errors.New("headers exceed size limit")
			}

			h.Add(string(kb), string(vb))
		}
	}
	return h, nil
}
