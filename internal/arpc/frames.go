package arpc

import (
	"errors"
	"fmt"
	"io"
	"net/http"

	"github.com/fxamacker/cbor/v2"
	"github.com/xtaci/smux"
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

	data := make([]byte, length)
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
	const maxHeaderSize = 8192
	const maxHeaderCount = 50

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

	if n > 50 {
		return nil, errors.New("too many headers")
	}

	for i := uint64(0); i < n; i++ {
		kl, err := readVarint(s)
		if err != nil {
			return nil, err
		}
		kb := make([]byte, kl)
		if _, err := io.ReadFull(s, kb); err != nil {
			return nil, err
		}
		vn, err := readVarint(s)
		if err != nil {
			return nil, err
		}
		for j := uint64(0); j < vn; j++ {
			vl, err := readVarint(s)
			if err != nil {
				return nil, err
			}
			vb := make([]byte, vl)
			if _, err := io.ReadFull(s, vb); err != nil {
				return nil, err
			}
			h.Add(string(kb), string(vb))
		}
	}
	return h, nil
}
