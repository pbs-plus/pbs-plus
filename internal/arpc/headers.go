package arpc

import (
	"io"
	"net/http"

	"github.com/quic-go/quic-go"
	"github.com/quic-go/quic-go/quicvarint"
)

func writeHeadersFrame(s *quic.SendStream, hdr http.Header) error {
	w := quicvarint.NewWriter(s)
	if err := writeVarint(w, uint64(len(hdr))); err != nil {
		return err
	}
	for k, vals := range hdr {
		kb := []byte(http.CanonicalHeaderKey(k))
		if err := writeVarint(w, uint64(len(kb))); err != nil {
			return err
		}
		if _, err := s.Write(kb); err != nil {
			return err
		}
		if err := writeVarint(w, uint64(len(vals))); err != nil {
			return err
		}
		for _, v := range vals {
			vb := []byte(v)
			if err := writeVarint(w, uint64(len(vb))); err != nil {
				return err
			}
			if _, err := s.Write(vb); err != nil {
				return err
			}
		}
	}
	return nil
}

func readHeadersFrame(s *quic.ReceiveStream) (http.Header, error) {
	h := http.Header{}
	r := quicvarint.NewReader(s)
	n, err := readVarint(r)
	if err != nil {
		return nil, err
	}
	for i := uint64(0); i < n; i++ {
		kl, err := readVarint(r)
		if err != nil {
			return nil, err
		}
		kb := make([]byte, kl)
		if _, err := io.ReadFull(s, kb); err != nil {
			return nil, err
		}
		vn, err := readVarint(r)
		if err != nil {
			return nil, err
		}
		for j := uint64(0); j < vn; j++ {
			vl, err := readVarint(r)
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

func writeVarint(w quicvarint.Writer, v uint64) error {
	_, err := w.Write(quicvarint.Append(nil, v))
	return err
}

func readVarint(r quicvarint.Reader) (uint64, error) {
	return quicvarint.Read(r)
}
