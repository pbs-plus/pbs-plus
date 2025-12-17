package arpc

import (
	"encoding/binary"
	"io"
	"net/http"

	"github.com/xtaci/smux"
)

func writeHeadersFrame(s *smux.Stream, hdr http.Header) error {
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

func writeVarint(w io.Writer, v uint64) error {
	var buf [binary.MaxVarintLen64]byte
	n := binary.PutUvarint(buf[:], v)
	_, err := w.Write(buf[:n])
	return err
}

func readVarint(r io.Reader) (uint64, error) {
	return binary.ReadUvarint(&byteReader{r: r})
}

type byteReader struct {
	r io.Reader
}

func (b *byteReader) ReadByte() (byte, error) {
	var buf [1]byte
	_, err := b.r.Read(buf[:])
	return buf[0], err
}
