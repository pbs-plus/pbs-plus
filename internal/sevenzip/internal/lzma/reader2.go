package lzma

import (
	"fmt"
	"io"
)

// chunk types per the LZMA2 chunk state machine, ported from upstream.
const (
	cEOS = iota
	cUD
	cU
	cL
	cLR
	cLRN
	cLRND
)

var (
	hEOS  = 0
	hUD   = 1
	hU    = 2
	hL    = 1 << 7
	hLR   = 1<<7 | 1<<5
	hLRN  = 1<<7 | 1<<6
	hLRND = 1<<7 | 1<<6 | 1<<5
)

func headerChunkType(h byte) (int, error) {
	if h&byte(hL) == 0 {
		switch int(h) {
		case hEOS:
			return cEOS, nil
		case hUD:
			return cUD, nil
		case hU:
			return cU, nil
		}
		return 0, errChunkType
	}
	switch int(h) & hLRND {
	case hL:
		return cL, nil
	case hLR:
		return cLR, nil
	case hLRN:
		return cLRN, nil
	case hLRND:
		return cLRND, nil
	}
	return 0, errChunkType
}

// chunkStateNext validates chunk order: first chunk must reset, and the
// stream ends only on cEOS.
func chunkStateNext(state byte, ctype int) (byte, error) {
	switch state {
	case 'S':
		switch ctype {
		case cEOS:
			return 'T', nil
		case cUD:
			return 'R', nil
		case cLRND:
			return 'L', nil
		}
	case 'L':
		switch ctype {
		case cEOS:
			return 'T', nil
		case cUD:
			return 'R', nil
		case cU:
			return 'U', nil
		case cL, cLR, cLRN, cLRND:
			return 'L', nil
		}
	case 'R':
		switch ctype {
		case cEOS:
			return 'T', nil
		case cUD, cU:
			return 'R', nil
		case cLRN, cLRND:
			return 'L', nil
		}
	case 'U':
		switch ctype {
		case cEOS:
			return 'T', nil
		case cUD:
			return 'R', nil
		case cU:
			return 'U', nil
		case cL, cLR, cLRN, cLRND:
			return 'L', nil
		}
	case 'T':
		return 0, errChunkType
	}
	return 0, errChunkType
}

// Reader2 reads an LZMA2 chunk sequence through the fused core.
type Reader2 struct {
	r        io.Reader
	err      error
	d        *core
	dictCap  int
	cstate   byte
	chunkEnd bool

	uncompressed bool
	remaining    int64
	uEOF         bool
	scratch      []byte
}

// NewReader2 creates a reader for an LZMA2 chunk sequence with the given
// dictionary capacity.
func NewReader2(dictCap int, r io.Reader) (*Reader2, error) {
	if !(minDictCap <= dictCap && int64(dictCap) <= maxDictCap) {
		return nil, errDictSize
	}
	d, err := newCore(props{lc: 3, lp: 0, pb: 2}, dictCap, -1)
	if err != nil {
		return nil, err
	}
	r2 := &Reader2{
		r:       r,
		d:       d,
		dictCap: dictCap,
		cstate:  'S',
		scratch: make([]byte, 64<<10),
	}
	if err := r2.startChunk(); err != nil {
		r2.err = err
	}
	return r2, nil
}

func (r2 *Reader2) startChunk() error {
	r2.uncompressed = false
	r2.chunkEnd = false
	var h [1]byte
	if _, err := io.ReadFull(r2.r, h[:]); err != nil {
		if err == io.EOF {
			err = io.ErrUnexpectedEOF
		}
		return err
	}
	ctype, err := headerChunkType(h[0])
	if err != nil {
		return err
	}
	if r2.cstate, err = chunkStateNext(r2.cstate, ctype); err != nil {
		return err
	}
	if ctype == cEOS {
		return io.EOF
	}
	var hdr [5]byte
	if ctype == cUD || ctype == cU {
		if _, err := io.ReadFull(r2.r, hdr[:2]); err != nil {
			return io.ErrUnexpectedEOF
		}
		size := int64(uint16(hdr[0])<<8|uint16(hdr[1])) + 1
		if ctype == cUD {
			r2.d.resetDict()
		}
		r2.uncompressed = true
		r2.remaining = size
		r2.uEOF = false
		return nil
	}
	n := 4
	if ctype == cLRN || ctype == cLRND {
		n = 5
	}
	if _, err := io.ReadFull(r2.r, hdr[:n]); err != nil {
		return io.ErrUnexpectedEOF
	}
	uncompressed := int64(uint32(h[0]&0x1f)<<16|uint32(hdr[0])<<8|uint32(hdr[1])) + 1
	compressed := int64(uint32(hdr[2])<<8|uint32(hdr[3])) + 1
	switch ctype {
	case cLRND:
		r2.d.resetDict()
		pr, err := propsForCode(hdr[4])
		if err != nil {
			return err
		}
		r2.d.initModel(pr)
	case cLRN:
		pr, err := propsForCode(hdr[4])
		if err != nil {
			return err
		}
		r2.d.initModel(pr)
	case cLR:
		r2.d.resetState()
	}
	return r2.d.reopen(io.LimitReader(r2.r, compressed), uncompressed)
}

// fillUncompressed moves uncompressed chunk bytes into the dict ring;
// io.EOF means the chunk is fully consumed and drained.
func (r2 *Reader2) fillUncompressed() error {
	if r2.remaining == 0 {
		if r2.d.dictBuffered() == 0 {
			return io.EOF
		}
		return nil
	}
	if r2.uEOF {
		return io.ErrUnexpectedEOF
	}
	if r2.d.avail() == 0 {
		return nil
	}
	n := min(int64(len(r2.scratch)), r2.remaining)
	m, err := r2.r.Read(r2.scratch[:n])
	if m > 0 {
		r2.d.writeRaw(r2.scratch[:m])
		r2.remaining -= int64(m)
	}
	if err != nil {
		r2.uEOF = true
	}
	if m > 0 {
		return nil
	}
	if r2.uEOF && r2.remaining != 0 {
		return io.ErrUnexpectedEOF
	}
	return nil
}

// Release returns pooled decoder buffers; the reader must not be used after.
func (r2 *Reader2) Release() {
	r2.d.release()
}

// Read returns uncompressed data from the current chunk, advancing across
// chunks as they end.
func (r2 *Reader2) Read(p []byte) (n int, err error) {
	if r2.err != nil {
		return 0, r2.err
	}
	for n < len(p) {
		var k int
		if r2.uncompressed {
			k = r2.d.readDict(p[n:])
			if k > 0 {
				n += k
				if n >= len(p) {
					return n, nil
				}
			}
			if r2.remaining == 0 && r2.d.readDict(nil) == 0 {
				if err = r2.startChunk(); err != nil {
					r2.err = err
					return n, err
				}
				continue
			}
			if err = r2.fillUncompressed(); err != nil {
				if err == io.EOF {
					continue
				}
				r2.err = err
				return n, err
			}
			continue
		}
		k, err = r2.d.Read(p[n:])
		n += k
		if err != nil {
			if err == io.EOF && !r2.d.eosMarker {
				if err = r2.startChunk(); err == nil {
					continue
				}
			}
			r2.err = err
			return n, err
		}
		if k == 0 {
			r2.err = fmt.Errorf("lzma: chunk produced no data")
			return n, r2.err
		}
	}
	return n, nil
}
