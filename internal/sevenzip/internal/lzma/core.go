// Package lzma implements the LZMA/LZMA2 decompressors.
//
// Derived from github.com/ulikunitz/xz/lzma (BSD, Ulrich Kunitz) with the
// decode loop rewritten: operations decode into a small struct instead of a
// boxed interface (the upstream decoder heap-allocates once per decoded
// byte), the range decoder reads through a windowed byte buffer instead of
// per-byte io.ByteReader calls, and literal/match application is inlined.
// Bit-level semantics are unchanged.
package lzma

import (
	"errors"
	"io"
)

const (
	probbits = 11
	probInit = 1 << (probbits - 1)
	movebits = 5

	states     = 12
	maxPosBits = 4
	posStates  = 1 << maxPosBits

	minMatchLen = 2
	maxMatchLen = minMatchLen + 16 + 256 - 1

	minDictCap = 4096
	maxDictCap = 1<<31 - 1

	eosDist = 1<<32 - 1

	numLenStates  = 4
	startPosModel = 4
	endPosModel   = 14
	posSlotBits   = 6
	alignBits     = 4
)

var (
	errFormat        = errors.New("lzma: invalid stream header")
	errProps         = errors.New("lzma: invalid properties code")
	errDictSize      = errors.New("lzma: dictionary size out of range")
	errDataAfterEOS  = errors.New("lzma: data after end of stream marker")
	errSize          = errors.New("lzma: wrong uncompressed data size")
	errNoSpace       = errors.New("lzma: no space left in dictionary")
	errMatchDist     = errors.New("lzma: match distance out of range")
	errAlreadyClosed = errors.New("lzma: already closed")
	errNeedOneReader = errors.New("lzma: need exactly one reader")
	errState         = errors.New("lzma: wrong chunk state")
	errChunkType     = errors.New("lzma: unexpected chunk type")
)

// opKind discriminates decoded operations without interface boxing.
type opKind uint8

const (
	opLit opKind = iota
	opMatch
)

// op is one decoded LZMA operation: a literal byte or a match of n bytes at
// dist distance back. Value type on purpose: the decode loop allocates none.
type op struct {
	kind opKind
	b    byte
	n    int
	dist int
}

// props are the LZMA stream properties.
type props struct {
	lc, lp, pb uint32
}

func propsForCode(code byte) (props, error) {
	if code > (maxPB+1)*(maxLP+1)*(maxLC+1)-1 {
		return props{}, errProps
	}
	return props{
		lc: uint32(code % 9),
		lp: uint32(code / 9 % 5),
		pb: uint32(code / 45),
	}, nil
}

const (
	minLC, maxLC = 0, 8
	minLP, maxLP = 0, 4
	minPB, maxPB = 0, 4
)

// core is a fused LZMA decoder: range decoder, probability model, state,
// and ring-buffer dictionary in one struct so the hot loop touches one
// object and no interfaces.
type core struct {
	r     io.Reader
	in    []byte
	ipos  int
	iend  int
	ioEOF bool
	inErr error

	nrange uint32
	code   uint32

	dict  []byte
	front int
	rear  int
	head  int64

	p        props
	litProbs []uint16
	isMatch  [states * posStates]uint16
	isRep    [states]uint16
	isRepG0  [states]uint16
	isRepG1  [states]uint16
	isRepG2  [states]uint16
	isRepG0L [states * posStates]uint16

	lenChoice [2]uint16
	lenLow    [posStates * 8]uint16
	lenMid    [posStates * 8]uint16
	lenHigh   [256]uint16

	repLenChoice [2]uint16
	repLenLow    [posStates * 8]uint16
	repLenMid    [posStates * 8]uint16
	repLenHigh   [256]uint16

	distSlots  [numLenStates][1 << posSlotBits]uint16
	distModels [endPosModel - startPosModel][32]uint16
	alignTree  [1 << alignBits]uint16

	state uint32
	rep   [4]uint32

	size      int64
	start     int64
	eos       bool
	eosMarker bool
}

// fill replenishes the input window. Sets inErr once when exhausted.
func (d *core) fill() {
	for d.ipos >= d.iend {
		if d.ioEOF {
			return
		}
		n, err := d.r.Read(d.in)
		d.ipos, d.iend = 0, n
		if err != nil {
			d.ioEOF = true
			if d.inErr == nil {
				if err == io.EOF {
					d.inErr = io.EOF
				} else {
					d.inErr = err
				}
			}
		}
		if n > 0 {
			return
		}
	}
}

// nextByte returns the next compressed byte and reports input health.
func (d *core) nextByte() (byte, error) {
	if d.ipos >= d.iend {
		d.fill()
		if d.ipos >= d.iend {
			if d.inErr == nil {
				d.inErr = io.EOF
			}
			return 0, d.inErr
		}
	}
	b := d.in[d.ipos]
	d.ipos++
	return b, nil
}

// initRangeDecoder consumes the 5 initialization bytes.
func (d *core) initRangeDecoder() error {
	b, err := d.nextByte()
	if err != nil {
		return err
	}
	if b != 0 {
		return errFormat
	}
	d.nrange = 0xffffffff
	d.code = 0
	for range 4 {
		b, err := d.nextByte()
		if err != nil {
			return err
		}
		d.code = d.code<<8 | uint32(b)
	}
	if d.code >= d.nrange {
		return errFormat
	}
	return nil
}

// decodeBit decodes one bit against p, updating the probability and
// normalizing the range. Inlined by the compiler into the literal hot loop.
func (d *core) decodeBit(p *uint16) uint32 {
	bound := (d.nrange >> probbits) * uint32(*p)
	var bit uint32
	if d.code < bound {
		d.nrange = bound
		*p += (1<<probbits - *p) >> movebits
	} else {
		d.code -= bound
		d.nrange -= bound
		*p -= *p >> movebits
		bit = 1
	}
	if d.nrange < 1<<24 {
		d.nrange <<= 8
		b, err := d.nextByte()
		if err != nil {
			if d.inErr == nil {
				d.inErr = io.EOF
			}
			return bit
		}
		d.code = d.code<<8 | uint32(b)
	}
	return bit
}

// initModel resets the full probability model and the LZMA state.
func (d *core) initModel(p props) {
	d.p = p
	for i := range d.isMatch {
		d.isMatch[i] = probInit
		d.isRepG0L[i] = probInit
	}
	for i := range d.isRep {
		d.isRep[i] = probInit
		d.isRepG0[i] = probInit
		d.isRepG1[i] = probInit
		d.isRepG2[i] = probInit
	}
	d.litProbs = make([]uint16, 0x300<<(p.lc+p.lp))
	for i := range d.litProbs {
		d.litProbs[i] = probInit
	}
	d.resetLenTree(&d.lenChoice, &d.lenLow, &d.lenMid, &d.lenHigh)
	d.resetLenTree(&d.repLenChoice, &d.repLenLow, &d.repLenMid, &d.repLenHigh)
	for s := range d.distSlots {
		for i := range d.distSlots[s] {
			d.distSlots[s][i] = probInit
		}
	}
	for m := range d.distModels {
		bits := uint32((startPosModel + m) >> 1)
		n := uint32(1) << (bits - 1)
		for i := range n {
			d.distModels[m][i] = probInit
		}
	}
	for i := range d.alignTree {
		d.alignTree[i] = probInit
	}
	d.state = 0
	d.rep = [4]uint32{}
}

// resetLenTree reinitializes a length codec tree set.
func (d *core) resetLenTree(choice *[2]uint16, low, mid *[posStates * 8]uint16, high *[256]uint16) {
	choice[0], choice[1] = probInit, probInit
	for i := range low {
		low[i] = probInit
		mid[i] = probInit
	}
	for i := range high {
		high[i] = probInit
	}
}

// resetState resets probability model and LZMA state only (LZMA2 cLR).
func (d *core) resetState() {
	p := d.p
	d.initModel(p)
}

// resetDict marks the dictionary as restarted (contents stay readable).
func (d *core) resetDict() {
	d.head = 0
}

// reopen restarts decoding on a new compressed stream (LZMA2 chunk).
func (d *core) reopen(r io.Reader, size int64) error {
	d.r = r
	d.ipos, d.iend, d.ioEOF, d.inErr = 0, 0, false, nil
	d.start = d.head
	d.size = size
	d.eos = false
	return d.initRangeDecoder()
}

// dictLen is the usable history length for match distances.
func (d *core) dictLen() int {
	capacity := int64(len(d.dict) - 1)
	if d.head >= capacity {
		return int(capacity)
	}
	return int(d.head)
}

// byteAt returns the byte dist positions back, 0 outside the history.
func (d *core) byteAt(dist int) byte {
	if !(0 < dist && dist <= d.dictLen()) {
		return 0
	}
	i := d.front - dist
	if i < 0 {
		i += len(d.dict)
	}
	return d.dict[i]
}

// avail is the writable space in the dictionary ring.
func (d *core) avail() int {
	delta := d.rear - 1 - d.front
	if delta < 0 {
		delta += len(d.dict)
	}
	return delta
}

// writeLit appends one literal byte.
func (d *core) writeLit(b byte) {
	d.dict[d.front] = b
	d.front++
	if d.front == len(d.dict) {
		d.front = 0
	}
	d.head++
}

// writeMatch copies n bytes from dist back into the ring head.
func (d *core) writeMatch(dist, n int) error {
	if !(0 < dist && dist <= d.dictLen()) {
		return errMatchDist
	}
	if !(0 < n && n <= maxMatchLen) {
		return errMatchDist
	}
	if n > d.avail() {
		return errNoSpace
	}
	d.head += int64(n)
	i := d.front - dist
	if i < 0 {
		i += len(d.dict)
	}
	for n > 0 {
		var p []byte
		if i >= d.front {
			p = d.dict[i:]
			i = 0
		} else {
			p = d.dict[i:d.front]
			i = d.front
		}
		if len(p) > n {
			p = p[:n]
		}
		f := d.front
		if f+len(p) > len(d.dict) {
			copy(d.dict[f:], p[:len(d.dict)-f])
			copy(d.dict[0:], p[len(d.dict)-f:])
		} else {
			copy(d.dict[f:], p)
		}
		d.front += len(p)
		if d.front >= len(d.dict) {
			d.front -= len(d.dict)
		}
		n -= len(p)
	}
	return nil
}

// writeRaw appends raw bytes (LZMA2 uncompressed chunks).
func (d *core) writeRaw(p []byte) int {
	m := d.avail()
	if m < len(p) {
		p = p[:m]
	}
	f := d.front
	if f+len(p) > len(d.dict) {
		k := copy(d.dict[f:], p)
		copy(d.dict[0:], p[k:])
	} else {
		copy(d.dict[f:], p)
	}
	d.front += len(p)
	if d.front >= len(d.dict) {
		d.front -= len(d.dict)
	}
	d.head += int64(len(p))
	return len(p)
}

// dictBuffered is the decoded-but-unread byte count in the ring.
func (d *core) dictBuffered() int {
	m := d.front - d.rear
	if m < 0 {
		m += len(d.dict)
	}
	return m
}

// readDict drains up to len(p) decoded bytes from the ring.
func (d *core) readDict(p []byte) int {
	m := d.dictBuffered()
	n := min(m, len(p))
	k := copy(p, d.dict[d.rear:])
	if k < n {
		copy(p[k:], d.dict)
	}
	d.rear += n
	if d.rear >= len(d.dict) {
		d.rear -= len(d.dict)
	}
	return n
}

// decodeTree decodes a bits-wide value MSB-first from probs.
func (d *core) decodeTree(probs []uint16, bits int) uint32 {
	m := uint32(1)
	for range bits {
		m = m<<1 | d.decodeBit(&probs[m])
	}
	return m - 1<<uint(bits)
}

// decodeTreeReverse decodes a bits-wide value LSB-first from probs.
func (d *core) decodeTreeReverse(probs []uint16, bits int) uint32 {
	var v, m uint32 = 0, 1
	for j := range bits {
		b := d.decodeBit(&probs[m])
		m = m<<1 | b
		v |= b << uint(j)
	}
	return v
}

// decodeLen decodes a length offset via the choice/low/mid/high trees.
func (d *core) decodeLen(choice *[2]uint16, low, mid *[posStates * 8]uint16, high *[256]uint16, posState uint32) uint32 {
	if d.decodeBit(&choice[0]) == 0 {
		return d.decodeTree(low[posState*8:(posState+1)*8], 3)
	}
	if d.decodeBit(&choice[1]) == 0 {
		return 8 + d.decodeTree(mid[posState*8:(posState+1)*8], 3)
	}
	return 16 + d.decodeTree(high[:], 8)
}

// decodeDist decodes a distance offset; eosDist marks the EOS marker.
func (d *core) decodeDist(l uint32) uint32 {
	if l >= numLenStates {
		l = numLenStates - 1
	}
	posSlot := d.decodeTree(d.distSlots[l][:], posSlotBits)
	if posSlot < startPosModel {
		return posSlot
	}
	bits := posSlot>>1 - 1
	dist := (2 | posSlot&1) << bits
	if posSlot < endPosModel {
		m := &d.distModels[posSlot-startPosModel]
		return dist + d.decodeTreeReverse(m[:1<<bits], int(bits))
	}
	v := uint32(0)
	for i := int(bits - alignBits - 1); i >= 0; i-- {
		d.nrange >>= 1
		d.code -= d.nrange
		t := 0 - (d.code >> 31)
		d.code += d.nrange & t
		v = v<<1 | (t+1)&1
		if d.nrange < 1<<24 {
			d.nrange <<= 8
			b, err := d.nextByte()
			if err != nil {
				if d.inErr == nil {
					d.inErr = io.EOF
				}
			} else {
				d.code = d.code<<8 | uint32(b)
			}
		}
	}
	dist += v << alignBits
	return dist + d.decodeTreeReverse(d.alignTree[:], alignBits)
}

var errEOS = errors.New("lzma: EOS marker found")

// decodeOp decodes the next operation without applying it.
func (d *core) decodeOp() (op, error) {
	posState := uint32(d.head) & ((1 << d.p.pb) - 1)
	state2 := d.state<<maxPosBits | posState

	if d.decodeBit(&d.isMatch[state2]) == 0 {
		prev := d.byteAt(1)
		ls := (uint32(d.head)&(1<<d.p.lp-1))<<d.p.lc | uint32(prev)>>(8-d.p.lc)
		probs := d.litProbs[ls*0x300 : ls*0x300+0x300]
		symbol := uint32(1)
		if d.state >= 7 {
			m := uint32(d.byteAt(int(d.rep[0]) + 1))
			for {
				matchBit := m >> 7 & 1
				m <<= 1
				i := (1+matchBit)<<8 | symbol
				bit := d.decodeBit(&probs[i])
				symbol = symbol<<1 | bit
				if matchBit != bit || symbol >= 0x100 {
					break
				}
			}
		}
		for symbol < 0x100 {
			symbol = symbol<<1 | d.decodeBit(&probs[symbol])
		}
		switch {
		case d.state < 4:
			d.state = 0
		case d.state < 10:
			d.state -= 3
		default:
			d.state -= 6
		}
		return op{kind: opLit, b: byte(symbol - 0x100)}, nil
	}

	if d.decodeBit(&d.isRep[d.state]) == 0 {
		d.rep[3], d.rep[2], d.rep[1] = d.rep[2], d.rep[1], d.rep[0]
		if d.state < 7 {
			d.state = 7
		} else {
			d.state = 10
		}
		l := d.decodeLen(&d.lenChoice, &d.lenLow, &d.lenMid, &d.lenHigh, posState)
		dist := d.decodeDist(l)
		d.rep[0] = dist
		if dist == eosDist {
			d.eosMarker = true
			return op{}, errEOS
		}
		return op{kind: opMatch, n: int(l) + minMatchLen, dist: int(dist) + 1}, nil
	}

	dist := d.rep[0]
	if d.decodeBit(&d.isRepG0[d.state]) == 0 {
		if d.decodeBit(&d.isRepG0L[state2]) == 0 {
			if d.state < 7 {
				d.state = 9
			} else {
				d.state = 11
			}
			return op{kind: opMatch, n: 1, dist: int(dist) + 1}, nil
		}
	} else {
		if d.decodeBit(&d.isRepG1[d.state]) == 0 {
			dist = d.rep[1]
		} else {
			if d.decodeBit(&d.isRepG2[d.state]) == 0 {
				dist = d.rep[2]
			} else {
				dist = d.rep[3]
				d.rep[3] = d.rep[2]
			}
			d.rep[2] = d.rep[1]
		}
		d.rep[1] = d.rep[0]
		d.rep[0] = dist
	}
	n := d.decodeLen(&d.repLenChoice, &d.repLenLow, &d.repLenMid, &d.repLenHigh, posState)
	if d.state < 7 {
		d.state = 8
	} else {
		d.state = 11
	}
	return op{kind: opMatch, n: int(n) + minMatchLen, dist: int(dist) + 1}, nil
}

// possiblyAtEnd mirrors the upstream code==0 end-of-stream check.
func (d *core) possiblyAtEnd() bool { return d.code == 0 }

// decompressed counts bytes produced since the current (re)start.
func (d *core) decompressed() int64 { return d.head - d.start }

// decompress fills the dictionary until full or the stream ends.
func (d *core) decompress() error {
	if d.eos {
		return io.EOF
	}
	for d.avail() >= maxMatchLen {
		if d.inErr != nil && d.ipos >= d.iend {
			d.eos = true
			if d.inErr == io.EOF {
				return io.ErrUnexpectedEOF
			}
			return d.inErr
		}
		o, err := d.decodeOp()
		if err == errEOS {
			d.eos = true
			if !d.possiblyAtEnd() {
				return errDataAfterEOS
			}
			if d.size >= 0 && d.size != d.decompressed() {
				return errSize
			}
			return io.EOF
		}
		if err != nil {
			return err
		}
		if d.inErr != nil && d.ipos >= d.iend {
			d.eos = true
			if d.inErr == io.EOF {
				return io.ErrUnexpectedEOF
			}
			return d.inErr
		}
		if o.kind == opLit {
			d.writeLit(o.b)
		} else if err := d.writeMatch(o.dist, o.n); err != nil {
			return err
		}
		if d.size >= 0 && d.decompressed() >= d.size {
			d.eos = true
			if d.decompressed() > d.size {
				return errSize
			}
			if !d.possiblyAtEnd() {
				_, err := d.decodeOp()
				switch {
				case d.inErr != nil && d.ipos >= d.iend:
					return io.ErrUnexpectedEOF
				case err == nil:
					return errSize
				case err == errEOS:
				default:
					return err
				}
			}
			return io.EOF
		}
	}
	return nil
}

// Read drains decoded data, decompressing as needed.
func (d *core) Read(p []byte) (n int, err error) {
	for {
		k := d.readDict(p[n:])
		n += k
		if k == 0 && d.eos {
			return n, io.EOF
		}
		if n >= len(p) {
			return n, nil
		}
		if err = d.decompress(); err != nil && err != io.EOF {
			return n, err
		}
	}
}
