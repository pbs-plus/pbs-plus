package lzma

import (
	"bytes"
	"fmt"
	"testing"
)

// refRD ports ulikunitz rangeDecoder for divergence hunting.
type refRD struct {
	br     *bytes.Reader
	nrange uint32
	code   uint32
}

func (d *refRD) init() error {
	b, err := d.br.ReadByte()
	if err != nil || b != 0 {
		return errFormat
	}
	d.nrange = 0xffffffff
	d.code = 0
	for range 4 {
		b, err := d.br.ReadByte()
		if err != nil {
			return err
		}
		d.code = d.code<<8 | uint32(b)
	}
	return nil
}

func (d *refRD) decodeBit(p *uint16) uint32 {
	bound := (d.nrange >> 11) * uint32(*p)
	var bit uint32
	if d.code < bound {
		d.nrange = bound
		*p += (2048 - *p) >> 5
	} else {
		d.code -= bound
		d.nrange -= bound
		*p -= *p >> 5
		bit = 1
	}
	if d.nrange < 1<<24 {
		d.nrange <<= 8
		b, err := d.br.ReadByte()
		if err != nil {
			panic(err)
		}
		d.code = d.code<<8 | uint32(b)
	}
	return bit
}

// refDec is a minimal ulikunitz-exact LZMA decoder for tracing: persistent
// probs, plain dict, literal-first streams only.
type refDec struct {
	rd      refRD
	isMatch [192]uint16
	lit     []uint16
	state   uint32
	head    int64
	dict    []byte
	lc, lp  uint32
	rep0    uint32
}

func newRefDec(body []byte, lc, lp uint32) *refDec {
	r := &refDec{lit: make([]uint16, 0x300<<(lc+lp)), dict: make([]byte, 1<<20), lc: lc, lp: lp}
	for i := range r.isMatch {
		r.isMatch[i] = 1024
	}
	for i := range r.lit {
		r.lit[i] = 1024
	}
	r.rd.br = bytes.NewReader(body)
	if err := r.rd.init(); err != nil {
		panic(err)
	}
	return r
}

func (r *refDec) byteAt(dist int) byte {
	if !(0 < dist && dist <= int(r.head)) {
		return 0
	}
	return r.dict[r.head-int64(dist)]
}

func (r *refDec) literal() byte {
	prev := r.byteAt(1)
	litState := ((uint32(r.head) & (1<<r.lp - 1)) << r.lc) | uint32(prev)>>(8-r.lc)
	probs := r.lit[litState*0x300 : litState*0x300+0x300]
	symbol := uint32(1)
	if r.state >= 7 {
		m := uint32(r.byteAt(int(r.rep0) + 1))
		for {
			matchBit := m >> 7 & 1
			m <<= 1
			i := (1+matchBit)<<8 | symbol
			bit := r.rd.decodeBit(&probs[i])
			symbol = symbol<<1 | bit
			if matchBit != bit || symbol >= 0x100 {
				break
			}
		}
	}
	for symbol < 0x100 {
		symbol = symbol<<1 | r.rd.decodeBit(&probs[symbol])
	}
	return byte(symbol - 0x100)
}

func (r *refDec) op() (op, error) {
	posState := uint32(r.head) & 3
	state2 := r.state<<4 | posState
	if r.rd.decodeBit(&r.isMatch[state2]) == 0 {
		b := r.literal()
		switch {
		case r.state < 4:
			r.state = 0
		case r.state < 10:
			r.state -= 3
		default:
			r.state -= 6
		}
		r.dict[r.head] = b
		r.head++
		return op{kind: opLit, b: b}, nil
	}
	return op{}, fmt.Errorf("match path not needed for trace")
}

var _ = 0

func TestOpTrace(t *testing.T) {
	body := []byte{0x00, 0x3a, 0x19, 0xca, 0x68, 0x37, 0x46, 0x97, 0xb8, 0x10, 0x15, 0x6e, 0x5e, 0x5a, 0xe6, 0xc2, 0x84, 0xab, 0x66, 0x8a}

	ref := newRefDec(body, 3, 0)
	ref.rep0 = 0

	d := &core{in: make([]byte, 64), dict: make([]byte, 1<<16+1)}
	d.r = bytes.NewReader(body)
	if err := d.initRangeDecoder(); err != nil {
		t.Fatal(err)
	}
	d.initModel(props{lc: 3, lp: 0, pb: 2})

	for i := range 12 {
		ro, err := ref.op()
		if err != nil {
			t.Fatalf("ref op %d: %v", i, err)
		}
		fo, err := d.decodeOp()
		if err != nil {
			t.Fatalf("fused op %d: %v", i, err)
		}
		mark := "="
		if ro != fo {
			mark = "DIFF"
		}
		fmt.Printf("op %2d ref lit=%02x fused lit=%02x %s (rd code=%08x/%08x range=%08x/%08x)\n",
			i, ro.b, fo.b, mark, ref.rd.code, d.code, ref.rd.nrange, d.nrange)
		if ro != fo {
			t.Fatalf("op %d diverged", i)
		}
		if fo.kind == opLit {
			d.writeLit(fo.b)
		}
		if ref.state != d.state {
			t.Fatalf("state diverged ref=%d fused=%d", ref.state, d.state)
		}
	}
}
