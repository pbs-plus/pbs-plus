package lzma

import (
	"fmt"
	"math/rand"
	"testing"
)

func TestBranchlessSelect(t *testing.T) {
	rngSrc := rand.New(rand.NewSource(7))
	for range 100000 {
		rng := rngSrc.Uint32()&0xffffffff | 1<<24
		code := rngSrc.Uint32()
		if code >= rng {
			code %= rng
		}
		pv := uint32(1 + rngSrc.Intn(2047))
		b2 := (rng >> probbits) * pv

		rr, cr, pr, br := rng, code, pv, uint32(0)
		if cr < b2 {
			rr = b2
			pr += (1<<probbits - pr) >> movebits
		} else {
			cr -= b2
			rr -= b2
			pr -= pr >> movebits
			br = 1
		}

		rb, cb, pb := rng, code, pv
		lt := ^uint32(0) * uint32((uint64(cb)-uint64(b2))>>63)
		rb = (b2 & lt) | ((rb - b2) &^ lt)
		cb = (cb & lt) | ((cb - b2) &^ lt)
		pb += (1<<probbits - pb) >> movebits & lt
		pb -= pb >> movebits &^ lt
		bb := ^lt & 1

		if rb != rr || cb != cr || pb != pr || bb != br {
			fmt.Printf("MISMATCH rng=%08x code=%08x b2=%08x lt=%08x: branchless(%08x,%08x,%d,%d) ref(%08x,%08x,%d,%d)\n",
				rng, code, b2, lt, rb, cb, pb, bb, rr, cr, pr, br)
			t.FailNow()
		}
	}
}
