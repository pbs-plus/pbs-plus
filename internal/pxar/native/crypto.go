package native

import (
	"crypto/sha256"
	"hash"
)

type Sha256Hasher struct {
	h hash.Hash
}

func NewSha256Hasher() *Sha256Hasher {
	return &Sha256Hasher{
		h: sha256.New(),
	}
}

func (h *Sha256Hasher) Write(p []byte) (int, error) {
	return h.h.Write(p)
}

func (h *Sha256Hasher) Sum256() [32]byte {
	var result [32]byte
	copy(result[:], h.h.Sum(nil))
	return result
}

func (h *Sha256Hasher) Reset() {
	h.h.Reset()
}

func Sha256Sum(data []byte) [32]byte {
	return sha256.Sum256(data)
}
