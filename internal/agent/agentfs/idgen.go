package agentfs

import (
	"sync/atomic"
)

type IDGenerator struct {
	counter uint64
}

func NewIDGenerator() *IDGenerator {
	return &IDGenerator{}
}

func (g *IDGenerator) NextID() uint64 {
	return atomic.AddUint64(&g.counter, 1)
}
