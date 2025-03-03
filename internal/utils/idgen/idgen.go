package idgen

import (
	"sync/atomic"
)

// IDGenerator generates simple auto-incrementing IDs
type IDGenerator struct {
	counter uint64
}

// NewIDGenerator creates a new instance of IDGenerator
func NewIDGenerator() *IDGenerator {
	return &IDGenerator{}
}

// NextID generates the next unique ID
func (g *IDGenerator) NextID() uint64 {
	return atomic.AddUint64(&g.counter, 1)
}
