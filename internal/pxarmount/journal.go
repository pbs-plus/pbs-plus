package pxarmount

import (
	"sync"
	"sync/atomic"
	"time"

	"github.com/cockroachdb/pebble"
)

const (
	NodeDir     uint8 = 0
	NodeFile    uint8 = 1
	NodeSymlink uint8 = 2
)

type GraphNode struct {
	ID         int64
	Kind       uint8
	Mode       uint32
	UID        uint32
	GID        uint32
	Size       uint64
	MtimeNs    int64
	CtimeNs    int64
	HasData    bool
	SymlinkTgt string
	RedirectTo string
	Opaque     bool
}

type GraphEdge struct {
	ParentID int64
	Name     string
	ChildID  int64
}

const (
	prefixNode     = "n:"
	prefixEdge     = "e:"
	prefixXattr    = "x:"
	prefixWhiteout = "w:"
	prefixMeta     = "m:"
	prefixCsum     = "c:"
)

const commitInterval = 5 * time.Second

const schemaVersion = 1

type journalOp struct {
	s    pebbleSet
	keys []pebbleSet
}

type pebbleSet struct {
	key       []byte
	value     []byte
	delete    bool
	deleteEnd []byte
}

type Journal struct {
	db         *pebble.DB
	mu         sync.RWMutex
	nextNodeID atomic.Int64

	overlay   map[string][]byte
	pending   []journalOp
	commitErr error

	commitCh chan struct{}
	stopCh   chan struct{}
	stopped  chan struct{}
}
