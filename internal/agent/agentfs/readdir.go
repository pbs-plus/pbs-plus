package agentfs

import (
	"bytes"
	"os"
	"sync"
)

const (
	defaultBatchSize        = 128
	defaultTargetEncodedLen = 1024 * 1024
)

type DirReader struct {
	file          *os.File
	path          string
	encodeBuf     bytes.Buffer
	targetEncoded int
	noMoreFiles   bool
	mu            sync.Mutex
	closed        bool
}
