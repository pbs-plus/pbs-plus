package agentfs

import "sync"

const (
	defaultBufSize          = 1024 * 1024
	defaultTargetEncodedLen = 1024 * 1024
)

var bufferPool = sync.Pool{
	New: func() any {
		return make([]byte, defaultBufSize)
	},
}
