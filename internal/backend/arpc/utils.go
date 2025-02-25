package arpcfs

import (
	"context"
	"time"
)

const FS_TIMEOUT = time.Second * 10

func TimeoutCtx() (context.Context, context.CancelFunc) {
	return context.WithTimeout(context.Background(), FS_TIMEOUT)
}
