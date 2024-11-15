//go:build windows

package cache

import "sync"

var SizeCache sync.Map // Map[filePath]*sync.Map (snapshotId -> size)
