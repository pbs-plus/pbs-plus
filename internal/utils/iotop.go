package utils

import (
	"sync"
	"time"
)

var previousRead = make(map[int]int64)
var previousWrite = make(map[int]int64)
var previousTime = make(map[int]time.Time)
var previousMu = make(map[int]*sync.Mutex)

func ClearIOStats(pid int) {
	if _, ok := previousMu[pid]; !ok {
		previousMu[pid] = &sync.Mutex{}
	}

	previousMu[pid].Lock()
	delete(previousRead, pid)
	delete(previousWrite, pid)
	delete(previousTime, pid)
	previousMu[pid].Unlock()

	delete(previousMu, pid)
}
