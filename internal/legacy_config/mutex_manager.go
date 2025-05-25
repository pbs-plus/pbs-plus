package config

import (
	"path/filepath"
	"sync"
)

type FileMutexManager struct {
	mu     sync.RWMutex
	locks  map[string]*sync.RWMutex
	refCnt map[string]int
}

func NewFileMutexManager() *FileMutexManager {
	return &FileMutexManager{
		locks:  make(map[string]*sync.RWMutex),
		refCnt: make(map[string]int),
	}
}

func (fm *FileMutexManager) getLock(path string) *sync.RWMutex {
	fm.mu.Lock()
	defer fm.mu.Unlock()

	absPath, err := filepath.Abs(path)
	if err != nil {
		absPath = path
	}

	if lock, exists := fm.locks[absPath]; exists {
		fm.refCnt[absPath]++
		return lock
	}

	lock := &sync.RWMutex{}
	fm.locks[absPath] = lock
	fm.refCnt[absPath] = 1
	return lock
}

func (fm *FileMutexManager) releaseLock(path string) {
	fm.mu.Lock()
	defer fm.mu.Unlock()

	absPath, err := filepath.Abs(path)
	if err != nil {
		absPath = path
	}

	if fm.refCnt[absPath] > 1 {
		fm.refCnt[absPath]--
		return
	}

	delete(fm.locks, absPath)
	delete(fm.refCnt, absPath)
}

func (fm *FileMutexManager) WithReadLock(path string, fn func() error) error {
	lock := fm.getLock(path)
	lock.RLock()
	defer func() {
		lock.RUnlock()
		fm.releaseLock(path)
	}()
	return fn()
}

func (fm *FileMutexManager) WithWriteLock(path string, fn func() error) error {
	lock := fm.getLock(path)
	lock.Lock()
	defer func() {
		lock.Unlock()
		fm.releaseLock(path)
	}()
	return fn()
}
