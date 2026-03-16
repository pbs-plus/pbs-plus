package native

import (
	"encoding/hex"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"
)

type ChunkStore struct {
	root      string
	crypt     *CryptConfig
	verify    bool
	chunkLock sync.RWMutex
}

func NewChunkStore(root string, crypt *CryptConfig, verify bool) *ChunkStore {
	return &ChunkStore{
		root:   root,
		crypt:  crypt,
		verify: verify,
	}
}

func (s *ChunkStore) chunkPath(digest [32]byte) string {
	hexDigest := hex.EncodeToString(digest[:])
	dir4 := hexDigest[:4]
	return filepath.Join(s.root, ".chunks", dir4, hexDigest)
}

func (s *ChunkStore) ReadChunk(digest [32]byte) ([]byte, error) {
	s.chunkLock.RLock()
	defer s.chunkLock.RUnlock()

	path := s.chunkPath(digest)
	f, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("open chunk %s: %w", hex.EncodeToString(digest[:]), err)
	}
	defer f.Close()

	data, err := io.ReadAll(f)
	if err != nil {
		return nil, fmt.Errorf("read chunk: %w", err)
	}

	blob, err := LoadDataBlob(data)
	if err != nil {
		return nil, fmt.Errorf("load blob: %w", err)
	}

	var digestPtr *[32]byte
	if s.verify {
		digestPtr = &digest
	}

	plaintext, err := blob.Decode(s.crypt, digestPtr)
	if err != nil {
		return nil, fmt.Errorf("decode chunk: %w", err)
	}

	return plaintext, nil
}

func (s *ChunkStore) ReadRawChunk(digest [32]byte) (*DataBlob, error) {
	s.chunkLock.RLock()
	defer s.chunkLock.RUnlock()

	path := s.chunkPath(digest)
	f, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("open chunk %s: %w", hex.EncodeToString(digest[:]), err)
	}
	defer f.Close()

	data, err := io.ReadAll(f)
	if err != nil {
		return nil, fmt.Errorf("read chunk: %w", err)
	}

	blob, err := LoadDataBlob(data)
	if err != nil {
		return nil, fmt.Errorf("load blob: %w", err)
	}

	return blob, nil
}

func (s *ChunkStore) StatChunk(digest [32]byte) (os.FileInfo, error) {
	path := s.chunkPath(digest)
	return os.Stat(path)
}
