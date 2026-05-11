//go:build linux

package pxar

import (
	"encoding/hex"
	"fmt"
	"os"
	"path/filepath"

	"github.com/pbs-plus/pxar/datastore"
)

// pbsChunkSource implements datastore.ChunkSource for PBS datastores.
// PBS uses a 4-character subdirectory prefix for chunk storage
// (.chunks/ABCD/ABCDEF...), unlike the pxar library's ChunkStore
// which uses 2-character prefixes.
type pbsChunkSource struct {
	base string
}

var _ datastore.ChunkSource = (*pbsChunkSource)(nil)

// newPBSChunkSource creates a PBS-compatible chunk source at the given
// datastore root path (which must contain a .chunks directory).
func newPBSChunkSource(datastoreRoot string) (*pbsChunkSource, error) {
	chunkDir := filepath.Join(datastoreRoot, ".chunks")
	if _, err := os.Stat(chunkDir); err != nil {
		return nil, fmt.Errorf("chunk directory not found at %s: %w", chunkDir, err)
	}
	return &pbsChunkSource{base: datastoreRoot}, nil
}

// GetChunk reads a chunk from the PBS datastore.
func (s *pbsChunkSource) GetChunk(digest [32]byte) ([]byte, error) {
	path := pbsChunkPath(s.base, digest)
	data, err := os.ReadFile(path)
	if err != nil {
		if os.IsNotExist(err) {
			var buf [64]byte
			hex.Encode(buf[:], digest[:])
			return nil, fmt.Errorf("chunk not found: %s", string(buf[:16]))
		}
		return nil, fmt.Errorf("read chunk: %w", err)
	}
	return data, nil
}

// pbsChunkPath returns the PBS filesystem path for a chunk.
// PBS layout: .chunks/ABCD/ABCDEF... (4-char subdirectory prefix).
func pbsChunkPath(base string, digest [32]byte) string {
	var buf [64]byte
	hex.Encode(buf[:], digest[:])
	return filepath.Join(base, ".chunks", string(buf[:4]), string(buf[:]))
}
