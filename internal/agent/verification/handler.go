package verification

import (
	"crypto/sha256"
	"fmt"
	"io"
	"os"

	"github.com/fxamacker/cbor/v2"
	"github.com/pbs-plus/pbs-plus/internal/arpc"
	"github.com/pbs-plus/pxar/buzhash"
)

// ChunkDigest is a chunk's SHA-256 digest and size.
type ChunkDigest struct {
	Digest [32]byte `json:"digest" cbor:"digest"`
	Size   int      `json:"size" cbor:"size"`
}

// VerifyFileReq is the request sent to the agent to chunk a file.
type VerifyFileReq struct {
	FilePath     string `cbor:"file_path"`
	AvgChunkSize int    `cbor:"avg_chunk_size"`
}

// VerifyFileResp contains the chunk digests from the agent.
type VerifyFileResp struct {
	Digests []ChunkDigest `cbor:"digests"`
	Error   string        `cbor:"error"`
}

// ChunkFile chunks a file using buzhash and returns the digests.
func ChunkFile(filePath string, avgChunkSize int) ([]ChunkDigest, error) {
	f, err := os.Open(filePath)
	if err != nil {
		return nil, fmt.Errorf("failed to open file: %w", err)
	}
	defer func() { _ = f.Close() }()

	config, err := buzhash.NewConfig(avgChunkSize)
	if err != nil {
		return nil, fmt.Errorf("invalid chunk size: %w", err)
	}

	chunker := buzhash.NewChunker(f, config)
	var digests []ChunkDigest

	for {
		chunk, err := chunker.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			return digests, fmt.Errorf("chunker error: %w", err)
		}

		digest := sha256.Sum256(chunk)
		digests = append(digests, ChunkDigest{
			Digest: digest,
			Size:   len(chunk),
		})
	}

	return digests, nil
}

// VerifyChunkFileHandler is the ARPC handler that runs on the agent.
func VerifyChunkFileHandler(req *arpc.Request) (arpc.Response, error) {
	var reqData VerifyFileReq
	if err := cbor.Unmarshal(req.Payload, &reqData); err != nil {
		return arpc.Response{}, err
	}

	digests, err := ChunkFile(reqData.FilePath, reqData.AvgChunkSize)
	resp := VerifyFileResp{}
	if err != nil {
		resp.Error = err.Error()
	} else {
		resp.Digests = digests
	}

	encoded, err := cbor.Marshal(resp)
	if err != nil {
		return arpc.Response{}, err
	}

	return arpc.Response{Status: 200, Data: encoded}, nil
}
