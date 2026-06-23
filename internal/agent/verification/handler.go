package verification

import (
	"fmt"
	sha256simd "github.com/minio/sha256-simd"
	"io"
	"os"
	"sync"

	"github.com/fxamacker/cbor/v2"
	"github.com/pbs-plus/pbs-plus/internal/arpc"
)

var bufPool = sync.Pool{
	New: func() any {
		buf := make([]byte, 256*1024)
		return &buf
	},
}

type VerifyFileReq struct {
	FilePath string `cbor:"file_path"`
}

type VerifyFileResp struct {
	SHA256 [32]byte `cbor:"sha256"`
	Size   int64    `cbor:"size"`
	Error  string   `cbor:"error"`
}

type VerifyStartReq struct {
	VerifyID string `cbor:"verify_id"`
}

func HashFile(filePath string) ([32]byte, int64, error) {
	// Fast existence check  -  avoids opening a file handle on a
	info, err := os.Stat(filePath)
	if err != nil {
		return [32]byte{}, 0, fmt.Errorf("failed to stat file: %w", err)
	}
	if !info.Mode().IsRegular() {
		return [32]byte{}, 0, fmt.Errorf("not a regular file")
	}

	f, err := os.Open(filePath)
	if err != nil {
		return [32]byte{}, 0, fmt.Errorf("failed to open file: %w", err)
	}
	defer func() { _ = f.Close() }()

	h := sha256simd.New()
	bufp := bufPool.Get().(*[]byte)
	size, err := io.CopyBuffer(h, f, *bufp)
	bufPool.Put(bufp)
	if err != nil {
		return [32]byte{}, 0, fmt.Errorf("failed to read file: %w", err)
	}

	var digest [32]byte
	copy(digest[:], h.Sum(nil))

	return digest, size, nil
}

// VerifyChunkFileHandler is the ARPC handler that runs on the agent.
// It computes a whole-file SHA-256 hash and returns it to the server.
func VerifyChunkFileHandler(req *arpc.Request) (arpc.Response, error) {
	var reqData VerifyFileReq
	if err := cbor.Unmarshal(req.Payload, &reqData); err != nil {
		return arpc.Response{}, err
	}

	digest, size, err := HashFile(reqData.FilePath)
	resp := VerifyFileResp{}
	if err != nil {
		resp.Error = err.Error()
	} else {
		resp.SHA256 = digest
		resp.Size = size
	}

	encoded, err := cbor.Marshal(resp)
	if err != nil {
		return arpc.Response{}, err
	}

	return arpc.Response{Status: 200, Data: encoded}, nil
}
