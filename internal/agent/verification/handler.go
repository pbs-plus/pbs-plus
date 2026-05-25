package verification

import (
	"crypto/sha256"
	"fmt"
	"io"
	"os"

	"github.com/fxamacker/cbor/v2"
	"github.com/pbs-plus/pbs-plus/internal/arpc"
)

// VerifyFileReq is the request sent to the agent to hash a file.
type VerifyFileReq struct {
	FilePath string `cbor:"file_path"`
}

// VerifyFileResp contains the file hash from the agent.
type VerifyFileResp struct {
	SHA256 [32]byte `cbor:"sha256"`
	Size   int64    `cbor:"size"`
	Error  string   `cbor:"error"`
}

// HashFile computes the SHA-256 hash of a file.
func HashFile(filePath string) ([32]byte, int64, error) {
	f, err := os.Open(filePath)
	if err != nil {
		return [32]byte{}, 0, fmt.Errorf("failed to open file: %w", err)
	}
	defer func() { _ = f.Close() }()

	h := sha256.New()
	size, err := io.Copy(h, f)
	if err != nil {
		return [32]byte{}, 0, fmt.Errorf("failed to read file: %w", err)
	}

	var digest [32]byte
	sum := h.Sum(nil)
	copy(digest[:], sum)

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
