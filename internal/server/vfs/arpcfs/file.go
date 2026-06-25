//go:build linux

package arpcfs

import (
	"context"
	"errors"
	"io"
	"os"
	"syscall"
	"time"

	"github.com/fxamacker/cbor/v2"
	"github.com/pbs-plus/pbs-plus/internal/agent/agentfs/types"
	"github.com/pbs-plus/pbs-plus/internal/log"
)

func (f *ARPCFile) Close(ctx context.Context) error {
	if f.isClosed.Load() {
		log.Debug("close called on already closed file",

			"path", f.name, "backup", f.backupID)

		return nil
	}

	pipe, err := f.fs.getPipe(ctx)
	if err != nil {
		log.Error(err,

			"arpc session is nil",
			"path", f.name)

		return syscall.ENOENT
	}
	log.Debug("issuing Close RPC",

		"handleID", f.handleID, "path", f.name)

	req := types.CloseReq{HandleID: f.handleID}

	ctxN, cancelN := context.WithTimeout(ctx, 1*time.Minute)
	defer cancelN()

	_, err = pipe.CallData(ctxN, "Close", &req)
	if err != nil && !errors.Is(err, os.ErrNotExist) {
		log.Error(err,

			"failed to handle close request",

			"handleID", f.handleID, "path", f.name)

		return nil
	}
	f.isClosed.Store(true)
	log.Debug("close RPC completed",

		"handleID", f.handleID, "path", f.name)

	return nil
}

func (f *ARPCFile) Lseek(ctx context.Context, off int64, whence int) (uint64, error) {
	log.Debug("lseek called",

		"whence", whence, "offset", off, "path", f.name)

	req := types.LseekReq{
		HandleID: f.handleID,
		Offset:   int64(off),
		Whence:   whence,
	}

	pipe, err := f.fs.getPipe(ctx)
	if err != nil {
		log.Error(err,

			"arpc session is nil",
			"path", f.name)

		return 0, syscall.EOPNOTSUPP
	}

	ctxN, cancelN := context.WithTimeout(ctx, 1*time.Minute)
	defer cancelN()

	respBytes, err := pipe.CallData(ctxN, "Lseek", &req)
	if err != nil {
		log.Error(err,

			"lseek call failed",

			"whence", whence, "offset", off, "path", f.name)

		return 0, syscall.EOPNOTSUPP
	}

	var resp types.LseekResp
	if err := cbor.Unmarshal(respBytes, &resp); err != nil {
		log.Error(err,

			"failed to handle lseek request",
			"path", f.name)

		return 0, syscall.EOPNOTSUPP
	}
	log.Debug("lseek completed",

		"newOffset", resp.NewOffset, "path", f.name)

	return uint64(resp.NewOffset), nil
}

func (f *ARPCFile) ReadAt(ctx context.Context, p []byte, off int64) (int, error) {
	if f.isClosed.Load() {
		return 0, syscall.ENOENT
	}

	pipe, err := f.fs.getPipe(ctx)
	if err != nil {
		log.Error(err,

			"fs session is nil",
			"path", f.name)

		return 0, syscall.ENOENT
	}
	log.Debug("readAt called",

		"length", len(p), "offset", off, "path", f.name)

	req := types.ReadAtReq{
		HandleID: f.handleID,
		Offset:   off,
		Length:   len(p),
	}

	n, err := pipe.CallBinary(f.fs.Ctx, "ReadAt", &req, p)
	if err != nil {
		log.Error(err,
			"failed to handle read request",

			"length", len(p), "offset", f.offset, "path", f.name)

		return 0, io.EOF
	}

	f.fs.TotalBytes.Add(int64(n))
	log.Debug("readAt completed",

		"bytesRead", n, "requested", len(p), "offset", off, "path", f.name)

	if n < len(p) {
		return n, io.EOF
	}

	return n, nil
}
