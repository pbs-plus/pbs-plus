//go:build linux

package arpcfs

import (
	"context"
	"errors"
	"io"
	"os"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/fxamacker/cbor/v2"
	"github.com/pbs-plus/pbs-plus/internal/agent/agentfs/fswire"
	"github.com/pbs-plus/pbs-plus/internal/arpc"
	"github.com/pbs-plus/pbs-plus/internal/log"
)

func (f *ARPCFile) Close(ctx context.Context) error {
	if f.zs != nil {
		f.zs.close()
		f.zs = nil
		return nil
	}
	if f.isClosed.Load() {
		log.Debug("close called on already closed file",

			"path", f.name, "backup", f.backupID)

		return nil
	}

	log.Debug("issuing Close RPC",

		"handleID", f.handleID, "path", f.name)

	err := f.fs.closeRemote(ctx, f.handleID)
	if errors.Is(err, ErrSessionLost) {
		return err
	}
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

// closeRemote closes an agent handle; only transport loss is wrapped as session loss.
func (fs *ARPCFS) closeRemote(ctx context.Context, handle fswire.FileHandleID) error {
	if fs.remoteClose != nil {
		return fs.remoteClose(ctx, handle)
	}
	pipe, err := fs.getPipe(ctx)
	if err != nil {
		return fs.sessionLost(err)
	}
	ctxN, cancelN := context.WithTimeout(ctx, 1*time.Minute)
	defer cancelN()
	_, err = pipe.CallData(ctxN, "Close", &fswire.CloseReq{HandleID: handle})
	return err
}

func (f *ARPCFile) Lseek(ctx context.Context, off int64, whence int) (uint64, error) {
	if f.zs != nil {
		return f.zs.Lseek(uint64(off), uint32(whence)), nil
	}
	log.Debug("lseek called",

		"whence", whence, "offset", off, "path", f.name)

	req := fswire.LseekReq{
		HandleID: f.handleID,
		Offset:   int64(off),
		Whence:   whence,
	}

	pipe, err := f.fs.getPipe(ctx)
	if err != nil {
		return 0, f.fs.sessionLost(err)
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

	var resp fswire.LseekResp
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
	if f.zs != nil {
		return f.zs.ReadAt(ctx, p, off)
	}
	if f.isClosed.Load() {
		return 0, syscall.ENOENT
	}

	log.Debug("readAt called",

		"length", len(p), "offset", off, "path", f.name)

	n, err := f.fs.readRemote(ctx, f.handleID, p, off)
	if err != nil {
		log.Error(err,
			"failed to handle read request",

			"length", len(p), "offset", off, "path", f.name)
		return 0, err
	}

	f.fs.TotalBytes.Add(int64(n))
	log.Debug("readAt completed",

		"bytesRead", n, "requested", len(p), "offset", off, "path", f.name)

	if n < len(p) {
		return n, io.EOF
	}

	return n, nil
}

// fetchRemote reads for readahead: it neither waits for a missing session nor
// latches session loss, because a failed prefetch only falls back to readRemote.
func (fs *ARPCFS) fetchRemote(ctx context.Context, handle fswire.FileHandleID, p []byte, off int64) (int, error) {
	if fs.remoteReadAt != nil {
		return fs.remoteReadAt(ctx, handle, p, off)
	}
	pipe, ok := fs.agentManager.GetStreamPipe(fs.sessionId)
	if !ok {
		return 0, ErrSessionLost
	}
	req := fswire.ReadAtReq{HandleID: handle, Offset: off, Length: len(p)}
	return pipe.CallBinary(ctx, "ReadAt", &req, p)
}

// readRemote reads len(p) bytes at off from an open agent handle. Transport
// failures are reported as session loss; errors the agent returned are not.
func (fs *ARPCFS) readRemote(ctx context.Context, handle fswire.FileHandleID, p []byte, off int64) (int, error) {
	if fs.remoteReadAt != nil {
		return fs.remoteReadAt(ctx, handle, p, off)
	}
	pipe, err := fs.getPipe(ctx)
	if err != nil {
		return 0, fs.sessionLost(err)
	}
	req := fswire.ReadAtReq{
		HandleID: handle,
		Offset:   off,
		Length:   len(p),
	}
	n, err := pipe.CallBinary(fs.Ctx, "ReadAt", &req, p)
	if err != nil {
		if _, remote := errors.AsType[*arpc.RemoteError](err); remote {
			return 0, err
		}
		return 0, fs.sessionLost(err)
	}
	return n, nil
}

func (f *ARPCFile) precheck(ctx context.Context) error {
	if f.zs == nil {
		return nil
	}
	return f.zs.precheck(ctx)
}

type ARPCFile struct {
	fs       *ARPCFS
	name     string
	handleID fswire.FileHandleID
	isClosed atomic.Bool
	backupID string
	zs       *zipFileState
}
