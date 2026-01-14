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
	"github.com/pbs-plus/pbs-plus/internal/syslog"
)

func (f *ARPCFile) Close(ctx context.Context) error {
	if f.isClosed.Load() {
		syslog.L.Debug().
			WithMessage("Close called on already closed file").
			WithField("backup", f.backupId).
			WithField("path", f.name).
			Write()
		return nil
	}

	pipe, err := f.fs.getPipe(ctx)
	if err != nil {
		syslog.L.Error(err).
			WithJob(f.backupId).
			WithMessage("arpc session is nil").
			WithField("path", f.name).
			Write()
		return syscall.ENOENT
	}

	syslog.L.Debug().
		WithMessage("Issuing Close RPC").
		WithJob(f.backupId).
		WithField("path", f.name).
		WithField("handleID", f.handleID).
		Write()

	req := types.CloseReq{HandleID: f.handleID}

	ctxN, cancelN := context.WithTimeout(ctx, 1*time.Minute)
	defer cancelN()

	_, err = pipe.CallData(ctxN, f.backupId+"/Close", &req)
	if err != nil && !errors.Is(err, os.ErrNotExist) {
		syslog.L.Error(err).
			WithJob(f.backupId).
			WithMessage("failed to handle close request").
			WithField("path", f.name).
			WithField("handleID", f.handleID).
			Write()
		return nil
	}
	f.isClosed.Store(true)

	syslog.L.Debug().
		WithMessage("Close RPC completed").
		WithJob(f.backupId).
		WithField("path", f.name).
		WithField("handleID", f.handleID).
		Write()

	return nil
}

func (f *ARPCFile) Lseek(ctx context.Context, off int64, whence int) (uint64, error) {
	syslog.L.Debug().
		WithMessage("Lseek called").
		WithJob(f.backupId).
		WithField("path", f.name).
		WithField("offset", off).
		WithField("whence", whence).
		Write()

	req := types.LseekReq{
		HandleID: f.handleID,
		Offset:   int64(off),
		Whence:   whence,
	}

	pipe, err := f.fs.getPipe(ctx)
	if err != nil {
		syslog.L.Error(err).
			WithJob(f.backupId).
			WithMessage("arpc session is nil").
			WithField("path", f.name).
			Write()
		return 0, syscall.EOPNOTSUPP
	}

	ctxN, cancelN := context.WithTimeout(ctx, 1*time.Minute)
	defer cancelN()

	respBytes, err := pipe.CallData(ctxN, f.backupId+"/Lseek", &req)
	if err != nil {
		syslog.L.Error(err).
			WithJob(f.backupId).
			WithMessage("lseek call failed").
			WithField("path", f.name).
			WithField("offset", off).
			WithField("whence", whence).
			Write()
		return 0, syscall.EOPNOTSUPP
	}

	var resp types.LseekResp
	if err := cbor.Unmarshal(respBytes, &resp); err != nil {
		syslog.L.Error(err).
			WithJob(f.backupId).
			WithMessage("failed to handle lseek request").
			WithField("path", f.name).
			Write()
		return 0, syscall.EOPNOTSUPP
	}

	syslog.L.Debug().
		WithMessage("Lseek completed").
		WithJob(f.backupId).
		WithField("path", f.name).
		WithField("newOffset", resp.NewOffset).
		Write()

	return uint64(resp.NewOffset), nil
}

func (f *ARPCFile) ReadAt(ctx context.Context, p []byte, off int64) (int, error) {
	if f.isClosed.Load() {
		syslog.L.Error(syscall.ENOENT).
			WithJob(f.backupId).
			WithMessage("file is closed").
			WithField("path", f.name).
			Write()
		return 0, syscall.ENOENT
	}

	pipe, err := f.fs.getPipe(ctx)
	if err != nil {
		syslog.L.Error(err).
			WithJob(f.backupId).
			WithMessage("fs session is nil").
			WithField("path", f.name).
			Write()
		return 0, syscall.ENOENT
	}

	syslog.L.Debug().
		WithMessage("ReadAt called").
		WithJob(f.backupId).
		WithField("path", f.name).
		WithField("offset", off).
		WithField("length", len(p)).
		Write()

	req := types.ReadAtReq{
		HandleID: f.handleID,
		Offset:   off,
		Length:   len(p),
	}

	n, err := pipe.CallBinary(f.fs.Ctx, f.backupId+"/ReadAt", &req, p)
	if err != nil {
		syslog.L.Error(err).WithJob(f.backupId).
			WithMessage("failed to handle read request").
			WithField("path", f.name).
			WithField("offset", f.offset).
			WithField("length", len(p)).
			Write()

		return 0, io.EOF
	}

	f.fs.TotalBytes.Add(int64(n))

	syslog.L.Debug().
		WithMessage("ReadAt completed").
		WithJob(f.backupId).
		WithField("path", f.name).
		WithField("offset", off).
		WithField("requested", len(p)).
		WithField("bytesRead", n).
		Write()

	if n < len(p) {
		return n, io.EOF
	}

	return n, nil
}
