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
	"github.com/pbs-plus/pbs-plus/internal/agent/agentfs/types"
	"github.com/pbs-plus/pbs-plus/internal/arpc"
	binarystream "github.com/pbs-plus/pbs-plus/internal/arpc/binary"
	"github.com/pbs-plus/pbs-plus/internal/syslog"
	"github.com/xtaci/smux"
)

func (f *ARPCFile) Close(ctx context.Context) error {
	if f.isClosed.Load() {
		syslog.L.Debug().
			WithMessage("Close called on already closed file").
			WithJob(f.jobId).
			WithField("path", f.name).
			Write()
		return nil
	}

	if f.fs.session.Load() == nil {
		syslog.L.Error(os.ErrInvalid).
			WithJob(f.jobId).
			WithMessage("arpc session is nil").
			WithField("path", f.name).
			Write()
		return syscall.ENOENT
	}

	syslog.L.Debug().
		WithMessage("Issuing Close RPC").
		WithJob(f.jobId).
		WithField("path", f.name).
		WithField("handleID", f.handleID).
		Write()

	req := types.CloseReq{HandleID: f.handleID}

	ctxN, cancelN := context.WithTimeout(ctx, 1*time.Minute)
	defer cancelN()

	_, err := f.fs.session.Load().CallData(ctxN, f.jobId+"/Close", &req)
	if err != nil && !errors.Is(err, os.ErrNotExist) {
		syslog.L.Error(err).
			WithJob(f.jobId).
			WithMessage("failed to handle close request").
			WithField("path", f.name).
			WithField("handleID", f.handleID).
			Write()
		return nil
	}
	f.isClosed.Store(true)

	syslog.L.Debug().
		WithMessage("Close RPC completed").
		WithJob(f.jobId).
		WithField("path", f.name).
		WithField("handleID", f.handleID).
		Write()

	return nil
}

func (f *ARPCFile) Lseek(ctx context.Context, off int64, whence int) (uint64, error) {
	syslog.L.Debug().
		WithMessage("Lseek called").
		WithJob(f.jobId).
		WithField("path", f.name).
		WithField("offset", off).
		WithField("whence", whence).
		Write()

	req := types.LseekReq{
		HandleID: f.handleID,
		Offset:   int64(off),
		Whence:   whence,
	}

	ctxN, cancelN := context.WithTimeout(ctx, 1*time.Minute)
	defer cancelN()

	respBytes, err := f.fs.session.Load().CallData(ctxN, f.jobId+"/Lseek", &req)
	if err != nil {
		syslog.L.Error(err).
			WithJob(f.jobId).
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
			WithJob(f.jobId).
			WithMessage("failed to handle lseek request").
			WithField("path", f.name).
			Write()
		return 0, syscall.EOPNOTSUPP
	}

	syslog.L.Debug().
		WithMessage("Lseek completed").
		WithJob(f.jobId).
		WithField("path", f.name).
		WithField("newOffset", resp.NewOffset).
		Write()

	return uint64(resp.NewOffset), nil
}

func (f *ARPCFile) ReadAt(ctx context.Context, p []byte, off int64) (int, error) {
	if f.isClosed.Load() {
		syslog.L.Error(syscall.ENOENT).
			WithJob(f.jobId).
			WithMessage("file is closed").
			WithField("path", f.name).
			Write()
		return 0, syscall.ENOENT
	}

	if f.fs.session.Load() == nil {
		syslog.L.Error(syscall.ENOENT).
			WithJob(f.jobId).
			WithMessage("fs session is nil").
			WithField("path", f.name).
			Write()
		return 0, syscall.ENOENT
	}

	syslog.L.Debug().
		WithMessage("ReadAt called").
		WithJob(f.jobId).
		WithField("path", f.name).
		WithField("offset", off).
		WithField("length", len(p)).
		Write()

	req := types.ReadAtReq{
		HandleID: f.handleID,
		Offset:   off,
		Length:   len(p),
	}

	bytesRead := 0

	ctxN, cancelN := context.WithTimeout(ctx, 1*time.Minute)
	defer cancelN()

	err := f.fs.session.Load().Call(ctxN, f.jobId+"/ReadAt", &req, arpc.RawStreamHandler(func(s *smux.Stream) error {
		n, err := binarystream.ReceiveDataInto(s, p)
		if err != nil {
			return err
		}
		bytesRead = n

		return nil
	}))
	if err != nil {
		syslog.L.Error(err).WithJob(f.jobId).
			WithMessage("failed to handle read request, replace failed reads with zeroes, likely corrupted").
			WithField("path", f.name).
			WithField("offset", f.offset).
			WithField("length", len(p)).
			Write()

		return 0, io.EOF
	}

	atomic.AddInt64(&f.fs.TotalBytes, int64(bytesRead))

	syslog.L.Debug().
		WithMessage("ReadAt completed").
		WithJob(f.jobId).
		WithField("path", f.name).
		WithField("offset", off).
		WithField("requested", len(p)).
		WithField("bytesRead", bytesRead).
		Write()

	if bytesRead < len(p) {
		return bytesRead, io.EOF
	}

	return bytesRead, nil
}
